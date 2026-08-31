/* Dispatch for PostgreSQL-independent SemMap/SemFilter result machines. */
#include <stddef.h>
#include <string.h>

#include "sem_operator_machine.h"

#define SEMLOOM_MACHINE_OPERATOR_MAP 1
#define SEMLOOM_MACHINE_OPERATOR_FILTER 2
#define SEMLOOM_RECORDING_PLAN_SCHEMA_VERSION 1
#define SEMLOOM_EXACT_FILTER_PLAN_SCHEMA_VERSION 2

#define SEMLOOM_FILTER_SYSTEM_DIRECTIVE \
	"Evaluate whether the input satisfies the instruction. Reply with exactly " \
	"TRUE, FALSE, or UNKNOWN. Use UNKNOWN only when the input lacks enough information."
#define SEMLOOM_FILTER_INSTRUCTION_SEPARATOR "\nInstruction:\n"

typedef struct SemloomTaskWriter
{
	uint8_t *destination;
	size_t capacity;
	size_t length;
	bool valid;
} SemloomTaskWriter;

static void semloom_task_writer_bytes(SemloomTaskWriter *writer,
										  const void *data,
										  size_t length);
static void semloom_task_writer_json_string(SemloomTaskWriter *writer,
												 const uint8_t *data,
												 size_t length);
static bool semloom_operator_machine_build_task(
	const SemloomOperatorMachine *machine,
	const SemloomBoundValue *input,
	uint8_t *destination,
	size_t destination_length,
	size_t *written_length);

bool
semloom_operator_machine_init(SemloomOperatorMachine *machine,
								 uint32_t operator_kind,
								 uint32_t plan_schema_version,
								 const uint8_t *instruction,
								 uint32_t instruction_length)
{
	if (machine == NULL)
		return false;
	machine->methods = NULL;
	machine->plan_schema_version = plan_schema_version;
	machine->instruction = instruction;
	machine->instruction_length = instruction_length;
	machine->invalid_completion_message = NULL;
	if (operator_kind == SEMLOOM_MACHINE_OPERATOR_MAP &&
		plan_schema_version == SEMLOOM_RECORDING_PLAN_SCHEMA_VERSION)
	{
		machine->methods = &semloom_map_machine_methods;
		machine->invalid_completion_message =
			"SemMap provider returned an invalid completion";
	}
	else if (operator_kind == SEMLOOM_MACHINE_OPERATOR_FILTER &&
			 plan_schema_version == SEMLOOM_RECORDING_PLAN_SCHEMA_VERSION)
	{
		machine->methods = &semloom_filter_recording_machine_methods;
		machine->invalid_completion_message =
			"SemFilter provider completion must be true, false, or unknown";
	}
	else if (operator_kind == SEMLOOM_MACHINE_OPERATOR_FILTER &&
			 plan_schema_version == SEMLOOM_EXACT_FILTER_PLAN_SCHEMA_VERSION)
	{
		if (instruction == NULL || instruction_length == 0)
			return false;
		machine->methods = &semloom_filter_exact_machine_methods;
		machine->invalid_completion_message =
			"SemFilter model completion must be TRUE, FALSE, or UNKNOWN";
	}
	return machine->methods != NULL;
}

size_t
semloom_operator_machine_task_size(const SemloomOperatorMachine *machine,
									 const SemloomBoundValue *input)
{
	size_t length = 0;

	if (!semloom_operator_machine_build_task(machine,
										 input,
										 NULL,
										 0,
										 &length))
		return 0;
	return length;
}

bool
semloom_operator_machine_write_task(const SemloomOperatorMachine *machine,
									const SemloomBoundValue *input,
									uint8_t *destination,
									size_t destination_length)
{
	size_t written_length = 0;

	return semloom_operator_machine_build_task(machine,
											 input,
											 destination,
											 destination_length,
											 &written_length) &&
		written_length == destination_length;
}

SemloomTupleDisposition
semloom_operator_machine_handle_null(const SemloomOperatorMachine *machine)
{
	return machine->methods->handle_null();
}

SemloomTupleDisposition
semloom_operator_machine_apply_completion(
	const SemloomOperatorMachine *machine,
	const SemloomMachineCompletion *completion)
{
	return machine->methods->apply_completion(completion);
}

const char *
semloom_operator_machine_explain_property(const SemloomOperatorMachine *machine)
{
	return machine->methods->input_explain_property;
}

const char *
semloom_operator_machine_invalid_message(const SemloomOperatorMachine *machine)
{
	return machine->invalid_completion_message;
}

static bool
semloom_operator_machine_build_task(const SemloomOperatorMachine *machine,
									const SemloomBoundValue *input,
									uint8_t *destination,
									size_t destination_length,
									size_t *written_length)
{
	static const char prefix[] = "[{\"role\":\"system\",\"content\":";
	static const char middle[] = "},{\"role\":\"user\",\"content\":";
	static const char suffix[] = "}]";
	static const char directive[] = SEMLOOM_FILTER_SYSTEM_DIRECTIVE;
	static const char separator[] = SEMLOOM_FILTER_INSTRUCTION_SEPARATOR;
	SemloomTaskWriter writer = {
		.destination = destination,
		.capacity = destination_length,
		.length = 0,
		.valid = true,
	};

	if (machine == NULL || input == NULL || written_length == NULL || input->is_null ||
		(input->length > 0 && input->data == NULL))
		return false;
	if (machine->plan_schema_version == SEMLOOM_RECORDING_PLAN_SCHEMA_VERSION)
	{
		*written_length = 0;
		return true;
	}
	if (machine->plan_schema_version != SEMLOOM_EXACT_FILTER_PLAN_SCHEMA_VERSION ||
		machine->instruction == NULL || machine->instruction_length == 0)
		return false;

	semloom_task_writer_bytes(&writer, prefix, sizeof(prefix) - 1);
	semloom_task_writer_bytes(&writer, "\"", 1);
	semloom_task_writer_json_string(&writer,
									(const uint8_t *) directive,
									sizeof(directive) - 1);
	semloom_task_writer_json_string(&writer,
									(const uint8_t *) separator,
									sizeof(separator) - 1);
	semloom_task_writer_json_string(&writer,
									machine->instruction,
									machine->instruction_length);
	semloom_task_writer_bytes(&writer, "\"", 1);
	semloom_task_writer_bytes(&writer, middle, sizeof(middle) - 1);
	semloom_task_writer_bytes(&writer, "\"", 1);
	semloom_task_writer_json_string(&writer, input->data, input->length);
	semloom_task_writer_bytes(&writer, "\"", 1);
	semloom_task_writer_bytes(&writer, suffix, sizeof(suffix) - 1);
	*written_length = writer.length;
	return writer.valid &&
		(destination == NULL || writer.length == destination_length);
}

static void
semloom_task_writer_bytes(SemloomTaskWriter *writer,
							  const void *data,
							  size_t length)
{
	if (writer->destination != NULL)
	{
		if (writer->length > writer->capacity ||
			length > writer->capacity - writer->length)
		{
			writer->valid = false;
			return;
		}
		memcpy(writer->destination + writer->length, data, length);
	}
	writer->length += length;
}

static void
semloom_task_writer_json_string(SemloomTaskWriter *writer,
								 const uint8_t *data,
								 size_t length)
{
	static const char hex[] = "0123456789abcdef";
	size_t index;

	for (index = 0; index < length; index++)
	{
		uint8_t byte = data[index];

		if (byte == '"' || byte == '\\')
		{
			uint8_t escaped[2] = {'\\', byte};

			semloom_task_writer_bytes(writer, escaped, sizeof(escaped));
		}
		else if (byte == '\b' || byte == '\f' || byte == '\n' ||
				 byte == '\r' || byte == '\t')
		{
			uint8_t escaped[2] = {'\\', 0};

			switch (byte)
			{
				case '\b': escaped[1] = 'b'; break;
				case '\f': escaped[1] = 'f'; break;
				case '\n': escaped[1] = 'n'; break;
				case '\r': escaped[1] = 'r'; break;
				default: escaped[1] = 't'; break;
			}
			semloom_task_writer_bytes(writer, escaped, sizeof(escaped));
		}
		else if (byte < 0x20)
		{
			uint8_t escaped[6] = {'\\', 'u', '0', '0',
				(uint8_t) hex[byte >> 4],
				(uint8_t) hex[byte & 0x0f]};

			semloom_task_writer_bytes(writer, escaped, sizeof(escaped));
		}
		else
			semloom_task_writer_bytes(writer, &byte, 1);
	}
}
