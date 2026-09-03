/* Dispatch for PostgreSQL-independent SemMap/SemFilter result machines. */
#include <stddef.h>

#include "sem_operator_machine.h"

#define SEMLOOM_MACHINE_OPERATOR_MAP 1
#define SEMLOOM_MACHINE_OPERATOR_FILTER 2
#define SEMLOOM_RECORDING_PLAN_SCHEMA_VERSION 1
#define SEMLOOM_EXACT_FILTER_PLAN_SCHEMA_VERSION 2
#define SEMLOOM_CHOICE_FILTER_PLAN_SCHEMA_VERSION 3

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
			 (plan_schema_version == SEMLOOM_EXACT_FILTER_PLAN_SCHEMA_VERSION ||
			  plan_schema_version == SEMLOOM_CHOICE_FILTER_PLAN_SCHEMA_VERSION))
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
	if (machine == NULL || input == NULL || written_length == NULL || input->is_null ||
		(input->length > 0 && input->data == NULL))
		return false;
	if (machine->plan_schema_version == SEMLOOM_RECORDING_PLAN_SCHEMA_VERSION)
	{
		*written_length = 0;
		return true;
	}
	if (machine->methods == NULL || machine->methods->build_task == NULL ||
		machine->instruction == NULL || machine->instruction_length == 0)
		return false;

	return machine->methods->build_task(machine, input, destination,
									  destination_length, written_length);
}
