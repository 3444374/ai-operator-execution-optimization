/* SemMap message compilation and recording result interpretation. */
#include "sem_operator_machine.h"
#include "sem_message_writer.h"
#include "semantic_map_contract.h"

static SemloomTupleDisposition semloom_map_handle_null(void);
static SemloomTupleDisposition semloom_map_apply_completion(
	const SemloomMachineCompletion *completion);
static bool semloom_map_text_valid(const SemloomBoundValue *value);

const SemloomOperatorMachineMethods semloom_map_machine_methods = {
	.input_explain_property = "Mapped Column",
	.handle_null = semloom_map_handle_null,
	.apply_completion = semloom_map_apply_completion,
};

size_t
semloom_map_task_size(const SemloomBoundValue *instruction,
					 const SemloomBoundValue *input)
{
	SemloomMessagePart system;
	SemloomMessagePart user;
	size_t length = 0;

	if (instruction == NULL || instruction->is_null || instruction->length == 0 ||
		instruction->length > SEMLOOM_MAP_MAX_INSTRUCTION_BYTES ||
		instruction->data == NULL || input == NULL || input->is_null ||
		input->length > SEMLOOM_MAP_MAX_INPUT_BYTES ||
		(input->length > 0 && input->data == NULL) ||
		!semloom_map_text_valid(instruction) || !semloom_map_text_valid(input))
		return 0;
	system = (SemloomMessagePart) {instruction->data, instruction->length};
	user = (SemloomMessagePart) {input->data, input->length};
	if (!semloom_message_write(&system, 1, user, NULL, 0, &length))
		return 0;
	return length;
}

bool
semloom_map_write_task(const SemloomBoundValue *instruction,
					  const SemloomBoundValue *input,
					  uint8_t *destination,
					  size_t destination_length)
{
	size_t required = semloom_map_task_size(instruction, input);
	size_t written = 0;
	SemloomMessagePart system;
	SemloomMessagePart user;

	if (required == 0 || destination == NULL || required != destination_length)
		return false;
	system = (SemloomMessagePart) {instruction->data, instruction->length};
	user = (SemloomMessagePart) {input->data, input->length};
	return semloom_message_write(&system, 1, user, destination, destination_length, &written);
}

static bool
semloom_map_text_valid(const SemloomBoundValue *value)
{
	size_t index = 0;

	while (index < value->length)
	{
		uint8_t byte = value->data[index++];
		size_t following;
		size_t offset;
		uint8_t minimum = 0x80;
		uint8_t maximum = 0xbf;

		if (byte == 0)
			return false;
		if (byte < 0x80)
			continue;
		if (byte >= 0xc2 && byte <= 0xdf)
			following = 1;
		else if (byte >= 0xe0 && byte <= 0xef)
		{
			following = 2;
			if (byte == 0xe0)
				minimum = 0xa0;
			if (byte == 0xed)
				maximum = 0x9f;
		}
		else if (byte >= 0xf0 && byte <= 0xf4)
		{
			following = 3;
			if (byte == 0xf0)
				minimum = 0x90;
			if (byte == 0xf4)
				maximum = 0x8f;
		}
		else
			return false;
		if (following > value->length - index ||
			value->data[index] < minimum || value->data[index] > maximum)
			return false;
		for (offset = 1; offset < following; offset++)
		{
			if (value->data[index + offset] < 0x80 || value->data[index + offset] > 0xbf)
				return false;
		}
		index += following;
	}
	return true;
}

static SemloomTupleDisposition
semloom_map_handle_null(void)
{
	return SEMLOOM_TUPLE_EMIT;
}

static SemloomTupleDisposition
semloom_map_apply_completion(const SemloomMachineCompletion *completion)
{
	if (completion == NULL ||
		(!completion->is_null && completion->length > 0 && completion->data == NULL))
		return SEMLOOM_TUPLE_INVALID_COMPLETION;
	return SEMLOOM_TUPLE_EMIT_COMPLETION;
}
