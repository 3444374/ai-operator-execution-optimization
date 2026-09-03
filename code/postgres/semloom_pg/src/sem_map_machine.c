/* SemMap message compilation and recording result interpretation. */
#include "sem_operator_machine.h"
#include "sem_message_writer.h"
#include "semantic_map_contract.h"
#include "sem_text.h"

static SemloomTupleDisposition semloom_map_handle_null(void);
static SemloomTupleDisposition semloom_map_apply_completion(
	const SemloomMachineCompletion *completion);
static bool semloom_map_build_task(const SemloomOperatorMachine *machine,
	const SemloomBoundValue *input, uint8_t *destination, size_t capacity,
	size_t *written);

const SemloomOperatorMachineMethods semloom_map_machine_methods = {
	.input_explain_property = "Mapped Column",
	.handle_null = semloom_map_handle_null,
	.apply_completion = semloom_map_apply_completion,
};

const SemloomOperatorMachineMethods semloom_map_generate_machine_methods = {
	.input_explain_property = "Mapped Column",
	.handle_null = semloom_map_handle_null,
	.apply_completion = semloom_map_apply_completion,
	.build_task = semloom_map_build_task,
};

static bool
semloom_map_build_task(const SemloomOperatorMachine *machine,
	const SemloomBoundValue *input, uint8_t *destination, size_t capacity,
	size_t *written)
{
	SemloomBoundValue instruction = {machine->instruction, machine->instruction_length, false};
	size_t required = semloom_map_task_size(&instruction, input);

	if (required == 0)
		return false;
	if (destination != NULL &&
		!semloom_map_write_task(&instruction, input, destination, capacity))
		return false;
	*written = required;
	return true;
}

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
		!semloom_text_is_utf8_no_nul(instruction->data, instruction->length) ||
		!semloom_text_is_utf8_no_nul(input->data, input->length))
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
