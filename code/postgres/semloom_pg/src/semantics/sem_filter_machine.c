/* SemFilter's exact recording and model-output parsers. */
#include <stddef.h>
#include <string.h>

#include "semantics/sem_operator_machine.h"
#include "semantics/sem_message_writer.h"
#include "semantics/semantic_filter_contract.h"

static SemloomTupleDisposition semloom_filter_handle_null(void);
static SemloomTupleDisposition semloom_filter_recording_apply_completion(
	const SemloomMachineCompletion *completion);
static SemloomTupleDisposition semloom_filter_exact_apply_completion(
	const SemloomMachineCompletion *completion);
static bool semloom_filter_build_task(const SemloomOperatorMachine *machine,
									 const SemloomBoundValue *input,
									 uint8_t *destination,
									 size_t destination_length,
									 size_t *written_length);

const SemloomOperatorMachineMethods semloom_filter_recording_machine_methods = {
	.input_explain_property = "Filter Input Column",
	.handle_null = semloom_filter_handle_null,
	.apply_completion = semloom_filter_recording_apply_completion,
};

const SemloomOperatorMachineMethods semloom_filter_exact_machine_methods = {
	.input_explain_property = "Filter Input Column",
	.handle_null = semloom_filter_handle_null,
	.apply_completion = semloom_filter_exact_apply_completion,
	.build_task = semloom_filter_build_task,
};

static bool
semloom_filter_build_task(const SemloomOperatorMachine *machine,
						 const SemloomBoundValue *input,
						 uint8_t *destination,
						 size_t destination_length,
						 size_t *written_length)
{
	static const char directive[] = SEMLOOM_FILTER_SYSTEM_DIRECTIVE;
	static const char separator[] = SEMLOOM_FILTER_INSTRUCTION_SEPARATOR;
	SemloomMessagePart system_parts[] = {
		{(const uint8_t *) directive, sizeof(directive) - 1},
		{(const uint8_t *) separator, sizeof(separator) - 1},
		{machine->instruction, machine->instruction_length},
	};
	SemloomMessagePart user = {input->data, input->length};

	return semloom_message_write(system_parts,
								 sizeof(system_parts) / sizeof(system_parts[0]),
								 user, destination, destination_length, written_length);
}

static SemloomTupleDisposition
semloom_filter_handle_null(void)
{
	return SEMLOOM_TUPLE_DROP;
}

static SemloomTupleDisposition
semloom_filter_recording_apply_completion(
	const SemloomMachineCompletion *completion)
{
	if (completion == NULL || completion->is_null)
		return SEMLOOM_TUPLE_DROP;
	if (completion->length == 4 && memcmp(completion->data, "true", 4) == 0)
		return SEMLOOM_TUPLE_EMIT;
	if ((completion->length == 5 &&
		 memcmp(completion->data, "false", 5) == 0) ||
		(completion->length == 7 &&
		 memcmp(completion->data, "unknown", 7) == 0))
		return SEMLOOM_TUPLE_DROP;
	return SEMLOOM_TUPLE_INVALID_COMPLETION;
}

static SemloomTupleDisposition
semloom_filter_exact_apply_completion(
	const SemloomMachineCompletion *completion)
{
	if (completion == NULL || completion->is_null ||
		(completion->length > 0 && completion->data == NULL))
		return SEMLOOM_TUPLE_INVALID_COMPLETION;
	if (completion->length == 4 && memcmp(completion->data, "TRUE", 4) == 0)
		return SEMLOOM_TUPLE_EMIT;
	if ((completion->length == 5 &&
		 memcmp(completion->data, "FALSE", 5) == 0) ||
		(completion->length == 7 &&
		 memcmp(completion->data, "UNKNOWN", 7) == 0))
		return SEMLOOM_TUPLE_DROP;
	return SEMLOOM_TUPLE_INVALID_COMPLETION;
}
