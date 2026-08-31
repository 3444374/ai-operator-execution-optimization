/* SemFilter's exact recording and model-output parsers. */
#include <stddef.h>
#include <string.h>

#include "sem_operator_machine.h"

static SemloomTupleDisposition semloom_filter_handle_null(void);
static SemloomTupleDisposition semloom_filter_recording_apply_completion(
	const SemloomMachineCompletion *completion);
static SemloomTupleDisposition semloom_filter_exact_apply_completion(
	const SemloomMachineCompletion *completion);

const SemloomOperatorMachineMethods semloom_filter_recording_machine_methods = {
	.input_explain_property = "Filter Input Column",
	.handle_null = semloom_filter_handle_null,
	.apply_completion = semloom_filter_recording_apply_completion,
};

const SemloomOperatorMachineMethods semloom_filter_exact_machine_methods = {
	.input_explain_property = "Filter Input Column",
	.handle_null = semloom_filter_handle_null,
	.apply_completion = semloom_filter_exact_apply_completion,
};

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
