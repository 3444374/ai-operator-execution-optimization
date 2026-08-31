/* SemMap's row-preserving result interpretation. */
#include "sem_operator_machine.h"

static SemloomTupleDisposition semloom_map_handle_null(void);
static SemloomTupleDisposition semloom_map_apply_completion(
	const SemloomMachineCompletion *completion);

const SemloomOperatorMachineMethods semloom_map_machine_methods = {
	.input_explain_property = "Mapped Column",
	.handle_null = semloom_map_handle_null,
	.apply_completion = semloom_map_apply_completion,
};

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
