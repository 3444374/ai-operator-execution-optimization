/* SemFilter's exact TRUE/FALSE/UNKNOWN row-cardinality semantics. */
#include "postgres.h"

#include "sem_operator_machine.h"

static SemloomTupleDisposition semloom_filter_handle_null(
	TupleTableSlot *slot,
	AttrNumber input_column);
static SemloomTupleDisposition semloom_filter_apply_completion(
	TupleTableSlot *slot,
	AttrNumber input_column,
	const PgSemanticCompletion *completion,
	MemoryContext result_context);

const SemloomOperatorMachineMethods semloom_filter_machine_methods = {
	.input_explain_property = "Filter Input Column",
	.invalid_completion_message =
		"SemFilter provider completion must be true, false, or unknown",
	.handle_null = semloom_filter_handle_null,
	.apply_completion = semloom_filter_apply_completion,
};

static SemloomTupleDisposition
semloom_filter_handle_null(TupleTableSlot *slot, AttrNumber input_column)
{
	Assert(slot != NULL);
	Assert(input_column > 0);
	Assert(slot->tts_isnull[input_column - 1]);
	return SEMLOOM_TUPLE_DROP;
}

static SemloomTupleDisposition
semloom_filter_apply_completion(TupleTableSlot *slot,
								AttrNumber input_column,
								const PgSemanticCompletion *completion,
								MemoryContext result_context)
{
	(void) slot;
	(void) input_column;
	(void) result_context;
	Assert(completion != NULL);
	if (completion->is_null)
		return SEMLOOM_TUPLE_DROP;
	if (completion->length == 4 &&
		memcmp(completion->data, "true", 4) == 0)
		return SEMLOOM_TUPLE_EMIT;
	if ((completion->length == 5 &&
		 memcmp(completion->data, "false", 5) == 0) ||
		(completion->length == 7 &&
		 memcmp(completion->data, "unknown", 7) == 0))
		return SEMLOOM_TUPLE_DROP;
	return SEMLOOM_TUPLE_INVALID_COMPLETION;
}
