/* SemMap's row-preserving completion interpretation. */
#include "postgres.h"

#include "utils/builtins.h"

#include "sem_operator_machine.h"

static SemloomTupleDisposition semloom_map_handle_null(
	TupleTableSlot *slot,
	AttrNumber input_column);
static SemloomTupleDisposition semloom_map_apply_completion(
	TupleTableSlot *slot,
	AttrNumber input_column,
	const PgSemanticCompletion *completion,
	MemoryContext result_context);

const SemloomOperatorMachineMethods semloom_map_machine_methods = {
	.input_explain_property = "Mapped Column",
	.invalid_completion_message = "SemMap provider returned an invalid completion",
	.handle_null = semloom_map_handle_null,
	.apply_completion = semloom_map_apply_completion,
};

static SemloomTupleDisposition
semloom_map_handle_null(TupleTableSlot *slot, AttrNumber input_column)
{
	Assert(slot != NULL);
	Assert(input_column > 0);
	Assert(slot->tts_isnull[input_column - 1]);
	return SEMLOOM_TUPLE_EMIT;
}

static SemloomTupleDisposition
semloom_map_apply_completion(TupleTableSlot *slot,
							 AttrNumber input_column,
							 const PgSemanticCompletion *completion,
							 MemoryContext result_context)
{
	const char *output_data;
	MemoryContext previous_context;
	text *output_text = NULL;

	Assert(slot != NULL);
	Assert(completion != NULL);
	if (completion->is_null)
	{
		slot->tts_isnull[input_column - 1] = true;
		slot->tts_values[input_column - 1] = (Datum) 0;
		return SEMLOOM_TUPLE_EMIT;
	}

	output_data = completion->length == 0 ? "" : (const char *) completion->data;
	previous_context = MemoryContextSwitchTo(result_context);
	PG_TRY();
	{
		output_text = cstring_to_text_with_len(output_data, completion->length);
		MemoryContextSwitchTo(previous_context);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(previous_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	slot->tts_isnull[input_column - 1] = false;
	slot->tts_values[input_column - 1] = PointerGetDatum(output_text);
	return SEMLOOM_TUPLE_EMIT;
}
