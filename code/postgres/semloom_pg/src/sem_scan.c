#include "postgres.h"

#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "executor/execScan.h"
#include "utils/memutils.h"

#include "semloom_pg.h"

typedef struct SemloomScanState
{
	CustomScanState custom_state;
	PlanState *child_state;
	AttrNumber mapped_column;
	uint64 accepted_rows;
	uint64 emitted_rows;
} SemloomScanState;

static Node *semloom_create_scan_state(CustomScan *scan);
static void semloom_begin_scan(CustomScanState *node, EState *estate, int executor_flags);
static TupleTableSlot *semloom_execute_scan(CustomScanState *node);
static TupleTableSlot *semloom_next_tuple(ScanState *scan_state);
static bool semloom_recheck_tuple(ScanState *scan_state, TupleTableSlot *slot);
static void semloom_end_scan(CustomScanState *node);
static void semloom_rescan(CustomScanState *node);
static void semloom_explain_scan(CustomScanState *node,
								 List *ancestors,
								 ExplainState *explain_state);
static Datum semloom_record_text(Datum input, ExprContext *expression_context);

static const CustomExecMethods semloom_exec_methods = {
	.CustomName = SEMLOOM_CUSTOM_SCAN_NAME,
	.BeginCustomScan = semloom_begin_scan,
	.ExecCustomScan = semloom_execute_scan,
	.EndCustomScan = semloom_end_scan,
	.ReScanCustomScan = semloom_rescan,
	.ExplainCustomScan = semloom_explain_scan,
};

const CustomScanMethods semloom_scan_methods = {
	.CustomName = SEMLOOM_CUSTOM_SCAN_NAME,
	.CreateCustomScanState = semloom_create_scan_state,
};

static Node *
semloom_create_scan_state(CustomScan *scan)
{
	SemloomScanState *state = palloc0(sizeof(SemloomScanState));

	NodeSetTag(&state->custom_state, T_CustomScanState);
	state->custom_state.methods = &semloom_exec_methods;
	return (Node *) state;
}

static void
semloom_begin_scan(CustomScanState *node, EState *estate, int executor_flags)
{
	SemloomScanState *state = (SemloomScanState *) node;
	CustomScan *scan = castNode(CustomScan, node->ss.ps.plan);
	int unsupported_flags = EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK | EXEC_FLAG_REWIND;

	if ((executor_flags & unsupported_flags) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("SemMap capability supports forward execution only")));
	if (list_length(scan->custom_plans) != 1 || list_length(scan->custom_private) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid SemMap executor state")));

	state->mapped_column = linitial_int(scan->custom_private);
	if (state->mapped_column <= 0 ||
		state->mapped_column > node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SemMap mapped output is outside the scan tuple")));

	state->child_state = ExecInitNode(linitial_node(Plan, scan->custom_plans), estate, executor_flags);
	node->custom_ps = list_make1(state->child_state);
}

static TupleTableSlot *
semloom_execute_scan(CustomScanState *node)
{
	return ExecScan(&node->ss, semloom_next_tuple, semloom_recheck_tuple);
}

static TupleTableSlot *
semloom_next_tuple(ScanState *scan_state)
{
	SemloomScanState *state = (SemloomScanState *) scan_state;
	TupleTableSlot *child_slot = ExecProcNode(state->child_state);
	TupleTableSlot *scan_slot = scan_state->ss_ScanTupleSlot;
	int attribute_index;

	if (TupIsNull(child_slot))
		return ExecClearTuple(scan_slot);
	slot_getallattrs(child_slot);
	if (child_slot->tts_tupleDescriptor->natts != scan_slot->tts_tupleDescriptor->natts)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SemMap child and scan tuple descriptors do not match")));

	ExecClearTuple(scan_slot);
	for (attribute_index = 0; attribute_index < scan_slot->tts_tupleDescriptor->natts;
		 attribute_index++)
	{
		bool is_null = child_slot->tts_isnull[attribute_index];

		scan_slot->tts_isnull[attribute_index] = is_null;
		if (is_null)
			scan_slot->tts_values[attribute_index] = (Datum) 0;
		else if (attribute_index + 1 == state->mapped_column)
			scan_slot->tts_values[attribute_index] =
				semloom_record_text(child_slot->tts_values[attribute_index],
									 state->custom_state.ss.ps.ps_ExprContext);
		else
			scan_slot->tts_values[attribute_index] = child_slot->tts_values[attribute_index];
	}

	state->accepted_rows++;
	state->emitted_rows++;
	return ExecStoreVirtualTuple(scan_slot);
}

static bool
semloom_recheck_tuple(ScanState *scan_state, TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("EvalPlanQual is not supported by the SemMap capability")));
	return false;
}

static void
semloom_end_scan(CustomScanState *node)
{
	SemloomScanState *state = (SemloomScanState *) node;

	if (state->child_state != NULL)
	{
		ExecEndNode(state->child_state);
		state->child_state = NULL;
	}
	node->custom_ps = NIL;
}

static void
semloom_rescan(CustomScanState *node)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("rescan is not supported by the SemMap capability")));
}

static void
semloom_explain_scan(CustomScanState *node,
						 List *ancestors,
						 ExplainState *explain_state)
{
	SemloomScanState *state = (SemloomScanState *) node;

	ExplainPropertyText("Provider", "in-process-recording", explain_state);
	ExplainPropertyText("Physical Role", "reference", explain_state);
	ExplainPropertyInteger("Mapped Column", NULL, state->mapped_column, explain_state);
	if (explain_state->analyze)
	{
		ExplainPropertyInteger("Accepted Rows", NULL, state->accepted_rows, explain_state);
		ExplainPropertyInteger("Emitted Rows", NULL, state->emitted_rows, explain_state);
	}
}

static Datum
semloom_record_text(Datum input, ExprContext *expression_context)
{
	text *input_text = DatumGetTextPP(input);
	Size prefix_length = strlen(SEMLOOM_RECORDING_PREFIX);
	Size input_length = VARSIZE_ANY_EXHDR(input_text);
	MemoryContext previous_context;
	text *output_text;

	previous_context = MemoryContextSwitchTo(expression_context->ecxt_per_tuple_memory);
	output_text = (text *) palloc(VARHDRSZ + prefix_length + input_length);
	SET_VARSIZE(output_text, VARHDRSZ + prefix_length + input_length);
	memcpy(VARDATA(output_text), SEMLOOM_RECORDING_PREFIX, prefix_length);
	memcpy(VARDATA(output_text) + prefix_length, VARDATA_ANY(input_text), input_length);
	MemoryContextSwitchTo(previous_context);

	return PointerGetDatum(output_text);
}
