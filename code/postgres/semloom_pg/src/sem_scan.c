#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "executor/execScan.h"

#include "semloom_pg.h"

typedef struct SemloomScanState
{
	CustomScanState custom_state;
	PlanState *child_state;
	SemloomProviderSession *provider_session;
	SemloomSemanticPlanSpec plan_spec;
	AttrNumber mapped_column;
} SemloomScanState;

static Node *semloom_create_scan_state(CustomScan *scan);
static void semloom_begin_scan(CustomScanState *node, EState *estate, int executor_flags);
static TupleTableSlot *semloom_execute_scan(CustomScanState *node);
static TupleTableSlot *semloom_next_tuple(ScanState *scan_state);
static void semloom_open_provider(SemloomScanState *state);
static bool semloom_recheck_tuple(ScanState *scan_state, TupleTableSlot *slot);
static void semloom_end_scan(CustomScanState *node);
static void semloom_rescan(CustomScanState *node);
static void semloom_explain_scan(CustomScanState *node,
								 List *ancestors,
								 ExplainState *explain_state);

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
	state->plan_spec.operator_kind = SEMLOOM_OPERATOR_MAP;
	state->plan_spec.input_type = TEXTOID;
	state->plan_spec.output_type = TEXTOID;
	state->plan_spec.null_policy = SEMLOOM_NULL_PROPAGATE;
	state->plan_spec.error_policy = SEMLOOM_ERROR_FAIL_QUERY;
	state->plan_spec.semantic_spec_version = SEMLOOM_RECORDING_SPEC_VERSION;
	state->plan_spec.semantic_spec_id = SEMLOOM_RECORDING_SPEC_ID;
	state->plan_spec.physical_algorithm = SEMLOOM_RECORDING_ALGORITHM;

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
	SemloomPreparedSemanticTask task;
	SemloomCompletionRecord completion;
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

		if (attribute_index + 1 == state->mapped_column)
		{
			if (is_null)
			{
				if (state->plan_spec.null_policy != SEMLOOM_NULL_PROPAGATE)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("SemMap has an unsupported NULL policy")));
				scan_slot->tts_isnull[attribute_index] = true;
				scan_slot->tts_values[attribute_index] = (Datum) 0;
				continue;
			}
			if (state->provider_session == NULL)
				semloom_open_provider(state);
			task.sequence = semloom_provider_accepted_rows(state->provider_session);
			task.input_type = TEXTOID;
			task.input = child_slot->tts_values[attribute_index];
			task.is_null = is_null;
			semloom_provider_drive(state->provider_session,
								   &task,
								   state->custom_state.ss.ps.ps_ExprContext->ecxt_per_tuple_memory,
								   &completion);
			scan_slot->tts_isnull[attribute_index] = completion.is_null;
			scan_slot->tts_values[attribute_index] = completion.output;
		}
		else
		{
			scan_slot->tts_isnull[attribute_index] = is_null;
			scan_slot->tts_values[attribute_index] = child_slot->tts_values[attribute_index];
		}
	}

	return ExecStoreVirtualTuple(scan_slot);
}

static void
semloom_open_provider(SemloomScanState *state)
{
	MemoryContext owner_context = state->custom_state.ss.ps.state->es_query_cxt;
	MemoryContext previous_context = MemoryContextSwitchTo(owner_context);

	PG_TRY();
	{
		state->provider_session = semloom_provider_open(&state->plan_spec);
		MemoryContextSwitchTo(previous_context);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(previous_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
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

	if (state->provider_session != NULL)
	{
		semloom_provider_close(state->provider_session);
		state->provider_session = NULL;
	}
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

	ExplainPropertyText("Provider", semloom_provider_name(state->provider_session), explain_state);
	ExplainPropertyText("Physical Role", "reference", explain_state);
	ExplainPropertyInteger("Mapped Column", NULL, state->mapped_column, explain_state);
	if (explain_state->analyze)
	{
		ExplainPropertyInteger("Accepted Rows", NULL,
							   semloom_provider_accepted_rows(state->provider_session), explain_state);
		ExplainPropertyInteger("Emitted Rows", NULL,
							   semloom_provider_emitted_rows(state->provider_session), explain_state);
	}
}
