/*
 * Thin PostgreSQL CustomScan callback adapter.
 *
 * It forwards begin/next/stop/explain to SemloomExecPump and retains only the
 * executor callbacks PostgreSQL requires, including explicit rescan/EPQ errors.
 * Plan: experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md.
 */
#include "postgres.h"

#include "executor/execScan.h"

#include "sem_pump.h"
#include "semloom_pg.h"

typedef struct SemloomScanState
{
	CustomScanState custom_state;
	SemloomExecPump *pump;
	const char *capability_name;
} SemloomScanState;

static Node *semloom_create_map_scan_state(CustomScan *scan);
static Node *semloom_create_filter_scan_state(CustomScan *scan);
static Node *semloom_create_scan_state(const CustomExecMethods *methods,
									  const char *capability_name);
static void semloom_begin_scan(CustomScanState *node, EState *estate, int executor_flags);
static TupleTableSlot *semloom_execute_scan(CustomScanState *node);
static TupleTableSlot *semloom_next_tuple(ScanState *scan_state);
static bool semloom_recheck_tuple(ScanState *scan_state, TupleTableSlot *slot);
static void semloom_end_scan(CustomScanState *node);
static void semloom_rescan(CustomScanState *node);
static void semloom_explain_scan(CustomScanState *node,
								 List *ancestors,
								 ExplainState *explain_state);

static const CustomExecMethods semloom_map_exec_methods = {
	.CustomName = SEMLOOM_MAP_CUSTOM_SCAN_NAME,
	.BeginCustomScan = semloom_begin_scan,
	.ExecCustomScan = semloom_execute_scan,
	.EndCustomScan = semloom_end_scan,
	.ReScanCustomScan = semloom_rescan,
	.ExplainCustomScan = semloom_explain_scan,
};

static const CustomExecMethods semloom_filter_exec_methods = {
	.CustomName = SEMLOOM_FILTER_CUSTOM_SCAN_NAME,
	.BeginCustomScan = semloom_begin_scan,
	.ExecCustomScan = semloom_execute_scan,
	.EndCustomScan = semloom_end_scan,
	.ReScanCustomScan = semloom_rescan,
	.ExplainCustomScan = semloom_explain_scan,
};

const CustomScanMethods semloom_map_scan_methods = {
	.CustomName = SEMLOOM_MAP_CUSTOM_SCAN_NAME,
	.CreateCustomScanState = semloom_create_map_scan_state,
};

const CustomScanMethods semloom_filter_scan_methods = {
	.CustomName = SEMLOOM_FILTER_CUSTOM_SCAN_NAME,
	.CreateCustomScanState = semloom_create_filter_scan_state,
};

static Node *
semloom_create_map_scan_state(CustomScan *scan)
{
	(void) scan;
	return semloom_create_scan_state(&semloom_map_exec_methods, "SemMap");
}

static Node *
semloom_create_filter_scan_state(CustomScan *scan)
{
	(void) scan;
	return semloom_create_scan_state(&semloom_filter_exec_methods, "SemFilter");
}

static Node *
semloom_create_scan_state(const CustomExecMethods *methods,
						  const char *capability_name)
{
	SemloomScanState *state = palloc0(sizeof(*state));

	NodeSetTag(&state->custom_state, T_CustomScanState);
	state->custom_state.methods = methods;
	state->capability_name = capability_name;
	return (Node *) state;
}

static void
semloom_begin_scan(CustomScanState *node, EState *estate, int executor_flags)
{
	SemloomScanState *state = (SemloomScanState *) node;

	state->pump = semloom_pump_begin(node, estate, executor_flags);
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

	return semloom_pump_next(state->pump, scan_state);
}

static bool
semloom_recheck_tuple(ScanState *scan_state, TupleTableSlot *slot)
{
	SemloomScanState *state = (SemloomScanState *) scan_state;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("EvalPlanQual is not supported by the %s capability",
					state->capability_name)));
	return false;
}

static void
semloom_end_scan(CustomScanState *node)
{
	SemloomScanState *state = (SemloomScanState *) node;

	semloom_pump_stop(state->pump, node);
	state->pump = NULL;
}

static void
semloom_rescan(CustomScanState *node)
{
	SemloomScanState *state = (SemloomScanState *) node;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("rescan is not supported by the %s capability",
					state->capability_name)));
}

static void
semloom_explain_scan(CustomScanState *node,
					 List *ancestors,
					 ExplainState *explain_state)
{
	SemloomScanState *state = (SemloomScanState *) node;

	semloom_pump_explain(state->pump, explain_state);
}
