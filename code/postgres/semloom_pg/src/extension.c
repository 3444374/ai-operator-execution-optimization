#include "postgres.h"

#include "fmgr.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "utils/guc.h"

#include "extension_config.h"
#include "planner/paths.h"
#include "planner/sem_map_call.h"
#include "executor/sem_scan.h"

PG_MODULE_MAGIC;

static create_upper_paths_hook_type previous_create_upper_paths_hook = NULL;
static set_rel_pathlist_hook_type previous_set_rel_pathlist_hook = NULL;
static planner_hook_type previous_planner_hook = NULL;
/* Scoped to one planner invocation, including nested planning and ERROR. */
static int generate_map_source_level = 0;
static char *semloom_gateway_socket = NULL;
static char *semloom_reference_calibration_file = NULL;
static char *test_map_binding_id_column = NULL;
static char *test_filter_binding_id_column = NULL;
static int semloom_execution_profile = SEMLOOM_PROVIDER_PROFILE_GOLDEN;
static int provider_window_tasks = 2;
static int provider_window_bytes = 8 * 1024 * 1024;
static bool enable_predicate_prefetch = false;
static bool enable_filter_count = false;
static bool enable_total_window_budget = false;
static bool test_window_memory = false;
static int provider_staging_bytes = 16 * 1024 * 1024;
int semloom_provider_window_tasks(void) { return provider_window_tasks; }
int semloom_provider_window_bytes(void) { return provider_window_bytes; }
bool semloom_predicate_prefetch_enabled(void) { return enable_predicate_prefetch; }
bool semloom_filter_count_enabled(void) { return enable_filter_count; }
bool semloom_total_window_budget_enabled(void) { return enable_total_window_budget; }
bool semloom_test_window_memory_enabled(void) { return test_window_memory; }
int semloom_provider_staging_bytes(void) { return provider_staging_bytes; }
const char *semloom_test_map_binding_column(void)
{ return test_map_binding_id_column == NULL ? "" : test_map_binding_id_column; }
const char *semloom_test_filter_binding_column(void)
{ return test_filter_binding_id_column == NULL ? "" : test_filter_binding_id_column; }

static const struct config_enum_entry semloom_execution_profile_options[] = {
	{"golden", SEMLOOM_PROVIDER_PROFILE_GOLDEN, false},
	{"openai-compatible-fixed",
	 SEMLOOM_PROVIDER_PROFILE_OPENAI_COMPATIBLE_FIXED,
	 false},
	{"incremental-map", SEMLOOM_PROVIDER_PROFILE_ASYNC_MAP, false},
	{"query-job", SEMLOOM_PROVIDER_PROFILE_QUERY_JOB, false},
	{NULL, 0, false},
};

static void semloom_create_upper_paths(PlannerInfo *root,
									   UpperRelationKind stage,
									   RelOptInfo *input_rel,
									   RelOptInfo *output_rel,
									   void *extra);
static void semloom_set_rel_pathlist(PlannerInfo *root,
									RelOptInfo *rel,
									Index rti,
									RangeTblEntry *rte);
static PlannedStmt *semloom_planner(Query *parse, const char *query_string,
	int cursor_options, ParamListInfo bound_params);

void _PG_init(void);
void _PG_fini(void);

const char *
semloom_gateway_socket_path(void)
{
	return semloom_gateway_socket == NULL ? "" : semloom_gateway_socket;
}

const char *
semloom_reference_calibration_path(void)
{
	return semloom_reference_calibration_file == NULL ? "" :
		semloom_reference_calibration_file;
}

SemloomProviderExecutionProfile
semloom_provider_execution_profile(void)
{
	return (SemloomProviderExecutionProfile) semloom_execution_profile;
}

const char *
semloom_provider_execution_profile_name(void)
{
	if (semloom_provider_execution_profile() == SEMLOOM_PROVIDER_PROFILE_ASYNC_MAP)
		return "incremental-map";
	return semloom_execution_profile ==
		SEMLOOM_PROVIDER_PROFILE_OPENAI_COMPATIBLE_FIXED ?
		"openai-compatible-fixed" : "golden";
}

void
_PG_init(void)
{
	DefineCustomStringVariable("semloom_pg.gateway_socket",
							   "Unix-domain socket for the external semantic provider.",
							   NULL,
							   &semloom_gateway_socket,
							   "",
							   PGC_SUSET,
							   0,
							   NULL,
							   NULL,
							   NULL);
	DefineCustomIntVariable("semloom_pg.provider_window_tasks", "Maximum retained Map rows.", NULL,
		&provider_window_tasks, 2, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("semloom_pg.provider_window_bytes", "Maximum retained Map row and task bytes.", NULL,
		&provider_window_bytes, 8 * 1024 * 1024, 1024 * 1024, 256 * 1024 * 1024, PGC_USERSET, GUC_UNIT_BYTE, NULL, NULL, NULL);
	DefineCustomBoolVariable("semloom_pg.enable_predicate_prefetch",
		"Allow experimental Map prefetch for explicitly supported ordinary predicates.", NULL,
		&enable_predicate_prefetch, false, PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("semloom_pg.enable_filter_count",
		"Allow experimental single-table COUNT(*) over semantic filters.", NULL,
		&enable_filter_count, false, PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("semloom_pg.enable_total_window_budget",
		"Use a total retained-row budget and one bounded pending row.", NULL,
		&enable_total_window_budget, false, PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("semloom_pg.provider_staging_bytes",
		"Maximum allocation bound for one pending semantic row, including its result buffer.", NULL,
		&provider_staging_bytes, 16 * 1024 * 1024, 65536, 256 * 1024 * 1024,
		PGC_USERSET, GUC_UNIT_BYTE, NULL, NULL, NULL);
	DefineCustomBoolVariable("semloom_pg.test_window_memory",
		"Test-only per-operator window memory cleanup trace.", NULL,
		&test_window_memory, false, PGC_SUSET, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);
	DefineCustomStringVariable("semloom_pg.test_map_binding_id_column",
		"Test-only text row ID column logged before Map task submission; empty disables tracing.",
		NULL, &test_map_binding_id_column, "", PGC_SUSET, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);
	DefineCustomStringVariable("semloom_pg.test_filter_binding_id_column",
		"Test-only text row ID retained for Filter input and decision traces; empty disables tracing.",
		NULL, &test_filter_binding_id_column, "", PGC_SUSET, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);
	DefineCustomEnumVariable("semloom_pg.provider_execution_profile",
							 "Execution profile for exact semantic provider queries.",
							 NULL,
							 &semloom_execution_profile,
							 SEMLOOM_PROVIDER_PROFILE_GOLDEN,
							 semloom_execution_profile_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomStringVariable("semloom_pg.reference_calibration_file",
							   "Planner-side exact SemFilter reference calibration artifact.",
							   NULL,
							   &semloom_reference_calibration_file,
							   "",
							   PGC_SUSET,
							   0,
							   NULL,
							   NULL,
							   NULL);
	RegisterCustomScanMethods(&semloom_map_scan_methods);
	RegisterCustomScanMethods(&semloom_filter_scan_methods);
	previous_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = semloom_create_upper_paths;
	previous_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = semloom_set_rel_pathlist;
	previous_planner_hook = planner_hook;
	planner_hook = semloom_planner;
}

void
_PG_fini(void)
{
	if (create_upper_paths_hook == semloom_create_upper_paths)
		create_upper_paths_hook = previous_create_upper_paths_hook;
	if (set_rel_pathlist_hook == semloom_set_rel_pathlist)
		set_rel_pathlist_hook = previous_set_rel_pathlist_hook;
	if (planner_hook == semloom_planner)
		planner_hook = previous_planner_hook;
}

static PlannedStmt *
semloom_planner(Query *parse, const char *query_string,
				int cursor_options, ParamListInfo bound_params)
{
	int previous_source_level = generate_map_source_level;
	PlannedStmt *result;

	generate_map_source_level = semloom_validate_generate_map_source(parse);
	PG_TRY();
	{
		if (previous_planner_hook != NULL)
			result = previous_planner_hook(parse, query_string, cursor_options, bound_params);
		else
			result = standard_planner(parse, query_string, cursor_options, bound_params);
	}
	PG_FINALLY();
	{
		generate_map_source_level = previous_source_level;
	}
	PG_END_TRY();
	return result;
}

bool
semloom_generate_map_source_checked(int query_level)
{
	return generate_map_source_level != 0 && query_level == generate_map_source_level;
}

static void
semloom_create_upper_paths(PlannerInfo *root,
								   UpperRelationKind stage,
								   RelOptInfo *input_rel,
								   RelOptInfo *output_rel,
								   void *extra)
{
	if (previous_create_upper_paths_hook != NULL)
		previous_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);

	semloom_add_sem_map_paths(root, stage, input_rel, output_rel);
}

static void
semloom_set_rel_pathlist(PlannerInfo *root,
						 RelOptInfo *rel,
						 Index rti,
						 RangeTblEntry *rte)
{
	if (previous_set_rel_pathlist_hook != NULL)
		previous_set_rel_pathlist_hook(root, rel, rti, rte);

	semloom_add_sem_filter_paths(root, rel, rti, rte);
}
