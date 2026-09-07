#include "postgres.h"

#include "fmgr.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "utils/guc.h"

#include "extension_config.h"
#include "planner/paths.h"
#include "executor/sem_scan.h"

PG_MODULE_MAGIC;

static create_upper_paths_hook_type previous_create_upper_paths_hook = NULL;
static set_rel_pathlist_hook_type previous_set_rel_pathlist_hook = NULL;
static planner_hook_type previous_planner_hook = NULL;
/* Scoped to one planner invocation, including nested planning and ERROR. */
static int generate_map_source_level = 0;
static char *semloom_gateway_socket = NULL;
static char *semloom_reference_calibration_file = NULL;
static int semloom_execution_profile = SEMLOOM_PROVIDER_PROFILE_GOLDEN;
static const struct config_enum_entry semloom_execution_profile_options[] = {
	{"golden", SEMLOOM_PROVIDER_PROFILE_GOLDEN, false},
	{"openai-compatible-fixed",
	 SEMLOOM_PROVIDER_PROFILE_OPENAI_COMPATIBLE_FIXED,
	 false},
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
