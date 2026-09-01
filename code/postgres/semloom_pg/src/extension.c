#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "parser/parse_func.h"
#include "utils/guc.h"

#include "semloom_pg.h"

PG_MODULE_MAGIC;

static create_upper_paths_hook_type previous_create_upper_paths_hook = NULL;
static set_rel_pathlist_hook_type previous_set_rel_pathlist_hook = NULL;
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

void _PG_init(void);
void _PG_fini(void);

Oid
semloom_map_function_oid(void)
{
	List *qualified_name = list_make2(makeString("ai_semantic"), makeString("map"));
	Oid argument_types[1] = {TEXTOID};

	return LookupFuncName(qualified_name, lengthof(argument_types), argument_types, true);
}

Oid
semloom_filter_function_oid(void)
{
	List *qualified_name = list_make2(makeString("ai_semantic"), makeString("filter"));
	Oid argument_types[1] = {TEXTOID};

	return LookupFuncName(qualified_name, lengthof(argument_types), argument_types, true);
}

Oid
semloom_exact_filter_function_oid(void)
{
	List *qualified_name = list_make2(makeString("ai_semantic"), makeString("filter"));
	Oid argument_types[3] = {TEXTOID, TEXTOID, JSONBOID};

	return LookupFuncName(qualified_name, lengthof(argument_types), argument_types, true);
}

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

bool
semloom_is_map_function(Oid function_oid)
{
	Oid marker_oid = semloom_map_function_oid();

	return OidIsValid(marker_oid) && function_oid == marker_oid;
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
}

void
_PG_fini(void)
{
	if (create_upper_paths_hook == semloom_create_upper_paths)
		create_upper_paths_hook = previous_create_upper_paths_hook;
	if (set_rel_pathlist_hook == semloom_set_rel_pathlist)
		set_rel_pathlist_hook = previous_set_rel_pathlist_hook;
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
