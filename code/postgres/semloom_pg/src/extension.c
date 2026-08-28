#include "postgres.h"

#include "fmgr.h"
#include "optimizer/planner.h"
#include "parser/parse_func.h"

#include "semloom_pg.h"

PG_MODULE_MAGIC;

static create_upper_paths_hook_type previous_create_upper_paths_hook = NULL;

static void semloom_create_upper_paths(PlannerInfo *root,
									   UpperRelationKind stage,
									   RelOptInfo *input_rel,
									   RelOptInfo *output_rel,
									   void *extra);

void _PG_init(void);
void _PG_fini(void);

Oid
semloom_map_function_oid(void)
{
	List *qualified_name = list_make2(makeString("ai_semantic"), makeString("map"));
	Oid argument_types[1] = {TEXTOID};

	return LookupFuncName(qualified_name, lengthof(argument_types), argument_types, true);
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
	RegisterCustomScanMethods(&semloom_scan_methods);
	previous_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = semloom_create_upper_paths;
}

void
_PG_fini(void)
{
	if (create_upper_paths_hook == semloom_create_upper_paths)
		create_upper_paths_hook = previous_create_upper_paths_hook;
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
