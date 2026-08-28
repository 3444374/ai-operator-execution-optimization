#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"

#include "semloom_pg.h"

typedef struct MarkerCountContext
{
	Oid marker_oid;
	int count;
} MarkerCountContext;

static bool semloom_count_marker(Node *node, void *context);
static int semloom_marker_count(Node *node, Oid marker_oid);
static FuncExpr *semloom_supported_marker(Query *parse, Oid marker_oid);
static void semloom_validate_query_shape(PlannerInfo *root, Oid marker_oid);
static Path *semloom_wrap_final_path(PlannerInfo *root,
									 RelOptInfo *input_rel,
									 RelOptInfo *output_rel,
									 Path *final_path);
static CustomPath *semloom_make_path(RelOptInfo *parent_rel, Path *child_path);
static Plan *semloom_plan_path(PlannerInfo *root,
								RelOptInfo *rel,
								CustomPath *best_path,
								List *target_list,
								List *clauses,
								List *custom_plans);
static Node *semloom_replace_marker(Node *node, void *context);
static void semloom_replace_marker_in_plan(Plan *plan, Oid marker_oid);

static const CustomPathMethods semloom_path_methods = {
	.CustomName = SEMLOOM_CUSTOM_SCAN_NAME,
	.PlanCustomPath = semloom_plan_path,
};

static bool
semloom_count_marker(Node *node, void *context)
{
	MarkerCountContext *marker_context = (MarkerCountContext *) context;

	if (node == NULL)
		return false;
	if (IsA(node, FuncExpr) && ((FuncExpr *) node)->funcid == marker_context->marker_oid)
		marker_context->count++;

	return expression_tree_walker(node, semloom_count_marker, context);
}

static int
semloom_marker_count(Node *node, Oid marker_oid)
{
	MarkerCountContext context = {
		.marker_oid = marker_oid,
		.count = 0,
	};

	semloom_count_marker(node, &context);
	return context.count;
}

static FuncExpr *
semloom_supported_marker(Query *parse, Oid marker_oid)
{
	FuncExpr *supported = NULL;
	ListCell *cell;

	foreach(cell, parse->targetList)
	{
		TargetEntry *entry = lfirst_node(TargetEntry, cell);
		int count = semloom_marker_count((Node *) entry->expr, marker_oid);

		if (count == 0)
			continue;
		if (count != 1 || !IsA(entry->expr, FuncExpr) ||
			((FuncExpr *) entry->expr)->funcid != marker_oid)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("nested ai_semantic.map expressions are not supported")));
		if (supported != NULL || entry->resjunk)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("the SemMap capability supports exactly one visible marker")));

		supported = (FuncExpr *) entry->expr;
	}

	return supported;
}

static void
semloom_validate_query_shape(PlannerInfo *root, Oid marker_oid)
{
	Query *parse = root->parse;
	FuncExpr *marker = semloom_supported_marker(parse, marker_oid);
	RangeTblRef *range_reference;
	RangeTblEntry *range_entry;

	if (marker == NULL)
		return;
	if (root->query_level != 1 ||
		(parse->commandType != CMD_SELECT && parse->commandType != CMD_INSERT) ||
		parse->setOperations != NULL || parse->cteList != NIL || parse->hasAggs ||
		parse->groupClause != NIL || parse->groupingSets != NIL || parse->havingQual != NULL ||
		parse->hasWindowFuncs || parse->windowClause != NIL || parse->distinctClause != NIL ||
		parse->sortClause != NIL || parse->rowMarks != NIL || parse->hasTargetSRFs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("query shape is outside the current SemMap capability"),
				 errdetail("Only a single-table SELECT or INSERT ... SELECT with ordinary filters, projections, and LIMIT is supported.")));
	if (parse->commandType == CMD_INSERT &&
		(parse->resultRelation == 0 || parse->onConflict != NULL ||
		 parse->returningList != NIL || parse->override != OVERRIDING_NOT_SET))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("INSERT shape is outside the current SemMap capability"),
				 errdetail("ON CONFLICT, RETURNING, and OVERRIDING are not supported.")));
	if (parse->jointree == NULL || list_length(parse->jointree->fromlist) != 1 ||
		!IsA(linitial(parse->jointree->fromlist), RangeTblRef))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the SemMap capability requires exactly one base relation")));

	range_reference = linitial_node(RangeTblRef, parse->jointree->fromlist);
	range_entry = rt_fetch(range_reference->rtindex, parse->rtable);
	if (range_entry->rtekind != RTE_RELATION || range_entry->inh || range_entry->tablesample != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the SemMap capability requires one non-inherited table")));
	if (semloom_marker_count(parse->jointree->quals, marker_oid) != 0 ||
		semloom_marker_count(parse->limitOffset, marker_oid) != 0 ||
		semloom_marker_count(parse->limitCount, marker_oid) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ai_semantic.map is only supported as a top-level output expression")));
	if (list_length(marker->args) != 1 || exprType(linitial(marker->args)) != TEXTOID ||
		marker->funcresulttype != TEXTOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("ai_semantic.map capability requires one text input and text output")));
}

void
semloom_add_sem_map_paths(PlannerInfo *root,
						   UpperRelationKind stage,
						   RelOptInfo *input_rel,
						   RelOptInfo *output_rel)
{
	Oid marker_oid;
	List *semantic_paths = NIL;
	ListCell *cell;

	if (stage != UPPERREL_FINAL)
		return;
	marker_oid = semloom_map_function_oid();
	if (!OidIsValid(marker_oid) ||
		semloom_marker_count((Node *) root->parse->targetList, marker_oid) == 0)
		return;

	semloom_validate_query_shape(root, marker_oid);
	foreach(cell, output_rel->pathlist)
	{
		Path *final_path = lfirst_node(Path, cell);

		semantic_paths = lappend(semantic_paths,
								 semloom_wrap_final_path(root,
													 input_rel,
													 output_rel,
													 final_path));
	}
	if (semantic_paths == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SemMap lowering found no ordinary child path")));

	output_rel->pathlist = semantic_paths;
	output_rel->partial_pathlist = NIL;
}

static Path *
semloom_wrap_final_path(PlannerInfo *root,
						RelOptInfo *input_rel,
						RelOptInfo *output_rel,
						Path *final_path)
{
	if (root->parse->commandType == CMD_SELECT)
		return (Path *) semloom_make_path(output_rel, final_path);

	if (root->parse->commandType == CMD_INSERT)
	{
		ModifyTablePath *modify_path;
		Path *source_path;
		CustomPath *semantic_path;

		if (!IsA(final_path, ModifyTablePath))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("SemMap INSERT path is not a ModifyTable path")));
		modify_path = castNode(ModifyTablePath, final_path);
		if (modify_path->operation != CMD_INSERT || modify_path->subpath == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("invalid SemMap INSERT path state")));

		source_path = modify_path->subpath;
		if (source_path->parent != input_rel)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("SemMap INSERT source path has an unexpected parent")));
		semantic_path = semloom_make_path(input_rel, source_path);
		modify_path->subpath = (Path *) semantic_path;
		modify_path->path.total_cost +=
			semantic_path->path.total_cost - source_path->total_cost;
		return &modify_path->path;
	}

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("unexpected SemMap command type")));
	return NULL;
}

static CustomPath *
semloom_make_path(RelOptInfo *parent_rel, Path *child_path)
{
	CustomPath *path = makeNode(CustomPath);

	if (child_path->param_info != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parameterized SemMap paths are not supported")));

	path->path.pathtype = T_CustomScan;
	path->path.parent = parent_rel;
	path->path.pathtarget = child_path->pathtarget;
	path->path.param_info = NULL;
	path->path.parallel_aware = false;
	path->path.parallel_safe = false;
	path->path.parallel_workers = 0;
	path->path.rows = child_path->rows;
	path->path.disabled_nodes = child_path->disabled_nodes;
	path->path.startup_cost = child_path->startup_cost;
	path->path.total_cost = child_path->total_cost + cpu_operator_cost * child_path->rows;
	path->path.pathkeys = child_path->pathkeys;
	path->flags = CUSTOMPATH_SUPPORT_PROJECTION;
	path->custom_paths = list_make1(child_path);
	path->custom_restrictinfo = NIL;
	path->custom_private = NIL;
	path->methods = &semloom_path_methods;

	return path;
}

static Plan *
semloom_plan_path(PlannerInfo *root,
				   RelOptInfo *rel,
				   CustomPath *best_path,
				   List *target_list,
				   List *clauses,
				   List *custom_plans)
{
	Oid marker_oid = semloom_map_function_oid();
	CustomScan *scan = makeNode(CustomScan);
	List *scan_target_list;
	List *mapped_columns = NIL;
	ListCell *cell;

	if (!OidIsValid(marker_oid) || list_length(custom_plans) != 1 || clauses != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid SemMap custom path state")));

	semloom_replace_marker_in_plan(linitial_node(Plan, custom_plans), marker_oid);
	scan_target_list = (List *) semloom_replace_marker((Node *) target_list, &marker_oid);
	foreach(cell, target_list)
	{
		TargetEntry *entry = lfirst_node(TargetEntry, cell);

		if (IsA(entry->expr, FuncExpr) &&
			((FuncExpr *) entry->expr)->funcid == marker_oid)
			mapped_columns = lappend_int(mapped_columns, entry->resno);
	}
	if (list_length(mapped_columns) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SemMap plan lost its mapped output identity")));

	/*
	 * Leave these expressions in planner form.  set_customscan_references()
	 * matches them against custom_scan_tlist and creates INDEX_VAR references
	 * after the complete plan tree is available.
	 */
	scan->scan.plan.targetlist = copyObject(scan_target_list);
	scan->scan.plan.qual = NIL;
	scan->scan.scanrelid = 0;
	scan->flags = CUSTOMPATH_SUPPORT_PROJECTION;
	scan->custom_plans = custom_plans;
	scan->custom_exprs = NIL;
	scan->custom_private = mapped_columns;
	scan->custom_scan_tlist = scan_target_list;
	scan->methods = &semloom_scan_methods;

	return &scan->scan.plan;
}

static Node *
semloom_replace_marker(Node *node, void *context)
{
	Oid marker_oid = *((Oid *) context);

	if (node == NULL)
		return NULL;
	if (IsA(node, FuncExpr) && ((FuncExpr *) node)->funcid == marker_oid)
	{
		FuncExpr *marker = (FuncExpr *) node;

		if (list_length(marker->args) != 1)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("invalid SemMap marker arguments during plan lowering")));
		return copyObject(linitial(marker->args));
	}

	return expression_tree_mutator(node, semloom_replace_marker, context);
}

static void
semloom_replace_marker_in_plan(Plan *plan, Oid marker_oid)
{
	if (plan == NULL)
		return;
	plan->targetlist = (List *) semloom_replace_marker((Node *) plan->targetlist, &marker_oid);
	plan->qual = (List *) semloom_replace_marker((Node *) plan->qual, &marker_oid);
	semloom_replace_marker_in_plan(plan->lefttree, marker_oid);
	semloom_replace_marker_in_plan(plan->righttree, marker_oid);
}
