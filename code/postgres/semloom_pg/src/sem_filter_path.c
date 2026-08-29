/* Planner carrier for an exact unary SemFilter reference path. */
#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"

#include "ai_provider_port.h"
#include "sem_path_common.h"
#include "semloom_pg.h"

static FuncExpr *semloom_supported_filter_marker(RelOptInfo *rel,
										  Oid marker_oid);
static void semloom_validate_filter_query_shape(PlannerInfo *root,
										 Oid marker_oid);
static CustomPath *semloom_make_filter_path(PlannerInfo *root,
										 RelOptInfo *rel,
										 Path *child_path,
										 FuncExpr *marker);
static Plan *semloom_plan_filter_path(PlannerInfo *root,
									 RelOptInfo *rel,
									 CustomPath *best_path,
									 List *target_list,
									 List *clauses,
									 List *custom_plans);
static Node *semloom_replace_filter_marker(Node *node, void *context);
static void semloom_replace_filter_marker_in_plan(Plan *plan, Oid marker_oid);

static const CustomPathMethods semloom_filter_path_methods = {
	.CustomName = SEMLOOM_FILTER_CUSTOM_SCAN_NAME,
	.PlanCustomPath = semloom_plan_filter_path,
};

void
semloom_add_sem_filter_paths(PlannerInfo *root,
							 RelOptInfo *rel,
							 Index rti,
							 RangeTblEntry *rte)
{
	Oid marker_oid = semloom_filter_function_oid();
	FuncExpr *marker;
	List *semantic_paths = NIL;
	ListCell *cell;

	(void) rte;
	if (!OidIsValid(marker_oid) || root->parse->jointree == NULL ||
		semloom_marker_count(root->parse->jointree->quals, marker_oid) == 0)
		return;
	semloom_validate_filter_query_shape(root, marker_oid);
	if (root->parse->jointree == NULL ||
		list_length(root->parse->jointree->fromlist) != 1 ||
		!IsA(linitial(root->parse->jointree->fromlist), RangeTblRef) ||
		linitial_node(RangeTblRef, root->parse->jointree->fromlist)->rtindex != rti)
		return;

	marker = semloom_supported_filter_marker(rel, marker_oid);
	foreach(cell, rel->pathlist)
	{
		Path *child_path = lfirst_node(Path, cell);

		semantic_paths = lappend(semantic_paths,
								 semloom_make_filter_path(root,
												  rel,
												  child_path,
												  marker));
	}
	if (semantic_paths == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SemFilter lowering found no ordinary child path")));

	rel->pathlist = semantic_paths;
	rel->partial_pathlist = NIL;
}

static FuncExpr *
semloom_supported_filter_marker(RelOptInfo *rel,
								Oid marker_oid)
{
	FuncExpr *marker = NULL;
	ListCell *cell;

	foreach(cell, rel->baserestrictinfo)
	{
		RestrictInfo *restriction = lfirst_node(RestrictInfo, cell);
		int count = semloom_marker_count((Node *) restriction->clause, marker_oid);

		if (count == 0)
			continue;
		if (count != 1 || !IsA(restriction->clause, FuncExpr) ||
			((FuncExpr *) restriction->clause)->funcid != marker_oid ||
			marker != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ai_semantic.filter must be one top-level AND predicate")));
		marker = (FuncExpr *) restriction->clause;
	}
	if (marker == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ai_semantic.filter must be a base-relation predicate")));
	if (list_length(marker->args) != 1 || exprType(linitial(marker->args)) != TEXTOID ||
		marker->funcresulttype != BOOLOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("ai_semantic.filter capability requires one text input and boolean output")));
	return marker;
}

static void
semloom_validate_filter_query_shape(PlannerInfo *root,
									Oid marker_oid)
{
	Query *parse = root->parse;
	bool insert_source = semloom_is_insert_source(root);
	Oid map_oid = semloom_map_function_oid();
	RangeTblRef *range_reference;
	RangeTblEntry *range_entry;

	if ((root->query_level != 1 && !insert_source) || parse->commandType != CMD_SELECT ||
		parse->setOperations != NULL || parse->cteList != NIL || parse->hasAggs ||
		parse->groupClause != NIL || parse->groupingSets != NIL ||
		parse->havingQual != NULL || parse->hasWindowFuncs ||
		parse->windowClause != NIL || parse->distinctClause != NIL ||
		parse->rowMarks != NIL || parse->hasTargetSRFs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("query shape is outside the current SemFilter capability"),
				 errdetail("Only a single-table SELECT or INSERT ... SELECT with ordinary filters, projections, ORDER BY, and LIMIT is supported.")));
	if (insert_source &&
		(root->parent_root->parse->resultRelation == 0 ||
		 root->parent_root->parse->onConflict != NULL ||
		 root->parent_root->parse->returningList != NIL ||
		 root->parent_root->parse->override != OVERRIDING_NOT_SET))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("INSERT shape is outside the current SemFilter capability"),
				 errdetail("ON CONFLICT, RETURNING, and OVERRIDING are not supported.")));
	if (parse->jointree == NULL || list_length(parse->jointree->fromlist) != 1 ||
		!IsA(linitial(parse->jointree->fromlist), RangeTblRef))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the SemFilter capability requires one non-inherited table")));
	range_reference = linitial_node(RangeTblRef, parse->jointree->fromlist);
	range_entry = rt_fetch(range_reference->rtindex, parse->rtable);
	if (range_entry->rtekind != RTE_RELATION || range_entry->inh ||
		range_entry->tablesample != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the SemFilter capability requires one non-inherited table")));
	if (semloom_marker_count((Node *) parse->targetList, marker_oid) != 0 ||
		semloom_marker_count(parse->limitOffset, marker_oid) != 0 ||
		semloom_marker_count(parse->limitCount, marker_oid) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ai_semantic.filter is only supported as a WHERE predicate")));
	if (OidIsValid(map_oid) &&
		semloom_marker_count((Node *) parse->targetList, map_oid) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("SemMap and SemFilter cannot be combined in the current capability")));
}

static CustomPath *
semloom_make_filter_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 Path *child_path,
						 FuncExpr *marker)
{
	CustomPath *path = makeNode(CustomPath);
	PathTarget *child_target;
	Path *projected_child = child_path;
	Node *input = linitial(marker->args);
	int input_column = 0;
	int column = 1;
	ListCell *cell;

	if (child_path->param_info != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parameterized SemFilter paths are not supported")));
	child_target = copy_pathtarget(child_path->pathtarget);
	foreach(cell, child_target->exprs)
	{
		if (equal(lfirst(cell), input))
		{
			input_column = column;
			break;
		}
		column++;
	}
	if (input_column == 0)
	{
		add_column_to_pathtarget(child_target,
								 (Expr *) copyObject(input),
								 0);
		set_pathtarget_cost_width(root, child_target);
		input_column = list_length(child_target->exprs);
		projected_child = (Path *) create_projection_path(root,
												   rel,
												   child_path,
												   child_target);
	}

	path->path.pathtype = T_CustomScan;
	path->path.parent = rel;
	path->path.pathtarget = rel->reltarget;
	path->path.param_info = NULL;
	path->path.parallel_aware = false;
	path->path.parallel_safe = false;
	path->path.parallel_workers = 0;
	path->path.rows = child_path->rows;
	path->path.disabled_nodes = child_path->disabled_nodes;
	path->path.startup_cost = child_path->startup_cost;
	path->path.total_cost = child_path->total_cost +
		cpu_operator_cost * child_path->rows;
	path->path.pathkeys = child_path->pathkeys;
	path->flags = CUSTOMPATH_SUPPORT_PROJECTION;
	path->custom_paths = list_make1(projected_child);
	path->custom_restrictinfo = NIL;
	path->custom_private = list_make1(makeInteger(input_column));
	path->methods = &semloom_filter_path_methods;
	return path;
}

static Plan *
semloom_plan_filter_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 CustomPath *best_path,
						 List *target_list,
						 List *clauses,
						 List *custom_plans)
{
	Oid marker_oid = semloom_filter_function_oid();
	CustomScan *scan = makeNode(CustomScan);
	Plan *child_plan;
	int input_column;

	(void) root;
	(void) rel;
	(void) clauses;
	if (!OidIsValid(marker_oid) || list_length(custom_plans) != 1 ||
		list_length(best_path->custom_private) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid SemFilter custom path state")));
	child_plan = linitial_node(Plan, custom_plans);
	input_column = intVal(linitial(best_path->custom_private));
	if (input_column <= 0 || input_column > list_length(child_plan->targetlist))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SemFilter plan lost its input identity")));

	semloom_replace_filter_marker_in_plan(child_plan, marker_oid);
	scan->scan.plan.targetlist = copyObject(target_list);
	scan->scan.plan.qual = NIL;
	scan->scan.scanrelid = 0;
	scan->flags = CUSTOMPATH_SUPPORT_PROJECTION;
	scan->custom_plans = custom_plans;
	scan->custom_exprs = NIL;
	scan->custom_private = list_make2(makeInteger(AI_PROVIDER_OPERATOR_FILTER),
									   makeInteger(input_column));
	scan->custom_scan_tlist = copyObject(child_plan->targetlist);
	scan->methods = &semloom_filter_scan_methods;
	return &scan->scan.plan;
}

static Node *
semloom_replace_filter_marker(Node *node, void *context)
{
	Oid marker_oid = *((Oid *) context);

	if (node == NULL)
		return NULL;
	if (IsA(node, FuncExpr) && ((FuncExpr *) node)->funcid == marker_oid)
		return (Node *) makeBoolConst(true, false);
	return expression_tree_mutator(node, semloom_replace_filter_marker, context);
}

static void
semloom_replace_filter_marker_in_plan(Plan *plan, Oid marker_oid)
{
	if (plan == NULL)
		return;
	plan->targetlist = (List *) semloom_replace_filter_marker(
		(Node *) plan->targetlist,
		&marker_oid);
	plan->qual = (List *) semloom_replace_filter_marker((Node *) plan->qual,
												  &marker_oid);
	semloom_replace_filter_marker_in_plan(plan->lefttree, marker_oid);
	semloom_replace_filter_marker_in_plan(plan->righttree, marker_oid);
}
