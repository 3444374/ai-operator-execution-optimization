/* Preserve ordinary projections; evaluate only the Map input in its own node. */
#include "postgres.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "executor/sem_scan.h"
#include "planner/sem_map_binding.h"
#include "planner/semantic_carrier.h"

bool
semloom_plan_has_filter(Plan *plan)
{
	if (plan == NULL)
		return false;
	if (IsA(plan, CustomScan) && ((CustomScan *) plan)->methods == &semloom_filter_scan_methods)
		return true;
	return semloom_plan_has_filter(plan->lefttree) || semloom_plan_has_filter(plan->righttree);
}

static Node *
remove_map_evaluation(Node *node, void *context)
{
	Oid function = *((Oid *) context);

	if (node == NULL)
		return NULL;
	if (IsA(node, FuncExpr) && ((FuncExpr *) node)->funcid == function)
		return (Node *) makeNullConst(exprType(node), exprTypmod(node), exprCollation(node));
	return expression_tree_mutator(node, remove_map_evaluation, context);
}

static void
retain_raw_inputs(Plan *plan, List *variables, Oid function, int *filters)
{
	ListCell *cell;

	if (plan == NULL)
		return;
	plan->targetlist = (List *) remove_map_evaluation((Node *) plan->targetlist, &function);
	foreach(cell, variables)
	{
		Expr *variable = lfirst(cell);
		ListCell *entry_cell;
		bool found = false;

		foreach(entry_cell, plan->targetlist)
			if (equal(lfirst_node(TargetEntry, entry_cell)->expr, variable))
				found = true;
		if (!found)
			plan->targetlist = lappend(plan->targetlist, makeTargetEntry(
				copyObject(variable), list_length(plan->targetlist) + 1, NULL, false));
	}
	if (IsA(plan, CustomScan))
	{
		CustomScan *scan = (CustomScan *) plan;

		if (scan->methods != &semloom_filter_scan_methods || list_length(scan->custom_plans) != 1)
			elog(ERROR, "unsupported child in Filter-to-Map binding");
		(*filters)++;
		retain_raw_inputs(linitial(scan->custom_plans), variables, function, filters);
		scan->custom_scan_tlist = copyObject(linitial_node(Plan, scan->custom_plans)->targetlist);
	}
	retain_raw_inputs(plan->lefttree, variables, function, filters);
	retain_raw_inputs(plan->righttree, variables, function, filters);
}

static Var *
scan_column(TargetEntry *entry)
{
	Var *reference = makeVar(INDEX_VAR, entry->resno, exprType((Node *) entry->expr),
		exprTypmod((Node *) entry->expr), exprCollation((Node *) entry->expr), 0);

	reference->varnosyn = 0;
	reference->varattnosyn = 0;
	return reference;
}

Plan *
semloom_plan_bound_map(List *target_list, Plan *child, const SemloomSemanticCall *call,
					   List *semantic_fields)
{
	CustomScan *scan = makeNode(CustomScan);
	List *variables = pull_var_clause((Node *) semloom_call_input(call), PVC_RECURSE_PLACEHOLDERS);
	List *mapping = NIL;
	ListCell *cell;
	int filters = 0;
	int result_column;
	int map_outputs = 0;

	retain_raw_inputs(child, variables, call->marker->funcid, &filters);
	if (filters != 1)
		elog(ERROR, "Filter-to-Map requires exactly one relational Filter child");
	result_column = list_length(child->targetlist) + 1;
	scan->custom_scan_tlist = copyObject(child->targetlist);
	foreach(cell, scan->custom_scan_tlist)
	{
		TargetEntry *entry = lfirst_node(TargetEntry, cell);

		mapping = lappend(mapping, list_make2(makeInteger(entry->resno), makeInteger(entry->resno)));
		/* An already computed output must not match a separate volatile input. */
		if (!IsA(entry->expr, Var))
			entry->expr = (Expr *) scan_column(entry);
	}
	scan->custom_scan_tlist = lappend(scan->custom_scan_tlist,
		makeTargetEntry((Expr *) copyObject(call->marker), result_column, NULL, false));
	scan->scan.plan.targetlist = copyObject(target_list);
	foreach(cell, scan->scan.plan.targetlist)
	{
		TargetEntry *entry = lfirst_node(TargetEntry, cell);

		if (IsA(entry->expr, FuncExpr) && ((FuncExpr *) entry->expr)->funcid == call->marker->funcid)
			map_outputs++;
		else if (!IsA(entry->expr, Var))
			entry->expr = (Expr *) scan_column(entry);
	}
	if (map_outputs != 1)
		elog(ERROR, "Filter-to-Map lost its output occurrence");
	scan->scan.scanrelid = 0;
	scan->flags = CUSTOMPATH_SUPPORT_PROJECTION;
	scan->custom_plans = list_make1(child);
	scan->custom_exprs = list_make1(copyObject(semloom_call_input(call)));
	scan->custom_private = semloom_carrier_make_map(call, semantic_fields, result_column, mapping);
	scan->methods = &semloom_map_scan_methods;
	return &scan->scan.plan;
}
