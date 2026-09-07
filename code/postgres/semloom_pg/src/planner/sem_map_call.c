/* Validate Map occurrences and fixed source arguments without constructing paths. */
#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"

#include "planner/marker_identity.h"
#include "planner/sem_map_call.h"
#include "planner/sem_path_common.h"

typedef struct SemloomMapPlacement
{
	Oid generate_oid;
	Oid recording_oid;
	Oid filter_oid;
	Oid exact_filter_oid;
	List *visible_markers;
	int generate_count;
	bool misplaced;
	bool has_recording_map;
	bool has_filter;
} SemloomMapPlacement;

static bool semloom_map_nonconstant_source(Node *node, void *context);
static bool semloom_map_constant_walker(Node *node, void *context);
static bool semloom_map_placement_walker(Node *node, void *context);
static void semloom_validate_map_placement(Query *parse, Oid marker_oid);

int
semloom_validate_generate_map_source(Query *parse)
{
	Oid marker_oid = semloom_generate_map_function_oid();
	int query_level = 1;
	ListCell *cell;

	if (!OidIsValid(marker_oid))
		return 0;
	semloom_map_constant_walker((Node *) parse, &marker_oid);
	if (parse->commandType == CMD_INSERT)
	{
		RangeTblRef *reference;
		RangeTblEntry *entry;

		if (parse->jointree == NULL || list_length(parse->jointree->fromlist) != 1 ||
			!IsA(linitial(parse->jointree->fromlist), RangeTblRef))
			return 0;
		reference = linitial_node(RangeTblRef, parse->jointree->fromlist);
		entry = rt_fetch(reference->rtindex, parse->rtable);
		if (entry->rtekind != RTE_SUBQUERY)
			return 0;
		parse = entry->subquery;
		query_level = 2;
	}
	if (parse->commandType != CMD_SELECT)
		return 0;
	foreach(cell, parse->targetList)
	{
		TargetEntry *entry = lfirst_node(TargetEntry, cell);

		if (!entry->resjunk && IsA(entry->expr, FuncExpr) &&
			((FuncExpr *) entry->expr)->funcid == marker_oid)
			return query_level;
	}
	return 0;
}

static bool
semloom_map_placement_walker(Node *node, void *context)
{
	SemloomMapPlacement *placement = context;

	/* Each query level is checked separately by the outer Query walker. */
	if (node == NULL || IsA(node, Query))
		return false;
	if (IsA(node, FuncExpr))
	{
		Oid function_oid = ((FuncExpr *) node)->funcid;

		if (function_oid == placement->generate_oid)
		{
			placement->generate_count++;
			if (!list_member_ptr(placement->visible_markers, node))
				placement->misplaced = true;
		}
		else if (function_oid == placement->recording_oid)
			placement->has_recording_map = true;
		else if (function_oid == placement->filter_oid || function_oid == placement->exact_filter_oid)
			placement->has_filter = true;
	}
	return expression_tree_walker(node, semloom_map_placement_walker, context);
}

static void
semloom_validate_map_placement(Query *parse, Oid marker_oid)
{
	SemloomMapPlacement placement = {
		.generate_oid = marker_oid,
		.recording_oid = semloom_map_function_oid(),
		.filter_oid = semloom_filter_function_oid(),
		.exact_filter_oid = semloom_exact_filter_function_oid(),
	};
	ListCell *cell;

	foreach(cell, parse->targetList)
	{
		TargetEntry *entry = lfirst_node(TargetEntry, cell);

		if (!entry->resjunk && IsA(entry->expr, FuncExpr) &&
			((FuncExpr *) entry->expr)->funcid == marker_oid)
			placement.visible_markers = lappend(placement.visible_markers, entry->expr);
	}
	query_tree_walker(parse, semloom_map_placement_walker, &placement, 0);
	list_free(placement.visible_markers);
	if (placement.generate_count == 0)
		return;
	if (placement.has_filter)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("SemMap and SemFilter cannot be combined in the current capability")));
	if (placement.has_recording_map || placement.generate_count != 1)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("the SemMap capability supports exactly one visible marker")));
	if (placement.misplaced)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("ai_semantic.map is only supported as a top-level output expression")));
}

static bool
semloom_map_nonconstant_source(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Param) || IsA(node, Var) || IsA(node, SubLink))
		return true;
	return expression_tree_walker(node, semloom_map_nonconstant_source, context);
}

static bool
semloom_map_constant_walker(Node *node, void *context)
{
	Oid marker_oid = *((Oid *) context);

	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		semloom_validate_map_placement((Query *) node, marker_oid);
		return query_tree_walker((Query *) node, semloom_map_constant_walker, context, 0);
	}
	if (IsA(node, FuncExpr) && ((FuncExpr *) node)->funcid == marker_oid)
	{
		FuncExpr *marker = (FuncExpr *) node;
		List *fixed_arguments;

		if (list_length(marker->args) != 3)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("invalid generative SemMap marker arguments")));
		fixed_arguments = list_make2(lsecond(marker->args), lthird(marker->args));
		if (semloom_map_nonconstant_source((Node *) fixed_arguments, NULL) ||
			contain_mutable_functions((Node *) fixed_arguments))
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("SemMap instruction and options must be fixed immutable constants")));
		list_free(fixed_arguments);
	}
	return expression_tree_walker(node, semloom_map_constant_walker, context);
}

Oid
semloom_map_marker_oid(Query *parse)
{
	Oid recording_oid = semloom_map_function_oid();
	Oid generate_oid = semloom_generate_map_function_oid();
	int recording_count = OidIsValid(recording_oid) ?
		semloom_marker_count((Node *) parse->targetList, recording_oid) : 0;
	int generate_count = OidIsValid(generate_oid) ?
		semloom_marker_count((Node *) parse->targetList, generate_oid) : 0;

	if (recording_count > 0 && generate_count > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the SemMap capability supports exactly one visible marker")));
	return generate_count > 0 ? generate_oid : recording_oid;
}

FuncExpr *
semloom_map_call(Query *parse, Oid marker_oid)
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
