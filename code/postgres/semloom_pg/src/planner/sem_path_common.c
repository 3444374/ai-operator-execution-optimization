/* PostgreSQL-private planner helpers shared by SemMap and SemFilter. */
#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"

#include "planner/sem_path_common.h"

typedef struct MarkerCountContext
{
	Oid marker_oid;
	int count;
} MarkerCountContext;

static bool semloom_count_marker(Node *node, void *context);

int
semloom_marker_count(Node *node, Oid marker_oid)
{
	MarkerCountContext context = {
		.marker_oid = marker_oid,
		.count = 0,
	};

	semloom_count_marker(node, &context);
	return context.count;
}

bool
semloom_is_insert_source(PlannerInfo *root)
{
	PlannerInfo *parent_root = root->parent_root;
	Query *parent_parse;
	RangeTblRef *source_reference;
	RangeTblEntry *source_entry;

	if (root->query_level != 2 || parent_root == NULL)
		return false;
	parent_parse = parent_root->parse;
	if (parent_parse->commandType != CMD_INSERT ||
		parent_parse->jointree == NULL ||
		list_length(parent_parse->jointree->fromlist) != 1 ||
		!IsA(linitial(parent_parse->jointree->fromlist), RangeTblRef))
		return false;

	source_reference = linitial_node(RangeTblRef, parent_parse->jointree->fromlist);
	source_entry = rt_fetch(source_reference->rtindex, parent_parse->rtable);
	return source_entry->rtekind == RTE_SUBQUERY;
}

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
