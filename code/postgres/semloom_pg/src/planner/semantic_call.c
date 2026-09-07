/* Occurrence ownership shared by Map and Filter; operator validation stays local. */
#include "postgres.h"
#include "nodes/makefuncs.h"
#include "planner/semantic_call.h"

SemloomSemanticCall *
semloom_call_create(int query_level, SemloomCallPlacement placement,
					int occurrence, int source_locator, FuncExpr *marker)
{
	SemloomSemanticCall *call;

	if (query_level <= 0 || occurrence < 0 || source_locator <= 0 ||
		(placement != SEMLOOM_CALL_BASE_FILTER && placement != SEMLOOM_CALL_FINAL_MAP) ||
		marker == NULL || !IsA(marker, FuncExpr) || marker->args == NIL)
		elog(ERROR, "invalid semantic call occurrence");
	call = palloc(sizeof(*call));
	call->query_level = query_level;
	call->placement = placement;
	call->occurrence = occurrence;
	call->source_locator = source_locator;
	call->marker = copyObject(marker);
	return call;
}

Expr *
semloom_call_input(const SemloomSemanticCall *call)
{
	return linitial(call->marker->args);
}

List *
semloom_call_key(const SemloomSemanticCall *call)
{
	return list_make3(makeInteger(call->query_level), makeInteger(call->placement),
					  makeInteger(call->occurrence));
}
