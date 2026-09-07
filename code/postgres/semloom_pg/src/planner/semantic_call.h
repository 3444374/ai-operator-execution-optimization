/* Planner-local call occurrences; identity is independent of expression equality. */
#ifndef SEMLOOM_SEMANTIC_CALL_H
#define SEMLOOM_SEMANTIC_CALL_H

#include "postgres.h"
#include "nodes/primnodes.h"

typedef enum SemloomCallPlacement
{
	SEMLOOM_CALL_BASE_FILTER = 1,
	SEMLOOM_CALL_FINAL_MAP = 2,
} SemloomCallPlacement;

typedef struct SemloomSemanticCall
{
	int query_level;
	SemloomCallPlacement placement;
	int occurrence;
	int source_locator;
	FuncExpr *marker;
} SemloomSemanticCall;

/* Own one copy of this occurrence in the current planning memory context. */
extern SemloomSemanticCall *semloom_call_create(int query_level,
	SemloomCallPlacement placement, int occurrence, int source_locator, FuncExpr *marker);
extern Expr *semloom_call_input(const SemloomSemanticCall *call);
extern List *semloom_call_key(const SemloomSemanticCall *call);

#endif
