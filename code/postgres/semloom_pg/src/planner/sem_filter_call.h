/* Query-local Filter calls, with validated constant arguments. */
#ifndef SEMLOOM_FILTER_CALL_H
#define SEMLOOM_FILTER_CALL_H

#include "postgres.h"
#include "nodes/pathnodes.h"
#include "planner/semantic_call.h"

#define SEMLOOM_MAX_FILTER_CALLS 2

typedef struct SemloomFilterCall
{
	RestrictInfo *restriction;
	SemloomSemanticCall *semantic;
	bool is_exact;
	char *instruction;
	char *model_id;
	bool choice_profile;
} SemloomFilterCall;

extern List *semloom_filter_calls(PlannerInfo *root, RelOptInfo *rel, Oid recording_oid, Oid exact_oid);
extern int semloom_filter_marker_count(Node *node, Oid recording_oid, Oid exact_oid);
extern bool semloom_is_filter_marker(Oid function_oid, Oid recording_oid, Oid exact_oid);

#endif
