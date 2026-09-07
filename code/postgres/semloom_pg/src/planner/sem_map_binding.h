/* Plan the projected-input Map above an existing relational Filter and LIMIT. */
#ifndef SEMLOOM_MAP_BINDING_H
#define SEMLOOM_MAP_BINDING_H

#include "postgres.h"
#include "nodes/pathnodes.h"
#include "planner/semantic_call.h"

extern Plan *semloom_plan_bound_map(List *target_list, Plan *child,
	const SemloomSemanticCall *call, List *semantic_fields);
extern bool semloom_plan_has_filter(Plan *plan);

#endif
