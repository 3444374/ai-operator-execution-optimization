#ifndef SEMLOOM_SEM_IMAGE_PLAN_H
#define SEMLOOM_SEM_IMAGE_PLAN_H

#include "planner/sem_plan_spec.h"
#include "nodes/primnodes.h"

extern List *semloom_image_plan_fields(FuncExpr *marker, bool staged);
extern bool semloom_image_plan_is_fields(List *fields);
extern void semloom_image_plan_decode(List *fields, MemoryContext owner, SemloomPlanSpec *plan);
extern void semloom_image_plan_explain(const SemloomPlanSpec *plan, ExplainState *state);

#endif
