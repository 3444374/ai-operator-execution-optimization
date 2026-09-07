/* Decode the internal plan envelope independently of semantic and wire versions. */
#ifndef SEMLOOM_SEMANTIC_CARRIER_H
#define SEMLOOM_SEMANTIC_CARRIER_H

#include "planner/sem_plan_spec.h"
#include "planner/sem_filter_cost.h"
#include "planner/semantic_call.h"

typedef struct SemloomPlanCarrier
{
	SemloomPlanSpec spec;
	SemloomFilterCostEstimate cost;
	bool has_cost;
	bool projected_input;
	AttrNumber input_column;
	List *binding_fields;
} SemloomPlanCarrier;

extern List *semloom_carrier_make_map(const SemloomSemanticCall *call,
	List *semantic_fields, int result_column, List *mapping);
extern void semloom_carrier_decode(List *fields, MemoryContext owner,
	SemloomPlanCarrier *carrier);

#endif
