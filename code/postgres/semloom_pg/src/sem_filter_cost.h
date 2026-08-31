/* Planner-owned cardinality and AI-work estimate for exact SemFilter. */
#ifndef SEMLOOM_SEM_FILTER_COST_H
#define SEMLOOM_SEM_FILTER_COST_H

#include "postgres.h"

#include "commands/explain_state.h"
#include "nodes/pg_list.h"

#define SEMLOOM_FILTER_COST_MODEL_ID "semloom.exact_filter.uncalibrated.v1"
#define SEMLOOM_FILTER_COST_CALIBRATION_STATUS "unavailable"

typedef struct SemloomFilterCostEstimate
{
	const char *cost_model_id;
	const char *model_role;
	double semantic_input_rows;
	double output_selectivity;
	double estimated_model_calls;
	double estimated_prompt_tokens;
	double estimated_output_tokens;
	double ai_work_cost;
} SemloomFilterCostEstimate;

extern List *semloom_filter_cost_make_private(
	const SemloomFilterCostEstimate *estimate);
extern bool semloom_filter_cost_decode(
	List *custom_private,
	SemloomFilterCostEstimate *estimate);
extern void semloom_filter_cost_explain(
	const SemloomFilterCostEstimate *estimate,
	ExplainState *explain_state);

#endif
