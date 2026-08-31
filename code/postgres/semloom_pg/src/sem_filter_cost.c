/* Strict copyObject-safe encoding for exact SemFilter planner estimates. */
#include "postgres.h"

#include <math.h>

#include "commands/explain_format.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"
#include "utils/float.h"

#include "semantic_filter_contract.h"
#include "sem_filter_cost.h"

#define SEMLOOM_FILTER_COST_SCHEMA_VERSION 1
#define SEMLOOM_FILTER_COST_FIELD_COUNT 9

#define SEMLOOM_COST_FIELD_SCHEMA_VERSION "schema_version"
#define SEMLOOM_COST_FIELD_MODEL_ID "cost_model_id"
#define SEMLOOM_COST_FIELD_MODEL_ROLE "model_role"
#define SEMLOOM_COST_FIELD_INPUT_ROWS "semantic_input_rows"
#define SEMLOOM_COST_FIELD_OUTPUT_SELECTIVITY "output_selectivity"
#define SEMLOOM_COST_FIELD_MODEL_CALLS "estimated_model_calls"
#define SEMLOOM_COST_FIELD_PROMPT_TOKENS "estimated_prompt_tokens"
#define SEMLOOM_COST_FIELD_OUTPUT_TOKENS "estimated_output_tokens"
#define SEMLOOM_COST_FIELD_AI_WORK_COST "ai_work_cost"

typedef enum SemloomFilterCostFieldBit
{
	SEMLOOM_COST_SEEN_SCHEMA_VERSION = 1U << 0,
	SEMLOOM_COST_SEEN_MODEL_ID = 1U << 1,
	SEMLOOM_COST_SEEN_MODEL_ROLE = 1U << 2,
	SEMLOOM_COST_SEEN_INPUT_ROWS = 1U << 3,
	SEMLOOM_COST_SEEN_OUTPUT_SELECTIVITY = 1U << 4,
	SEMLOOM_COST_SEEN_MODEL_CALLS = 1U << 5,
	SEMLOOM_COST_SEEN_PROMPT_TOKENS = 1U << 6,
	SEMLOOM_COST_SEEN_OUTPUT_TOKENS = 1U << 7,
	SEMLOOM_COST_SEEN_AI_WORK_COST = 1U << 8,
} SemloomFilterCostFieldBit;

#define SEMLOOM_FILTER_COST_FIELDS ((1U << 9) - 1U)

static List *semloom_filter_cost_integer_field(const char *name, int value);
static List *semloom_filter_cost_string_field(const char *name, const char *value);
static double semloom_filter_cost_read_double(Node *value);
static void semloom_filter_cost_mark_seen(uint32 *seen_fields, uint32 field_bit);
pg_noreturn static void semloom_filter_cost_invalid(void);

List *
semloom_filter_cost_make_private(const SemloomFilterCostEstimate *estimate)
{
	List *fields = NIL;

	if (estimate == NULL || estimate->cost_model_id == NULL ||
		estimate->model_role == NULL ||
		strcmp(estimate->cost_model_id, SEMLOOM_FILTER_COST_MODEL_ID) != 0 ||
		strcmp(estimate->model_role, SEMLOOM_EXACT_FILTER_ROLE) != 0 ||
		!isfinite(estimate->semantic_input_rows) ||
		estimate->semantic_input_rows < 0 ||
		!isfinite(estimate->output_selectivity) ||
		estimate->output_selectivity < 0 ||
		estimate->output_selectivity > 1 ||
		!isfinite(estimate->estimated_model_calls) ||
		estimate->estimated_model_calls < 0 ||
		!isfinite(estimate->estimated_prompt_tokens) ||
		estimate->estimated_prompt_tokens < 0 ||
		!isfinite(estimate->estimated_output_tokens) ||
		estimate->estimated_output_tokens < 0 ||
		!isfinite(estimate->ai_work_cost) || estimate->ai_work_cost < 0)
		semloom_filter_cost_invalid();

	fields = lappend(fields, semloom_filter_cost_integer_field(
		SEMLOOM_COST_FIELD_SCHEMA_VERSION, SEMLOOM_FILTER_COST_SCHEMA_VERSION));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_MODEL_ID, estimate->cost_model_id));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_MODEL_ROLE, estimate->model_role));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_INPUT_ROWS,
		float8out_internal(estimate->semantic_input_rows)));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_OUTPUT_SELECTIVITY,
		float8out_internal(estimate->output_selectivity)));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_MODEL_CALLS,
		float8out_internal(estimate->estimated_model_calls)));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_PROMPT_TOKENS,
		float8out_internal(estimate->estimated_prompt_tokens)));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_OUTPUT_TOKENS,
		float8out_internal(estimate->estimated_output_tokens)));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_AI_WORK_COST,
		float8out_internal(estimate->ai_work_cost)));
	return fields;
}

bool
semloom_filter_cost_decode(List *custom_private,
							   SemloomFilterCostEstimate *estimate)
{
	List *fields;
	ListCell *cell;
	uint32 seen_fields = 0;
	int schema_version = 0;

	Assert(estimate != NULL);
	MemSet(estimate, 0, sizeof(*estimate));
	if (list_length(custom_private) == 2)
		return false;
	if (list_length(custom_private) != 3 ||
		!IsA(lthird(custom_private), List))
		semloom_filter_cost_invalid();
	fields = (List *) lthird(custom_private);
	foreach(cell, fields)
	{
		List *field = (List *) lfirst(cell);
		Node *name_node;
		Node *value_node;
		const char *name;

		if (!IsA(field, List) || list_length(field) != 2)
			semloom_filter_cost_invalid();
		name_node = (Node *) linitial(field);
		value_node = (Node *) lsecond(field);
		if (!IsA(name_node, String))
			semloom_filter_cost_invalid();
		name = strVal(name_node);
		if (strcmp(name, SEMLOOM_COST_FIELD_SCHEMA_VERSION) == 0)
		{
			semloom_filter_cost_mark_seen(
				&seen_fields, SEMLOOM_COST_SEEN_SCHEMA_VERSION);
			if (!IsA(value_node, Integer))
				semloom_filter_cost_invalid();
			schema_version = intVal(value_node);
		}
		else if (strcmp(name, SEMLOOM_COST_FIELD_MODEL_ID) == 0)
		{
			semloom_filter_cost_mark_seen(&seen_fields, SEMLOOM_COST_SEEN_MODEL_ID);
			if (!IsA(value_node, String))
				semloom_filter_cost_invalid();
			estimate->cost_model_id = strVal(value_node);
		}
		else if (strcmp(name, SEMLOOM_COST_FIELD_MODEL_ROLE) == 0)
		{
			semloom_filter_cost_mark_seen(&seen_fields, SEMLOOM_COST_SEEN_MODEL_ROLE);
			if (!IsA(value_node, String))
				semloom_filter_cost_invalid();
			estimate->model_role = strVal(value_node);
		}
#define READ_DOUBLE(field_name, bit, target) \
		else if (strcmp(name, (field_name)) == 0) \
		{ \
			semloom_filter_cost_mark_seen(&seen_fields, (bit)); \
			(target) = semloom_filter_cost_read_double(value_node); \
		}
		READ_DOUBLE(SEMLOOM_COST_FIELD_INPUT_ROWS,
					SEMLOOM_COST_SEEN_INPUT_ROWS, estimate->semantic_input_rows)
		READ_DOUBLE(SEMLOOM_COST_FIELD_OUTPUT_SELECTIVITY,
					SEMLOOM_COST_SEEN_OUTPUT_SELECTIVITY,
					estimate->output_selectivity)
		READ_DOUBLE(SEMLOOM_COST_FIELD_MODEL_CALLS,
					SEMLOOM_COST_SEEN_MODEL_CALLS, estimate->estimated_model_calls)
		READ_DOUBLE(SEMLOOM_COST_FIELD_PROMPT_TOKENS,
					SEMLOOM_COST_SEEN_PROMPT_TOKENS,
					estimate->estimated_prompt_tokens)
		READ_DOUBLE(SEMLOOM_COST_FIELD_OUTPUT_TOKENS,
					SEMLOOM_COST_SEEN_OUTPUT_TOKENS,
					estimate->estimated_output_tokens)
		READ_DOUBLE(SEMLOOM_COST_FIELD_AI_WORK_COST,
					SEMLOOM_COST_SEEN_AI_WORK_COST, estimate->ai_work_cost)
#undef READ_DOUBLE
		else
			semloom_filter_cost_invalid();
	}
	if (schema_version != SEMLOOM_FILTER_COST_SCHEMA_VERSION ||
		seen_fields != SEMLOOM_FILTER_COST_FIELDS ||
		list_length(fields) != SEMLOOM_FILTER_COST_FIELD_COUNT ||
		strcmp(estimate->cost_model_id, SEMLOOM_FILTER_COST_MODEL_ID) != 0 ||
		strcmp(estimate->model_role, SEMLOOM_EXACT_FILTER_ROLE) != 0 ||
		estimate->output_selectivity > 1)
		semloom_filter_cost_invalid();
	return true;
}

void
semloom_filter_cost_explain(const SemloomFilterCostEstimate *estimate,
							ExplainState *explain_state)
{
	ExplainPropertyText("AI Cost Model", estimate->cost_model_id, explain_state);
	ExplainPropertyText("AI Cost Calibration",
						SEMLOOM_FILTER_COST_CALIBRATION_STATUS,
						explain_state);
	ExplainPropertyText("Model Role", estimate->model_role, explain_state);
	ExplainPropertyFloat("Semantic Input Rows", NULL,
						 estimate->semantic_input_rows, 2, explain_state);
	ExplainPropertyFloat("Output Selectivity", NULL,
						 estimate->output_selectivity, 6, explain_state);
	ExplainPropertyFloat("Estimated Model Calls", NULL,
						 estimate->estimated_model_calls, 2, explain_state);
	ExplainPropertyFloat("Estimated Prompt Tokens", NULL,
						 estimate->estimated_prompt_tokens, 2, explain_state);
	ExplainPropertyFloat("Estimated Output Tokens", NULL,
						 estimate->estimated_output_tokens, 2, explain_state);
	ExplainPropertyFloat("AI Work Cost", NULL,
						 estimate->ai_work_cost, 4, explain_state);
}

static List *
semloom_filter_cost_integer_field(const char *name, int value)
{
	return list_make2(makeString(pstrdup(name)), makeInteger(value));
}

static List *
semloom_filter_cost_string_field(const char *name, const char *value)
{
	return list_make2(makeString(pstrdup(name)), makeString(pstrdup(value)));
}

static double
semloom_filter_cost_read_double(Node *value)
{
	char *end = NULL;
	const char *text;
	double parsed;

	if (!IsA(value, String))
		semloom_filter_cost_invalid();
	text = strVal(value);
	parsed = float8in_internal((char *) text, &end, "double precision", text, NULL);
	if (end == text || *end != '\0' || !isfinite(parsed) || parsed < 0)
		semloom_filter_cost_invalid();
	return parsed;
}

static void
semloom_filter_cost_mark_seen(uint32 *seen_fields, uint32 field_bit)
{
	if ((*seen_fields & field_bit) != 0)
		semloom_filter_cost_invalid();
	*seen_fields |= field_bit;
}

static void
semloom_filter_cost_invalid(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("invalid SemFilter cost specification")));
	pg_unreachable();
}
