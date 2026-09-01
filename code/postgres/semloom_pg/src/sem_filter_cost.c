/* Strict copyObject-safe encoding for exact SemFilter planner estimates. */
#include "postgres.h"

#include <math.h>

#include "commands/explain_format.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"
#include "utils/float.h"

#include "semantic_filter_contract.h"
#include "sem_filter_cost.h"

#define SEMLOOM_FILTER_COST_SCHEMA_VERSION 2
#define SEMLOOM_FILTER_COST_FIELD_COUNT 17
#define SEMLOOM_FILTER_CALIBRATED_MODEL_ID \
	"semloom.exact_filter.reference-calibrated.v1"

#define SEMLOOM_COST_FIELD_SCHEMA_VERSION "schema_version"
#define SEMLOOM_COST_FIELD_MODEL_ID "cost_model_id"
#define SEMLOOM_COST_FIELD_CALIBRATION_STATUS "calibration_status"
#define SEMLOOM_COST_FIELD_CALIBRATION_REASON "calibration_reason"
#define SEMLOOM_COST_FIELD_CALIBRATION_ID "calibration_id"
#define SEMLOOM_COST_FIELD_WORKLOAD_SIGNATURE "workload_signature"
#define SEMLOOM_COST_FIELD_SERVICE_SIGNATURE "service_signature"
#define SEMLOOM_COST_FIELD_MODEL_ROLE "model_role"
#define SEMLOOM_COST_FIELD_INPUT_ROWS "semantic_input_rows"
#define SEMLOOM_COST_FIELD_OUTPUT_SELECTIVITY "output_selectivity"
#define SEMLOOM_COST_FIELD_MODEL_CALLS "estimated_model_calls"
#define SEMLOOM_COST_FIELD_PROMPT_TOKENS "estimated_prompt_tokens"
#define SEMLOOM_COST_FIELD_OUTPUT_TOKENS "estimated_output_tokens"
#define SEMLOOM_COST_FIELD_SERVICE_MS "estimated_service_milliseconds"
#define SEMLOOM_COST_FIELD_HELD_OUT_MAX_ERROR "held_out_max_relative_error"
#define SEMLOOM_COST_FIELD_ACCEPTED_MAX_ERROR "accepted_max_relative_error"
#define SEMLOOM_COST_FIELD_AI_WORK_COST "ai_work_cost"

typedef enum SemloomFilterCostFieldBit
{
	SEMLOOM_COST_SEEN_SCHEMA_VERSION = 1U << 0,
	SEMLOOM_COST_SEEN_MODEL_ID = 1U << 1,
	SEMLOOM_COST_SEEN_CALIBRATION_STATUS = 1U << 2,
	SEMLOOM_COST_SEEN_CALIBRATION_REASON = 1U << 3,
	SEMLOOM_COST_SEEN_CALIBRATION_ID = 1U << 4,
	SEMLOOM_COST_SEEN_WORKLOAD_SIGNATURE = 1U << 5,
	SEMLOOM_COST_SEEN_SERVICE_SIGNATURE = 1U << 6,
	SEMLOOM_COST_SEEN_MODEL_ROLE = 1U << 7,
	SEMLOOM_COST_SEEN_INPUT_ROWS = 1U << 8,
	SEMLOOM_COST_SEEN_OUTPUT_SELECTIVITY = 1U << 9,
	SEMLOOM_COST_SEEN_MODEL_CALLS = 1U << 10,
	SEMLOOM_COST_SEEN_PROMPT_TOKENS = 1U << 11,
	SEMLOOM_COST_SEEN_OUTPUT_TOKENS = 1U << 12,
	SEMLOOM_COST_SEEN_SERVICE_MS = 1U << 13,
	SEMLOOM_COST_SEEN_HELD_OUT_MAX_ERROR = 1U << 14,
	SEMLOOM_COST_SEEN_ACCEPTED_MAX_ERROR = 1U << 15,
	SEMLOOM_COST_SEEN_AI_WORK_COST = 1U << 16,
} SemloomFilterCostFieldBit;

#define SEMLOOM_FILTER_COST_FIELDS ((1U << 17) - 1U)

static List *semloom_filter_cost_integer_field(const char *name, int value);
static List *semloom_filter_cost_string_field(const char *name, const char *value);
static double semloom_filter_cost_read_double(Node *value);
static void semloom_filter_cost_mark_seen(uint32 *seen_fields, uint32 field_bit);
static bool semloom_filter_cost_valid_calibration(
	const SemloomFilterCostEstimate *estimate);
static bool semloom_filter_cost_sha256(const char *value);
static bool semloom_filter_cost_rejection_reason(const char *value);
pg_noreturn static void semloom_filter_cost_invalid(void);
pg_noreturn static void semloom_filter_cost_estimate_invalid(void);

List *
semloom_filter_cost_make_private(const SemloomFilterCostEstimate *estimate)
{
	List *fields = NIL;

	if (estimate == NULL || estimate->cost_model_id == NULL ||
		estimate->calibration_status == NULL ||
		estimate->calibration_reason == NULL ||
		estimate->calibration_id == NULL ||
		estimate->workload_signature == NULL ||
		estimate->service_signature == NULL ||
		estimate->model_role == NULL ||
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
		!isfinite(estimate->estimated_service_milliseconds) ||
		estimate->estimated_service_milliseconds < 0 ||
		!isfinite(estimate->held_out_max_relative_error) ||
		estimate->held_out_max_relative_error < 0 ||
		!isfinite(estimate->accepted_max_relative_error) ||
		estimate->accepted_max_relative_error < 0 ||
		!isfinite(estimate->ai_work_cost) || estimate->ai_work_cost < 0 ||
		!semloom_filter_cost_valid_calibration(estimate))
		semloom_filter_cost_estimate_invalid();

	fields = lappend(fields, semloom_filter_cost_integer_field(
		SEMLOOM_COST_FIELD_SCHEMA_VERSION, SEMLOOM_FILTER_COST_SCHEMA_VERSION));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_MODEL_ID, estimate->cost_model_id));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_CALIBRATION_STATUS, estimate->calibration_status));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_CALIBRATION_REASON, estimate->calibration_reason));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_CALIBRATION_ID, estimate->calibration_id));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_WORKLOAD_SIGNATURE, estimate->workload_signature));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_SERVICE_SIGNATURE, estimate->service_signature));
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
		SEMLOOM_COST_FIELD_SERVICE_MS,
		float8out_internal(estimate->estimated_service_milliseconds)));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_HELD_OUT_MAX_ERROR,
		float8out_internal(estimate->held_out_max_relative_error)));
	fields = lappend(fields, semloom_filter_cost_string_field(
		SEMLOOM_COST_FIELD_ACCEPTED_MAX_ERROR,
		float8out_internal(estimate->accepted_max_relative_error)));
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
		else if (strcmp(name, SEMLOOM_COST_FIELD_CALIBRATION_STATUS) == 0)
		{
			semloom_filter_cost_mark_seen(
				&seen_fields, SEMLOOM_COST_SEEN_CALIBRATION_STATUS);
			if (!IsA(value_node, String))
				semloom_filter_cost_invalid();
			estimate->calibration_status = strVal(value_node);
		}
		else if (strcmp(name, SEMLOOM_COST_FIELD_CALIBRATION_REASON) == 0)
		{
			semloom_filter_cost_mark_seen(
				&seen_fields, SEMLOOM_COST_SEEN_CALIBRATION_REASON);
			if (!IsA(value_node, String))
				semloom_filter_cost_invalid();
			estimate->calibration_reason = strVal(value_node);
		}
		else if (strcmp(name, SEMLOOM_COST_FIELD_CALIBRATION_ID) == 0)
		{
			semloom_filter_cost_mark_seen(
				&seen_fields, SEMLOOM_COST_SEEN_CALIBRATION_ID);
			if (!IsA(value_node, String))
				semloom_filter_cost_invalid();
			estimate->calibration_id = strVal(value_node);
		}
		else if (strcmp(name, SEMLOOM_COST_FIELD_WORKLOAD_SIGNATURE) == 0)
		{
			semloom_filter_cost_mark_seen(
				&seen_fields, SEMLOOM_COST_SEEN_WORKLOAD_SIGNATURE);
			if (!IsA(value_node, String))
				semloom_filter_cost_invalid();
			estimate->workload_signature = strVal(value_node);
		}
		else if (strcmp(name, SEMLOOM_COST_FIELD_SERVICE_SIGNATURE) == 0)
		{
			semloom_filter_cost_mark_seen(
				&seen_fields, SEMLOOM_COST_SEEN_SERVICE_SIGNATURE);
			if (!IsA(value_node, String))
				semloom_filter_cost_invalid();
			estimate->service_signature = strVal(value_node);
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
		READ_DOUBLE(SEMLOOM_COST_FIELD_SERVICE_MS,
					SEMLOOM_COST_SEEN_SERVICE_MS,
					estimate->estimated_service_milliseconds)
		READ_DOUBLE(SEMLOOM_COST_FIELD_HELD_OUT_MAX_ERROR,
					SEMLOOM_COST_SEEN_HELD_OUT_MAX_ERROR,
					estimate->held_out_max_relative_error)
		READ_DOUBLE(SEMLOOM_COST_FIELD_ACCEPTED_MAX_ERROR,
					SEMLOOM_COST_SEEN_ACCEPTED_MAX_ERROR,
					estimate->accepted_max_relative_error)
		READ_DOUBLE(SEMLOOM_COST_FIELD_AI_WORK_COST,
					SEMLOOM_COST_SEEN_AI_WORK_COST, estimate->ai_work_cost)
#undef READ_DOUBLE
		else
			semloom_filter_cost_invalid();
	}
	if (schema_version != SEMLOOM_FILTER_COST_SCHEMA_VERSION ||
		seen_fields != SEMLOOM_FILTER_COST_FIELDS ||
		list_length(fields) != SEMLOOM_FILTER_COST_FIELD_COUNT ||
		strcmp(estimate->model_role, SEMLOOM_EXACT_FILTER_ROLE) != 0 ||
		estimate->output_selectivity > 1 ||
		estimate->accepted_max_relative_error > 1 ||
		estimate->held_out_max_relative_error >
			estimate->accepted_max_relative_error ||
		!semloom_filter_cost_valid_calibration(estimate))
		semloom_filter_cost_invalid();
	return true;
}

void
semloom_filter_cost_explain(const SemloomFilterCostEstimate *estimate,
							ExplainState *explain_state)
{
	ExplainPropertyText("AI Cost Model", estimate->cost_model_id, explain_state);
	ExplainPropertyText("AI Cost Calibration",
						estimate->calibration_status,
						explain_state);
	ExplainPropertyText("AI Cost Calibration Reason",
						estimate->calibration_reason,
						explain_state);
	if (estimate->calibration_id[0] != '\0')
	{
		ExplainPropertyText("AI Cost Calibration ID",
							estimate->calibration_id,
							explain_state);
		ExplainPropertyText("AI Cost Workload Signature",
							estimate->workload_signature,
							explain_state);
		ExplainPropertyText("AI Cost Service Signature",
							estimate->service_signature,
							explain_state);
		ExplainPropertyFloat("Estimated Service Milliseconds", NULL,
							 estimate->estimated_service_milliseconds, 3,
							 explain_state);
		ExplainPropertyFloat("Held-out Max Relative Error", NULL,
							 estimate->held_out_max_relative_error, 6,
							 explain_state);
		ExplainPropertyFloat("Accepted Max Relative Error", NULL,
							 estimate->accepted_max_relative_error, 6,
							 explain_state);
	}
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

static bool
semloom_filter_cost_valid_calibration(const SemloomFilterCostEstimate *estimate)
{
	if (strcmp(estimate->calibration_status, "matched") == 0)
		return strcmp(estimate->cost_model_id,
					  SEMLOOM_FILTER_CALIBRATED_MODEL_ID) == 0 &&
			strcmp(estimate->calibration_reason, "matched") == 0 &&
			semloom_filter_cost_sha256(estimate->calibration_id) &&
			semloom_filter_cost_sha256(estimate->workload_signature) &&
			semloom_filter_cost_sha256(estimate->service_signature) &&
			estimate->estimated_service_milliseconds > 0 &&
			estimate->ai_work_cost == estimate->estimated_service_milliseconds;
	if (strcmp(estimate->cost_model_id, SEMLOOM_FILTER_COST_MODEL_ID) != 0 ||
		estimate->calibration_id[0] != '\0' ||
		estimate->workload_signature[0] != '\0' ||
		estimate->service_signature[0] != '\0' ||
		estimate->estimated_service_milliseconds != 0 ||
		estimate->held_out_max_relative_error != 0 ||
		estimate->accepted_max_relative_error != 0)
		return false;
	if (strcmp(estimate->calibration_status, "unavailable") == 0)
		return strcmp(estimate->calibration_reason, "not-configured") == 0;
	return strcmp(estimate->calibration_status, "rejected") == 0 &&
		semloom_filter_cost_rejection_reason(estimate->calibration_reason);
}

static bool
semloom_filter_cost_sha256(const char *value)
{
	int index;

	if (strlen(value) != 64)
		return false;
	for (index = 0; index < 64; index++)
		if (!((value[index] >= '0' && value[index] <= '9') ||
			  (value[index] >= 'a' && value[index] <= 'f')))
			return false;
	return true;
}

static bool
semloom_filter_cost_rejection_reason(const char *value)
{
	return strcmp(value, "unreadable-artifact") == 0 ||
		strcmp(value, "invalid-artifact") == 0 ||
		strcmp(value, "semantic-spec-mismatch") == 0 ||
		strcmp(value, "physical-algorithm-mismatch") == 0 ||
		strcmp(value, "model-mismatch") == 0 ||
		strcmp(value, "model-role-mismatch") == 0 ||
		strcmp(value, "provider-profile-mismatch") == 0;
}

static void
semloom_filter_cost_invalid(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("invalid SemFilter cost specification")));
	pg_unreachable();
}

static void
semloom_filter_cost_estimate_invalid(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("invalid SemFilter cost estimate")));
	pg_unreachable();
}
