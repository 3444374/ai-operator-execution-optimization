/* Fixed Map identity bytes and value checks, independent of PostgreSQL and I/O. */
#include <string.h>

#include "semantics/semantic_map_contract.h"
#include "semantics/sem_text.h"

typedef struct SemloomMapIdentityWriter
{
	uint8_t *output;
	size_t length;
} SemloomMapIdentityWriter;

static bool
map_plan_valid(const SemloomMapPlanValues *plan)
{
	return plan != NULL &&
		!plan->instruction.is_null && plan->instruction.length > 0 &&
		plan->instruction.length <= SEMLOOM_MAP_MAX_INSTRUCTION_BYTES &&
		!plan->model_id.is_null && plan->model_id.length > 0 &&
		plan->model_id.length <= SEMLOOM_MAP_MAX_MODEL_BYTES &&
		plan->max_tokens >= 1 && plan->max_tokens <= SEMLOOM_MAP_MAX_GENERATION_TOKENS &&
		semloom_text_is_utf8_no_nul(plan->instruction.data, plan->instruction.length) &&
		semloom_text_is_utf8_no_nul(plan->model_id.data, plan->model_id.length);
}

static void
identity_bytes(SemloomMapIdentityWriter *writer, const void *data, size_t length)
{
	if (writer->output != NULL && length > 0)
		memcpy(writer->output + writer->length, data, length);
	writer->length += length;
}

static void
identity_uint32(SemloomMapIdentityWriter *writer, uint32_t value)
{
	uint8_t bytes[4] = {(uint8_t) (value >> 24), (uint8_t) (value >> 16),
		(uint8_t) (value >> 8), (uint8_t) value};

	identity_bytes(writer, bytes, sizeof(bytes));
}

static void
identity_text(SemloomMapIdentityWriter *writer, const uint8_t *data, uint32_t length)
{
	identity_uint32(writer, length);
	identity_bytes(writer, data, length);
}

static void
identity_literal(SemloomMapIdentityWriter *writer, const char *value)
{
	identity_text(writer, (const uint8_t *) value, (uint32_t) strlen(value));
}

static void
encode_plan(SemloomMapIdentityWriter *writer, const SemloomMapPlanValues *plan)
{
	static const char domain[] = "semloom-semantic-spec-v4";
	static const uint8_t flags[] = {0, 0};

	identity_bytes(writer, domain, sizeof(domain));
	identity_uint32(writer, 4);
	identity_literal(writer, SEMLOOM_MAP_SPEC_ID);
	identity_uint32(writer, 1);
	identity_literal(writer, "SEM_MAP");
	identity_literal(writer, "text");
	identity_literal(writer, "text");
	identity_text(writer, plan->instruction.data, plan->instruction.length);
	identity_literal(writer, SEMLOOM_MAP_PROMPT_PROGRAM_ID);
	identity_uint32(writer, 1);
	identity_literal(writer, SEMLOOM_MAP_PROMPT_PROGRAM_DIGEST);
	identity_literal(writer, SEMLOOM_MAP_RESULT_PARSER_ID);
	identity_uint32(writer, 1);
	identity_literal(writer, SEMLOOM_MAP_RESULT_PARSER_DIGEST);
	identity_literal(writer, "PROPAGATE_NULL");
	identity_literal(writer, "FAIL_QUERY");
	identity_literal(writer, "INPUT_ORDER");
	identity_text(writer, plan->model_id.data, plan->model_id.length);
	identity_uint32(writer, 0);
	identity_uint32(writer, 1);
	identity_uint32(writer, plan->max_tokens);
	identity_uint32(writer, 1);
	identity_bytes(writer, flags, sizeof(flags));
	identity_uint32(writer, SEMLOOM_MAP_MAX_INPUT_BYTES);
	identity_uint32(writer, SEMLOOM_MAP_MAX_OUTPUT_BYTES);
}

bool
semloom_map_plan_encode(const SemloomMapPlanValues *plan,
						uint8_t *output, size_t capacity, size_t *written)
{
	SemloomMapIdentityWriter writer = {NULL, 0};
	size_t required;

	if (written == NULL)
		return false;
	*written = 0;
	if (!map_plan_valid(plan))
		return false;
	encode_plan(&writer, plan);
	required = writer.length;
	if (output == NULL)
	{
		if (capacity != 0)
			return false;
		*written = required;
		return true;
	}
	if (capacity < required)
		return false;
	writer.output = output;
	writer.length = 0;
	encode_plan(&writer, plan);
	*written = writer.length;
	return true;
}

uint32_t
semloom_map_completion_status(const SemloomMapPlanValues *plan,
							  const SemloomMachineCompletion *completion)
{
	if (!map_plan_valid(plan) || completion == NULL || completion->is_null ||
		completion->response_model_id.is_null || completion->finish_reason.is_null ||
		completion->response_model_id.length == 0 ||
		completion->response_model_id.length > SEMLOOM_MAP_MAX_MODEL_BYTES ||
		completion->finish_reason.length == 0 ||
		completion->finish_reason.length > SEMLOOM_MAP_MAX_FINISH_REASON_BYTES ||
		!semloom_text_is_utf8_no_nul(completion->data, completion->length) ||
		!semloom_text_is_utf8_no_nul(completion->response_model_id.data, completion->response_model_id.length) ||
		!semloom_text_is_utf8_no_nul(completion->finish_reason.data, completion->finish_reason.length) ||
		completion->output_tokens > plan->max_tokens ||
		completion->response_model_id.length != plan->model_id.length ||
		memcmp(completion->response_model_id.data, plan->model_id.data, plan->model_id.length) != 0)
		return SEMLOOM_MAP_COMPLETION_INVALID;
	if (completion->length > SEMLOOM_MAP_MAX_OUTPUT_BYTES)
		return SEMLOOM_MAP_COMPLETION_TOO_LARGE;
	if (completion->finish_reason.length != 4 || memcmp(completion->finish_reason.data, "stop", 4) != 0)
		return SEMLOOM_MAP_COMPLETION_INCOMPLETE;
	return SEMLOOM_MAP_COMPLETION_VALID;
}
