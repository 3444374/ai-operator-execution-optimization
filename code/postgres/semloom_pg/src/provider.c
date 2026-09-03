/*
 * Select a query-fixed provider and implement shared neutral-error helpers.
 *
 * Input is PostgreSQL-owned configuration plus an AiOpenSpec; output is an
 * opaque adapter/config pair.  Passing requires this module to perform no I/O.
 * Plan: experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md.
 */
#include "postgres.h"

#include "utils/memutils.h"

#include "generation_profile.h"
#include "provider_private.h"
#include "semantic_filter_contract.h"
#include "semantic_map_contract.h"
#include "sem_text.h"
#include "sem_plan_spec.h"
#include "semloom_pg.h"

static bool semloom_slice_equals(const AiByteSlice *slice, const char *expected);
static bool semloom_slice_is_sha256(const AiByteSlice *slice);

void
semloom_provider_select(MemoryContext owner_context,
						const AiOpenSpec *spec,
						AiProvider *provider)
{
	const char *socket_path = semloom_gateway_socket_path();

	Assert(owner_context != NULL);
	Assert(spec != NULL);
	Assert(provider != NULL);
	if (!semloom_provider_spec_is_recording(spec) &&
		!semloom_provider_spec_is_exact_filter(spec) &&
		!semloom_provider_spec_is_generate_map(spec))
		elog(ERROR, "unsupported semantic provider plan specification");
	if (socket_path[0] == '\0' && semloom_provider_spec_is_recording(spec))
		semloom_recording_provider_select(provider);
	else
		semloom_uds_provider_select(owner_context,
								 socket_path,
								 spec,
								 semloom_provider_execution_profile(),
								 provider);
}

bool
semloom_provider_spec_is_recording(const AiOpenSpec *spec)
{
	bool map_spec;
	bool filter_spec;

	if (spec == NULL)
		return false;
	map_spec = spec->operator_kind == AI_PROVIDER_OPERATOR_MAP &&
		spec->output_value_kind == AI_PROVIDER_VALUE_TEXT &&
		semloom_slice_equals(&spec->semantic_spec_id,
						 SEMLOOM_MAP_RECORDING_SPEC_ID);
	filter_spec = spec->operator_kind == AI_PROVIDER_OPERATOR_FILTER &&
		spec->output_value_kind == AI_PROVIDER_VALUE_TRISTATE &&
		semloom_slice_equals(&spec->semantic_spec_id,
						 SEMLOOM_FILTER_RECORDING_SPEC_ID);

	return spec->plan_schema_version == SEMLOOM_PLAN_SPEC_SCHEMA_VERSION &&
		!spec->has_generation_profile &&
		(map_spec || filter_spec) &&
		spec->input_value_kind == AI_PROVIDER_VALUE_TEXT &&
		spec->null_policy == AI_PROVIDER_NULL_PROPAGATE &&
		spec->error_policy == AI_PROVIDER_ERROR_FAIL_QUERY &&
		spec->semantic_spec_version == SEMLOOM_RECORDING_SPEC_VERSION &&
		semloom_slice_equals(&spec->physical_algorithm, SEMLOOM_RECORDING_ALGORITHM);
}

bool
semloom_provider_spec_is_exact_filter(const AiOpenSpec *spec)
{
	uint8 bytes[SEMLOOM_GENERATION_PROFILE_CANONICAL_BYTES];
	uint32 length;
	bool version_matches;

	if (spec == NULL)
		return false;
	version_matches = spec->plan_schema_version == SEMLOOM_EXACT_FILTER_PLAN_SCHEMA_VERSION &&
		!spec->has_generation_profile;
	if (spec->plan_schema_version == SEMLOOM_CHOICE_FILTER_PLAN_SCHEMA_VERSION)
		version_matches = spec->has_generation_profile &&
			semloom_generation_profile_encode(&spec->generation_profile,
				bytes, sizeof(bytes), &length);
	return version_matches &&
		spec->operator_kind == AI_PROVIDER_OPERATOR_FILTER &&
		spec->input_value_kind == AI_PROVIDER_VALUE_TEXT &&
		spec->output_value_kind == AI_PROVIDER_VALUE_TRISTATE &&
		spec->null_policy == AI_PROVIDER_NULL_PROPAGATE &&
		spec->error_policy == AI_PROVIDER_ERROR_FAIL_QUERY &&
		spec->order_policy == AI_PROVIDER_ORDER_INPUT &&
		spec->semantic_spec_version == SEMLOOM_EXACT_FILTER_SPEC_VERSION &&
		semloom_slice_equals(&spec->semantic_spec_id, SEMLOOM_EXACT_FILTER_SPEC_ID) &&
		semloom_slice_equals(&spec->physical_algorithm,
						 SEMLOOM_MODEL_REFERENCE_ALGORITHM) &&
		semloom_slice_equals(&spec->physical_role, SEMLOOM_MODEL_REFERENCE_ROLE) &&
		semloom_slice_equals(&spec->prompt_program_digest,
						 SEMLOOM_PROMPT_PROGRAM_DIGEST) &&
		semloom_slice_equals(&spec->result_parser_digest,
						 SEMLOOM_RESULT_PARSER_DIGEST) &&
		spec->model_id.length > 0 &&
		spec->model_id.length <= SEMLOOM_FILTER_MODEL_MAX_BYTES &&
		spec->model_id.data != NULL &&
		semloom_slice_is_sha256(&spec->semantic_spec_digest) &&
		semloom_slice_is_sha256(&spec->physical_algorithm_digest) &&
		spec->temperature == SEMLOOM_FILTER_TEMPERATURE &&
		spec->top_p == SEMLOOM_FILTER_TOP_P &&
		spec->max_tokens == SEMLOOM_FILTER_MAX_TOKENS &&
		spec->n == SEMLOOM_FILTER_N &&
		spec->stream == (bool) SEMLOOM_FILTER_STREAM &&
		semloom_slice_equals(&spec->stop, SEMLOOM_FILTER_STOP);
}

bool
semloom_provider_spec_is_generate_map(const AiOpenSpec *spec)
{
	return spec != NULL && spec->plan_schema_version == SEMLOOM_MAP_PLAN_SCHEMA_VERSION &&
		spec->operator_kind == AI_PROVIDER_OPERATOR_MAP &&
		spec->input_value_kind == AI_PROVIDER_VALUE_TEXT &&
		spec->output_value_kind == AI_PROVIDER_VALUE_TEXT &&
		spec->null_policy == AI_PROVIDER_NULL_PROPAGATE &&
		spec->error_policy == AI_PROVIDER_ERROR_FAIL_QUERY &&
		spec->order_policy == AI_PROVIDER_ORDER_INPUT &&
		spec->semantic_spec_version == 1 &&
		semloom_slice_equals(&spec->semantic_spec_id, SEMLOOM_MAP_SPEC_ID) &&
		semloom_slice_equals(&spec->physical_algorithm, SEMLOOM_MODEL_REFERENCE_ALGORITHM) &&
		semloom_slice_equals(&spec->physical_role, SEMLOOM_MODEL_REFERENCE_ROLE) &&
		semloom_slice_equals(&spec->prompt_program_digest, SEMLOOM_MAP_PROMPT_PROGRAM_DIGEST) &&
		semloom_slice_equals(&spec->result_parser_digest, SEMLOOM_MAP_RESULT_PARSER_DIGEST) &&
		spec->model_id.length > 0 && spec->model_id.length <= SEMLOOM_MAP_MAX_MODEL_BYTES &&
		semloom_text_is_utf8_no_nul(spec->model_id.data, spec->model_id.length) &&
		semloom_slice_is_sha256(&spec->semantic_spec_digest) &&
		semloom_slice_is_sha256(&spec->physical_algorithm_digest) &&
		spec->temperature == 0 && spec->top_p == 1 && spec->n == 1 && !spec->stream &&
		spec->max_tokens >= 1 && spec->max_tokens <= SEMLOOM_MAP_MAX_GENERATION_TOKENS &&
		!spec->has_stop && spec->stop.length == 0 && !spec->has_generation_profile &&
		spec->max_input_bytes == SEMLOOM_MAP_MAX_INPUT_BYTES &&
		spec->max_output_bytes == SEMLOOM_MAP_MAX_OUTPUT_BYTES;
}

void
semloom_provider_error_clear(AiProviderError *error)
{
	if (error != NULL)
		MemSet(error, 0, sizeof(*error));
}

void
semloom_provider_error_set(AiProviderError *error,
						   uint32 code,
						   int system_errno,
						   uint32 limit_bytes,
						   const char *detail)
{
	Size detail_length = 0;

	Assert(error != NULL);
	semloom_provider_error_clear(error);
	error->code = code;
	error->system_errno = system_errno;
	error->limit_bytes = limit_bytes;
	if (detail != NULL)
	{
		detail_length = strlen(detail);
		if (detail_length >= AI_PROVIDER_ERROR_DETAIL_CAPACITY)
			detail_length = AI_PROVIDER_ERROR_DETAIL_CAPACITY - 1;
		memcpy(error->detail, detail, detail_length);
	}
	error->detail[detail_length] = '\0';
	error->detail_length = (uint16) detail_length;
}

static bool
semloom_slice_equals(const AiByteSlice *slice, const char *expected)
{
	Size expected_length = strlen(expected);

	return slice != NULL && slice->length == expected_length &&
		(slice->length == 0 ||
		 (slice->data != NULL &&
		  memcmp(slice->data, expected, slice->length) == 0));
}

static bool
semloom_slice_is_sha256(const AiByteSlice *slice)
{
	uint32 index;

	if (slice == NULL || slice->data == NULL ||
		slice->length != SEMLOOM_SHA256_HEX_LENGTH)
		return false;
	for (index = 0; index < slice->length; index++)
	{
		uint8 value = slice->data[index];

		if (!((value >= '0' && value <= '9') ||
			  (value >= 'a' && value <= 'f')))
			return false;
	}
	return true;
}
