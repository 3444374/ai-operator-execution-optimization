/* Fixed exact-Filter schemas for wire v3/v4 above shared bounded framing. */
#include "postgres.h"

#include "common/cryptohash.h"
#include "common/sha2.h"
#include "lib/stringinfo.h"
#include "utils/json.h"

#include "provider_private.h"
#include "wire_common.h"
#include "wire_semantic.h"
#include "generation_profile.h"

#define SEMLOOM_SEMANTIC_ERROR_FIELD_COUNT 4

static void semloom_semantic_hash_begin(pg_cryptohash_ctx **context);
static void semloom_semantic_hash_bytes(pg_cryptohash_ctx *context,
								  const void *data,
								  Size length);
static void semloom_semantic_hash_uint32(pg_cryptohash_ctx *context, uint32 value);
static void semloom_semantic_hash_uint64(pg_cryptohash_ctx *context, uint64 value);
static void semloom_semantic_hash_slice(pg_cryptohash_ctx *context, AiByteSlice value);
static void semloom_semantic_hash_finish(
	pg_cryptohash_ctx *context,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
static void semloom_semantic_provider_execution_digest(
	const AiOpenSpec *spec,
	const char *provider_execution_id,
	uint32 protocol_version,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
static void semloom_semantic_completion_digest(
	const SemloomWireSemanticIdentity *identity,
	AiByteSlice payload_digest,
	uint64 sequence,
	AiByteSlice raw_output,
	AiByteSlice finish_reason,
	AiByteSlice response_model,
	uint64 prompt_tokens,
	uint64 output_tokens,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
static bool semloom_semantic_json_slice(Jsonb *message,
								 const char *key,
								 AiByteSlice *result,
								 AiProviderError *error);
static bool semloom_semantic_json_uint64(Jsonb *message,
								  const char *key,
								  uint64 *result,
								  AiProviderError *error);
static bool semloom_semantic_slice_equals(AiByteSlice actual, AiByteSlice expected);
static bool semloom_semantic_slice_equals_cstring(AiByteSlice actual,
											 const char *expected);
static bool semloom_semantic_validate_response(Jsonb *message,
								 uint32 wire_version,
										 const char *expected_type,
										 uint32 expected_fields,
										 const char *expected_error_sequence,
										 AiProviderError *error);
static bool semloom_semantic_validate_error(Jsonb *message,
							  uint32 wire_version,
									  const char *expected_sequence,
									  AiByteSlice *code,
									  AiProviderError *error);
static bool semloom_semantic_error_code_allowed(AiByteSlice code);
static void semloom_semantic_set_error_code(AiByteSlice code,
									 AiProviderError *error);

static void
semloom_semantic_append_profile(StringInfo request,
	const AiGenerationProfile *profile, const char *digest)
{
	uint32 index;

	appendStringInfoString(request, ",\"generation_profile\":{\"profile_id\":");
	escape_json_with_len(request, (const char *) profile->profile_id.data,
		profile->profile_id.length);
	appendStringInfo(request, ",\"profile_version\":%u,\"constraint_kind\":\"CHOICE\",\"choices\":[",
		profile->profile_version);
	for (index = 0; index < profile->choice_count; index++)
	{
		if (index != 0)
			appendStringInfoChar(request, ',');
		escape_json_with_len(request, (const char *) profile->choices[index].data,
			profile->choices[index].length);
	}
	appendStringInfo(request, "],\"profile_digest\":\"%s\"}", digest);
}

void
semloom_wire_semantic_identity_init(const AiOpenSpec *spec,
								  const char *provider_execution_id,
								  uint32 protocol_version,
								  SemloomWireSemanticIdentity *identity)
{
	Assert(spec != NULL);
	Assert(provider_execution_id != NULL);
	Assert(identity != NULL);
	Assert(spec->semantic_spec_digest.length == SEMLOOM_SHA256_HEX_LENGTH);
	Assert(spec->physical_algorithm_digest.length == SEMLOOM_SHA256_HEX_LENGTH);
	Assert(protocol_version == 3 || protocol_version == 4);
	identity->protocol_version = protocol_version;
	identity->provider_execution_id = provider_execution_id;
	memcpy(identity->semantic_spec_digest,
		   spec->semantic_spec_digest.data,
		   SEMLOOM_SHA256_HEX_LENGTH);
	identity->semantic_spec_digest[SEMLOOM_SHA256_HEX_LENGTH] = '\0';
	memcpy(identity->physical_algorithm_digest,
		   spec->physical_algorithm_digest.data,
		   SEMLOOM_SHA256_HEX_LENGTH);
	identity->physical_algorithm_digest[SEMLOOM_SHA256_HEX_LENGTH] = '\0';
	semloom_semantic_provider_execution_digest(spec,
									 provider_execution_id,
									 protocol_version,
									 identity->provider_execution_digest);
	if (protocol_version == 4)
	{
		uint8 bytes[SEMLOOM_GENERATION_PROFILE_CANONICAL_BYTES];
		uint32 length;
		pg_cryptohash_ctx *context;

		if (!semloom_generation_profile_encode(&spec->generation_profile,
			bytes, sizeof(bytes), &length))
			elog(ERROR, "invalid semantic generation profile");
		semloom_semantic_hash_begin(&context);
		semloom_semantic_hash_bytes(context, bytes, length);
		semloom_semantic_hash_finish(context, identity->generation_profile_digest);
	}
}

AiProviderStatus
semloom_wire_semantic_open(pgsocket socket_fd,
					 const AiOpenSpec *spec,
					 const SemloomWireSemanticIdentity *identity,
					 AiProviderError *error)
{
	StringInfoData request;
	char *response;
	Jsonb *message;
	int32 integer_value;
	bool matches;
	AiProviderStatus status;

	initStringInfo(&request);
	appendStringInfo(&request,
		"{\"type\":\"open\",\"protocol_version\":%d,"
		"\"semantic_spec_digest\":\"%s\","
		"\"physical_algorithm_digest\":\"%s\","
		"\"provider_execution_digest\":\"%s\","
		"\"provider_execution_id\":",
		identity->protocol_version,
		identity->semantic_spec_digest,
		identity->physical_algorithm_digest,
		identity->provider_execution_digest);
	escape_json(&request, identity->provider_execution_id);
	appendStringInfoString(&request, ",\"operator_kind\":\"SEM_FILTER\",\"semantic_spec_id\":");
	escape_json_with_len(&request,
						 (const char *) spec->semantic_spec_id.data,
						 spec->semantic_spec_id.length);
	appendStringInfo(&request,
		",\"semantic_spec_version\":%u,\"physical_algorithm\":",
		spec->semantic_spec_version);
	escape_json_with_len(&request,
						 (const char *) spec->physical_algorithm.data,
						 spec->physical_algorithm.length);
	appendStringInfoString(&request, ",\"physical_role\":");
	escape_json_with_len(&request,
						 (const char *) spec->physical_role.data,
						 spec->physical_role.length);
	appendStringInfoString(&request, ",\"prompt_program_digest\":");
	escape_json_with_len(&request,
						 (const char *) spec->prompt_program_digest.data,
						 spec->prompt_program_digest.length);
	appendStringInfoString(&request, ",\"result_parser_digest\":");
	escape_json_with_len(&request,
						 (const char *) spec->result_parser_digest.data,
						 spec->result_parser_digest.length);
	appendStringInfoString(&request, ",\"model_id\":");
	escape_json_with_len(&request,
						 (const char *) spec->model_id.data,
						 spec->model_id.length);
	appendStringInfo(&request,
		",\"generation_constraints\":{\"temperature\":%u,\"top_p\":%u,"
		"\"max_tokens\":%u,\"n\":%u,\"stream\":%s,\"stop\":[",
		spec->temperature,
		spec->top_p,
		spec->max_tokens,
		spec->n,
		spec->stream ? "true" : "false");
	escape_json_with_len(&request, (const char *) spec->stop.data, spec->stop.length);
	appendStringInfoString(&request,
		"]},\"null_policy\":\"PROPAGATE_NULL\","
		"\"error_policy\":\"FAIL_QUERY\",\"order_policy\":\"INPUT_ORDER\","
		"\"input_type\":\"text\",\"raw_output_type\":\"tristate_ascii\"");
	if (identity->protocol_version == 4)
		semloom_semantic_append_profile(&request, &spec->generation_profile,
			identity->generation_profile_digest);
	appendStringInfoChar(&request, '}');
	status = semloom_wire_common_send_frame(socket_fd,
										request.data,
										request.len,
										error);
	pfree(request.data);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	status = semloom_wire_common_receive_frame(socket_fd, &response, error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	status = (identity->protocol_version == 4 ?
		semloom_wire_common_parse_json_unique(response, &message, error) :
		semloom_wire_common_parse_json(response, &message, error));
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	if (!semloom_semantic_validate_response(message, identity->protocol_version,
		"opened", identity->protocol_version == 4 ? 9 : 8, NULL, error))
		return AI_PROVIDER_STATUS_ERROR;
	if (!semloom_wire_common_json_int32(message,
										"protocol_version",
										&integer_value,
										error) ||
		integer_value != identity->protocol_version)
		goto mismatch;
#define REQUIRE_IDENTITY(field, expected) \
	if (!semloom_wire_common_json_string_equals(message, (field), (expected), \
			&matches, error) || !matches) goto mismatch_or_error
	REQUIRE_IDENTITY("semantic_spec_digest", identity->semantic_spec_digest);
	REQUIRE_IDENTITY("physical_algorithm_digest", identity->physical_algorithm_digest);
	REQUIRE_IDENTITY("provider_execution_digest", identity->provider_execution_digest);
	if (identity->protocol_version == 4)
	{
		REQUIRE_IDENTITY("generation_profile_digest", identity->generation_profile_digest);
	}
#undef REQUIRE_IDENTITY
	if (!semloom_wire_common_json_int32(message,
										"max_inflight_tasks",
										&integer_value,
										error) || integer_value != 1)
		goto mismatch;
	if (!semloom_wire_common_json_int32(message,
										"max_frame_bytes",
										&integer_value,
										error) ||
		integer_value != SEMLOOM_WIRE_SEMANTIC_MAX_FRAME_BYTES)
		goto mismatch;
	if (!semloom_wire_common_json_int32(message,
										"max_input_bytes",
										&integer_value,
										error) ||
		integer_value != SEMLOOM_WIRE_SEMANTIC_MAX_INPUT_BYTES)
		goto mismatch;
	return AI_PROVIDER_STATUS_OK;

mismatch_or_error:
	if (error->code != AI_PROVIDER_ERROR_NONE)
		return AI_PROVIDER_STATUS_ERROR;
mismatch:
	semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   0,
								   0,
								   identity->protocol_version == 4 ?
		"SemLoom provider open response does not match wire v4" :
		"SemLoom provider open response does not match wire v3");
	return AI_PROVIDER_STATUS_ERROR;
}

AiProviderStatus
semloom_wire_semantic_drive(pgsocket socket_fd,
					  const AiOpenSpec *spec,
					  const AiPreparedTask *task,
					  const SemloomWireSemanticIdentity *identity,
					  AiCompletion *completion,
					  AiProviderError *error)
{
	char sequence[32];
	char expected_evidence_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
	StringInfoData request;
	char *response;
	Jsonb *message;
	AiByteSlice response_sequence;
	AiByteSlice payload_digest;
	AiByteSlice raw_output;
	AiByteSlice response_model;
	AiByteSlice finish_reason;
	AiByteSlice evidence_digest;
	uint64 prompt_tokens;
	uint64 output_tokens;
	bool matches;
	AiProviderStatus status;

	if (task->semantic_payload_digest.length != SEMLOOM_SHA256_HEX_LENGTH ||
		task->semantic_payload_digest.data == NULL ||
		task->canonical_messages.length == 0 ||
		task->canonical_messages.data == NULL)
	{
		semloom_provider_error_set(error, AI_PROVIDER_ERROR_TASK_MISMATCH, 0, 0, NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	pg_snprintf(sequence, sizeof(sequence), UINT64_FORMAT, task->sequence);
	initStringInfo(&request);
	appendStringInfo(&request,
		"{\"type\":\"task\",\"protocol_version\":%d,\"sequence\":\"%s\","
		"\"semantic_spec_digest\":\"%s\","
		"\"physical_algorithm_digest\":\"%s\","
		"\"provider_execution_digest\":\"%s\","
		"\"semantic_payload_digest\":\"%.*s\",\"canonical_messages\":",
		identity->protocol_version,
		sequence,
		identity->semantic_spec_digest,
		identity->physical_algorithm_digest,
		identity->provider_execution_digest,
		(int) task->semantic_payload_digest.length,
		(const char *) task->semantic_payload_digest.data);
	appendBinaryStringInfo(&request,
						   (const char *) task->canonical_messages.data,
						   task->canonical_messages.length);
	if (identity->protocol_version == 4)
		appendStringInfo(&request, ",\"generation_profile_digest\":\"%s\"",
			identity->generation_profile_digest);
	appendStringInfoChar(&request, '}');
	status = semloom_wire_common_send_frame(socket_fd,
										request.data,
										request.len,
										error);
	pfree(request.data);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	status = semloom_wire_common_receive_frame(socket_fd, &response, error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	status = (identity->protocol_version == 4 ?
		semloom_wire_common_parse_json_unique(response, &message, error) :
		semloom_wire_common_parse_json(response, &message, error));
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	if (!semloom_semantic_validate_response(message, identity->protocol_version,
		"completion", identity->protocol_version == 4 ? 14 : 13, sequence, error))
		return AI_PROVIDER_STATUS_ERROR;
	{
		int32 protocol_version;

		if (!semloom_wire_common_json_int32(message,
											"protocol_version",
											&protocol_version,
											error) ||
			protocol_version != identity->protocol_version)
			goto mismatch;
	}
	if (!semloom_semantic_json_slice(message, "sequence", &response_sequence, error) ||
		!semloom_semantic_slice_equals_cstring(response_sequence, sequence))
		goto mismatch_or_error;
#define REQUIRE_IDENTITY(field, expected) \
	if (!semloom_wire_common_json_string_equals(message, (field), (expected), \
			&matches, error) || !matches) goto mismatch_or_error
	REQUIRE_IDENTITY("semantic_spec_digest", identity->semantic_spec_digest);
	REQUIRE_IDENTITY("physical_algorithm_digest", identity->physical_algorithm_digest);
	REQUIRE_IDENTITY("provider_execution_digest", identity->provider_execution_digest);
	if (identity->protocol_version == 4)
	{
		REQUIRE_IDENTITY("generation_profile_digest", identity->generation_profile_digest);
	}
#undef REQUIRE_IDENTITY
	if (!semloom_semantic_json_slice(message, "semantic_payload_digest", &payload_digest, error) ||
		!semloom_semantic_slice_equals(payload_digest, task->semantic_payload_digest) ||
		!semloom_semantic_json_slice(message, "raw_output", &raw_output, error) ||
		!semloom_semantic_json_slice(message, "response_model_id", &response_model, error) ||
		!semloom_semantic_slice_equals(response_model, spec->model_id) ||
		!semloom_semantic_json_uint64(message, "prompt_tokens", &prompt_tokens, error) ||
		!semloom_semantic_json_uint64(message, "output_tokens", &output_tokens, error) ||
		!semloom_semantic_json_slice(message, "finish_reason", &finish_reason, error) ||
		!semloom_semantic_slice_equals_cstring(finish_reason, "stop") ||
		!semloom_semantic_json_slice(message,
								 "completion_evidence_digest",
								 &evidence_digest,
								 error))
		goto mismatch_or_error;
	semloom_semantic_completion_digest(identity,
								 payload_digest,
								 task->sequence,
								 raw_output,
								 finish_reason,
								 response_model,
								 prompt_tokens,
								 output_tokens,
								 expected_evidence_digest);
	if (!semloom_semantic_slice_equals_cstring(evidence_digest,
										 expected_evidence_digest))
		goto mismatch;

	completion->sequence = task->sequence;
	completion->is_null = false;
	completion->output = raw_output;
	completion->response_model_id = response_model;
	completion->finish_reason = finish_reason;
	completion->prompt_tokens = prompt_tokens;
	completion->output_tokens = output_tokens;
	return AI_PROVIDER_STATUS_OK;

mismatch_or_error:
	if (error->code != AI_PROVIDER_ERROR_NONE)
		return AI_PROVIDER_STATUS_ERROR;
mismatch:
	semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   0,
								   0,
								   identity->protocol_version == 4 ?
		"SemLoom provider completion does not match wire v4 task identity" :
		"SemLoom provider completion does not match wire v3 task identity");
	return AI_PROVIDER_STATUS_ERROR;
}

static bool
semloom_semantic_json_slice(Jsonb *message,
						  const char *key,
						  AiByteSlice *result,
						  AiProviderError *error)
{
	JsonbValue *value;

	if (!semloom_wire_common_json_value(message, key, &value, error))
		return false;
	if (value->type != jbvString || value->val.string.len > PG_UINT32_MAX)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   0,
								   0,
								   "SemLoom provider response has an invalid text field");
		return false;
	}
	result->data = (const uint8 *) value->val.string.val;
	result->length = (uint32) value->val.string.len;
	return true;
}

static bool
semloom_semantic_json_uint64(Jsonb *message,
						   const char *key,
						   uint64 *result,
						   AiProviderError *error)
{
	AiByteSlice value;
	uint64 parsed = 0;
	uint32 index;

	if (!semloom_semantic_json_slice(message, key, &value, error))
		return false;
	if (value.length == 0 || value.length > 20 ||
		(value.length > 1 && value.data[0] == '0'))
		goto invalid;
	for (index = 0; index < value.length; index++)
	{
		uint8 digit;

		if (value.data[index] < '0' || value.data[index] > '9')
			goto invalid;
		digit = value.data[index] - '0';
		if (parsed > (PG_UINT64_MAX - digit) / 10)
			goto invalid;
		parsed = parsed * 10 + digit;
	}
	*result = parsed;
	return true;

invalid:
	semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   0,
								   0,
								   "SemLoom provider response has an invalid uint64 field");
	return false;
}

static bool
semloom_semantic_slice_equals(AiByteSlice actual, AiByteSlice expected)
{
	return actual.length == expected.length &&
		(actual.length == 0 ||
		 (actual.data != NULL && expected.data != NULL &&
		  memcmp(actual.data, expected.data, actual.length) == 0));
}

static bool
semloom_semantic_slice_equals_cstring(AiByteSlice actual, const char *expected)
{
	AiByteSlice expected_slice = {
		.data = (const uint8 *) expected,
		.length = (uint32) strlen(expected),
	};

	return semloom_semantic_slice_equals(actual, expected_slice);
}

static bool
semloom_semantic_validate_response(Jsonb *message,
								 uint32 wire_version,
								 const char *expected_type,
								 uint32 expected_fields,
								 const char *expected_error_sequence,
								 AiProviderError *error)
{
	bool matches;

	if (!semloom_wire_common_json_string_equals(message,
											 "type",
											 "error",
											 &matches,
											 error))
		return false;
	if (matches)
	{
		AiByteSlice code;

		if (!semloom_semantic_validate_error(message,
									   wire_version,
									   expected_error_sequence,
									   &code,
									   error))
			return false;
		semloom_semantic_set_error_code(code, error);
		return false;
	}
	if (JsonContainerSize(&message->root) != expected_fields)
		goto unexpected;
	if (!semloom_wire_common_json_string_equals(message,
											 "type",
											 expected_type,
											 &matches,
											 error))
		return false;
	if (matches)
		return true;

unexpected:
	semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   0,
								   0,
								   "SemLoom provider returned an unexpected message");
	return false;
}

static bool
semloom_semantic_validate_error(Jsonb *message,
							  uint32 wire_version,
							  const char *expected_sequence,
							  AiByteSlice *code,
							  AiProviderError *error)
{
	JsonbValue *sequence_value;
	int32 protocol_version;

	if (JsonContainerSize(&message->root) != SEMLOOM_SEMANTIC_ERROR_FIELD_COUNT ||
		!semloom_wire_common_json_int32(message,
										  "protocol_version",
										  &protocol_version,
										  error) ||
		protocol_version != wire_version ||
		!semloom_wire_common_json_value(message, "sequence", &sequence_value, error) ||
		!semloom_semantic_json_slice(message, "code", code, error) ||
		!semloom_semantic_error_code_allowed(*code))
		goto invalid;
	if (expected_sequence == NULL)
	{
		if (sequence_value->type != jbvNull)
			goto invalid;
	}
	else if (sequence_value->type != jbvString ||
			 sequence_value->val.string.len != strlen(expected_sequence) ||
			 memcmp(sequence_value->val.string.val,
					expected_sequence,
					sequence_value->val.string.len) != 0)
		goto invalid;
	return true;

invalid:
	semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   0,
								   0,
								   wire_version == 4 ?
		"SemLoom provider returned an invalid wire v4 error frame" :
		"SemLoom provider returned an invalid wire v3 error frame");
	return false;
}

static void
semloom_semantic_set_error_code(AiByteSlice code, AiProviderError *error)
{
	uint32 neutral_code = AI_PROVIDER_ERROR_PROTOCOL;
	const char *detail = "SemLoom provider rejected the protocol message";

	if (semloom_semantic_slice_equals_cstring(code, "MODEL_UNAVAILABLE"))
	{
		neutral_code = AI_PROVIDER_ERROR_REMOTE_UNAVAILABLE;
		detail = NULL;
	}
	else if (semloom_semantic_slice_equals_cstring(code, "MODEL_TIMEOUT"))
	{
		neutral_code = AI_PROVIDER_ERROR_REMOTE_TIMEOUT;
		detail = NULL;
	}
	else if (semloom_semantic_slice_equals_cstring(code, "MODEL_REQUEST_REJECTED"))
	{
		neutral_code = AI_PROVIDER_ERROR_REQUEST_REJECTED;
		detail = NULL;
	}
	else if (semloom_semantic_slice_equals_cstring(code, "MODEL_RESPONSE_INVALID"))
	{
		neutral_code = AI_PROVIDER_ERROR_INVALID_RESPONSE;
		detail = NULL;
	}
	else if (semloom_semantic_slice_equals_cstring(code, "GATEWAY_INTERNAL"))
	{
		neutral_code = AI_PROVIDER_ERROR_ADAPTER_INTERNAL;
		detail = NULL;
	}
	semloom_provider_error_set(error, neutral_code, 0, 0, detail);
}

static bool
semloom_semantic_error_code_allowed(AiByteSlice code)
{
	static const char *allowed_codes[] = {
		"GATEWAY_INTERNAL",
		"GOLDEN_FIXTURE_INVALID",
		"GOLDEN_FIXTURE_MISSING",
		"INVALID_OPEN",
		"INVALID_TASK",
		"MODEL_REQUEST_REJECTED",
		"MODEL_RESPONSE_INVALID",
		"MODEL_TIMEOUT",
		"MODEL_UNAVAILABLE",
	};
	int index;

	for (index = 0; index < lengthof(allowed_codes); index++)
	{
		if (semloom_semantic_slice_equals_cstring(code, allowed_codes[index]))
			return true;
	}
	return false;
}

static void
semloom_semantic_provider_execution_digest(
	const AiOpenSpec *spec,
	const char *provider_execution_id,
	uint32 protocol_version,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1])
{
	pg_cryptohash_ctx *context;
	AiByteSlice execution_id = {
		.data = (const uint8 *) provider_execution_id,
		.length = (uint32) strlen(provider_execution_id),
	};

	semloom_semantic_hash_begin(&context);
	semloom_semantic_hash_bytes(context,
						  protocol_version == 4 ? "semloom-provider-execution-v4" : "semloom-provider-execution-v3",
						  sizeof("semloom-provider-execution-v3"));
	semloom_semantic_hash_uint32(context, protocol_version);
	semloom_semantic_hash_slice(context, execution_id);
	semloom_semantic_hash_slice(context, spec->model_id);
	semloom_semantic_hash_finish(context, output);
}

static void
semloom_semantic_completion_digest(
	const SemloomWireSemanticIdentity *identity,
	AiByteSlice payload_digest,
	uint64 sequence,
	AiByteSlice raw_output,
	AiByteSlice finish_reason,
	AiByteSlice response_model,
	uint64 prompt_tokens,
	uint64 output_tokens,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1])
{
	pg_cryptohash_ctx *context;

	semloom_semantic_hash_begin(&context);
	semloom_semantic_hash_bytes(context,
						  identity->protocol_version == 4 ? "semloom-completion-v4" : "semloom-completion-v3",
						  sizeof("semloom-completion-v3"));
	semloom_semantic_hash_bytes(context,
						  identity->semantic_spec_digest,
						  SEMLOOM_SHA256_HEX_LENGTH);
	semloom_semantic_hash_bytes(context,
						  identity->physical_algorithm_digest,
						  SEMLOOM_SHA256_HEX_LENGTH);
	semloom_semantic_hash_bytes(context,
						  identity->provider_execution_digest,
						  SEMLOOM_SHA256_HEX_LENGTH);
	semloom_semantic_hash_bytes(context, payload_digest.data, payload_digest.length);
	semloom_semantic_hash_uint64(context, sequence);
	semloom_semantic_hash_slice(context, raw_output);
	semloom_semantic_hash_slice(context, finish_reason);
	semloom_semantic_hash_slice(context, response_model);
	semloom_semantic_hash_uint64(context, prompt_tokens);
	semloom_semantic_hash_uint64(context, output_tokens);
	semloom_semantic_hash_finish(context, output);
}

static void
semloom_semantic_hash_begin(pg_cryptohash_ctx **context)
{
	*context = pg_cryptohash_create(PG_SHA256);
	if (*context == NULL || pg_cryptohash_init(*context) < 0)
	{
		if (*context != NULL)
			pg_cryptohash_free(*context);
		elog(ERROR, "could not initialize SemLoom wire-v3 digest");
	}
}

static void
semloom_semantic_hash_bytes(pg_cryptohash_ctx *context,
						  const void *data,
						  Size length)
{
	if (length > 0 && pg_cryptohash_update(context, data, length) < 0)
	{
		pg_cryptohash_free(context);
		elog(ERROR, "could not update SemLoom wire-v3 digest");
	}
}

static void
semloom_semantic_hash_uint32(pg_cryptohash_ctx *context, uint32 value)
{
	uint8 bytes[4] = {
		(uint8) (value >> 24),
		(uint8) (value >> 16),
		(uint8) (value >> 8),
		(uint8) value,
	};

	semloom_semantic_hash_bytes(context, bytes, sizeof(bytes));
}

static void
semloom_semantic_hash_uint64(pg_cryptohash_ctx *context, uint64 value)
{
	uint8 bytes[8];
	int shift;

	for (shift = 7; shift >= 0; shift--)
		bytes[7 - shift] = (uint8) (value >> (shift * 8));
	semloom_semantic_hash_bytes(context, bytes, sizeof(bytes));
}

static void
semloom_semantic_hash_slice(pg_cryptohash_ctx *context, AiByteSlice value)
{
	semloom_semantic_hash_uint32(context, value.length);
	semloom_semantic_hash_bytes(context, value.data, value.length);
}

static void
semloom_semantic_hash_finish(pg_cryptohash_ctx *context,
						   char output[SEMLOOM_SHA256_HEX_LENGTH + 1])
{
	uint8 digest[PG_SHA256_DIGEST_LENGTH];
	static const char hex[] = "0123456789abcdef";
	int index;

	if (pg_cryptohash_final(context, digest, sizeof(digest)) < 0)
	{
		pg_cryptohash_free(context);
		elog(ERROR, "could not finish SemLoom wire-v3 digest");
	}
	pg_cryptohash_free(context);
	for (index = 0; index < PG_SHA256_DIGEST_LENGTH; index++)
	{
		output[index * 2] = hex[digest[index] >> 4];
		output[index * 2 + 1] = hex[digest[index] & 0x0f];
	}
	output[SEMLOOM_SHA256_HEX_LENGTH] = '\0';
}
