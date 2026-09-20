/* Binary image data over existing bounded, cancellable PostgreSQL UDS frames. */
#include "postgres.h"
#include "lib/stringinfo.h"
#include "utils/json.h"
#include "provider/provider_private.h"
#include "provider/wire/wire_common.h"
#include "provider/wire/wire_image.h"
#include "semantics/image_identity.h"
#include "semantics/semantic_image_contract.h"

static AiProviderStatus
invalid(AiProviderError *error)
{
	semloom_provider_error_set(error, AI_PROVIDER_ERROR_PROTOCOL, 0, 0, "invalid image provider message");
	return AI_PROVIDER_STATUS_ERROR;
}

static bool
slice(Jsonb *message, const char *key, AiByteSlice *result, AiProviderError *error)
{
	JsonbValue *value;

	if (!semloom_wire_common_json_value(message, key, &value, error) || value->type != jbvString)
		return false;
	result->data = (const uint8 *) value->val.string.val;
	result->length = value->val.string.len;
	return true;
}

static bool
equal_slice(Jsonb *message, const char *key, AiByteSlice expected, AiProviderError *error)
{
	AiByteSlice actual;

	return slice(message, key, &actual, error) && actual.length == expected.length &&
		memcmp(actual.data, expected.data, expected.length) == 0;
}

static bool
equal_text(Jsonb *message, const char *key, const char *expected, AiProviderError *error)
{
	return equal_slice(message, key, (AiByteSlice) {(const uint8 *) expected, strlen(expected)}, error);
}

static bool
uint64_field(Jsonb *message, const char *key, uint64 *result, AiProviderError *error)
{
	AiByteSlice value;
	uint32 index;
	uint64 number = 0;

	if (!slice(message, key, &value, error) || value.length < 1 || value.length > 20 ||
		(value.length > 1 && value.data[0] == '0'))
		return false;
	for (index = 0; index < value.length; index++)
	{
		uint8 digit = value.data[index];

		if (digit < '0' || digit > '9' || number > (PG_UINT64_MAX - (digit - '0')) / 10)
			return false;
		number = number * 10 + digit - '0';
	}
	*result = number;
	return true;
}

static AiProviderStatus
receive(pgsocket fd, const char *type, uint32 fields, Jsonb **message, AiProviderError *error)
{
	char *raw;
	int32 version;
	AiProviderStatus status = semloom_wire_common_receive_frame(fd, &raw, error);

	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	status = semloom_wire_common_parse_json_unique(raw, message, error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	if (!JB_ROOT_IS_OBJECT(*message) ||
		!semloom_wire_common_json_int32(*message, "protocol_version", &version, error) || version != 7)
		return invalid(error);
	if (equal_text(*message, "type", "error", error) && JB_ROOT_COUNT(*message) == 3 &&
		equal_text(*message, "code", "IMAGE_EXECUTION_FAILED", error))
	{
		semloom_provider_error_set(error, AI_PROVIDER_ERROR_INVALID_RESPONSE, 0, 0, "image provider execution failed");
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (JB_ROOT_COUNT(*message) != fields || !equal_text(*message, "type", type, error))
		return invalid(error);
	return AI_PROVIDER_STATUS_OK;
}

static void
append_slice(StringInfo output, const char *key, AiByteSlice value)
{
	appendStringInfo(output, ",\"%s\":", key);
	escape_json_with_len(output, (const char *) value.data, value.length);
}

static void
append_identities(StringInfo output, const AiOpenSpec *spec)
{
	char execution[65];

	semloom_image_execution_digest(spec, execution);
	append_slice(output, "semantic_spec_digest", spec->semantic_spec_digest);
	append_slice(output, "physical_algorithm_digest", spec->physical_algorithm_digest);
	append_slice(output, "provider_execution_digest", (AiByteSlice) {(const uint8 *) execution, 64});
}

static bool
identities_match(Jsonb *message, const AiOpenSpec *spec, AiProviderError *error)
{
	char execution[65];

	semloom_image_execution_digest(spec, execution);
	return equal_slice(message, "semantic_spec_digest", spec->semantic_spec_digest, error) &&
		equal_slice(message, "physical_algorithm_digest", spec->physical_algorithm_digest, error) &&
		equal_text(message, "provider_execution_digest", execution, error);
}

AiProviderStatus
semloom_wire_image_open(pgsocket fd, const AiOpenSpec *spec, uint32 window, AiProviderError *error)
{
	StringInfoData request;
	Jsonb *message;
	int32 actual_window;
	AiProviderStatus status;

	initStringInfo(&request);
	appendStringInfoString(&request, "{\"type\":\"open\",\"protocol_version\":7");
	append_identities(&request, spec);
	appendStringInfo(&request, ",\"provider_execution_id\":\"%s\",\"execution_mode\":\"%s\",\"max_inflight_tasks\":%u,\"plan\":{\"dimension\":%u,\"input_size\":%u",
		semloom_image_execution_id(spec->image_staged), spec->image_staged ? "staged" : "reference",
		window, spec->image_dimension, spec->image_input_size);
	append_slice(&request, "model_id", spec->model_id);
	append_slice(&request, "model_revision", spec->image_model_revision);
	append_slice(&request, "processor_id", spec->image_processor_id);
	append_slice(&request, "processor_revision", spec->image_processor_revision);
	append_slice(&request, "dtype", spec->image_dtype);
	appendStringInfoString(&request, "}}");
	status = semloom_wire_common_send_frame(fd, request.data, request.len, error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	status = receive(fd, "opened", 6, &message, error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	if (!identities_match(message, spec, error) ||
		!semloom_wire_common_json_int32(message, "max_inflight_tasks", &actual_window, error) ||
		actual_window != window)
		return invalid(error);
	return AI_PROVIDER_STATUS_OK;
}

AiProviderStatus
semloom_wire_image_task(pgsocket fd, const AiOpenSpec *spec, const AiPreparedTask *task,
	bool *accepted, AiProviderError *error)
{
	StringInfoData request;
	Jsonb *message;
	AiProviderStatus status;
	uint64 sequence;
	uint32 index;
	const char *hex = "0123456789abcdef";

	if (task->is_null || task->input.data == NULL || task->input.length < 1 ||
		task->input.length > SEMLOOM_IMAGE_MAX_INPUT_BYTES || task->canonical_messages.length != 0)
		return invalid(error);
	initStringInfo(&request);
	appendStringInfo(&request, "{\"type\":\"%s\",\"protocol_version\":7,\"sequence\":\"" UINT64_FORMAT "\"",
		spec->image_staged ? "offer" : "task", task->sequence);
	append_identities(&request, spec);
	append_slice(&request, "semantic_payload_digest", task->semantic_payload_digest);
	appendStringInfoString(&request, ",\"encoded_hex\":\"");
	enlargeStringInfo(&request, task->input.length * 2 + 3);
	for (index = 0; index < task->input.length; index++)
	{
		appendStringInfoCharMacro(&request, hex[task->input.data[index] >> 4]);
		appendStringInfoCharMacro(&request, hex[task->input.data[index] & 15]);
	}
	appendStringInfoString(&request, "\"}");
	status = semloom_wire_common_send_frame(fd, request.data, request.len, error);
	if (status != AI_PROVIDER_STATUS_OK || !spec->image_staged)
		return status;
	status = receive(fd, "offered", 4, &message, error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	if (accepted == NULL || !uint64_field(message, "sequence", &sequence, error) || sequence != task->sequence ||
		!semloom_wire_common_json_bool(message, "accepted", accepted, error))
		return invalid(error);
	return AI_PROVIDER_STATUS_OK;
}

static int
hex_digit(uint8 value)
{
	if (value >= '0' && value <= '9') return value - '0';
	if (value >= 'a' && value <= 'f') return value - 'a' + 10;
	return -1;
}

AiProviderStatus
semloom_wire_image_collect(pgsocket fd, const AiOpenSpec *spec, const AiPreparedTask *pending,
	uint32 count, AiCompletion *completion, AiProviderError *error)
{
	Jsonb *message;
	AiProviderStatus status = receive(fd, "completion", 9, &message, error);
	AiByteSlice encoded;
	AiByteSlice payload = {0};
	uint64 sequence;
	uint32 index;
	uint8 *bytes;
	char evidence[65];

	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	if (!identities_match(message, spec, error) || !uint64_field(message, "sequence", &sequence, error))
		return invalid(error);
	for (index = 0; index < count; index++)
		if (pending[index].semantic_payload_digest.length && pending[index].sequence == sequence)
		{
			payload = pending[index].semantic_payload_digest;
			break;
		}
	if (payload.length != 64 || !equal_slice(message, "semantic_payload_digest", payload, error) ||
		!slice(message, "output_hex", &encoded, error) || encoded.length != spec->image_dimension * 8)
		return invalid(error);
	bytes = palloc(spec->image_dimension * 4);
	for (index = 0; index < encoded.length; index += 2)
	{
		int high = hex_digit(encoded.data[index]);
		int low = hex_digit(encoded.data[index + 1]);

		if (high < 0 || low < 0) return invalid(error);
		bytes[index / 2] = (uint8) ((high << 4) | low);
	}
	if (!semloom_image_vector_valid(bytes, encoded.length / 2, spec->image_dimension))
		return invalid(error);
	completion->output = (AiByteSlice) {bytes, encoded.length / 2};
	semloom_image_completion_digest(spec, payload, sequence, completion->output, evidence);
	if (!equal_text(message, "completion_evidence_digest", evidence, error))
		return invalid(error);
	completion->sequence = sequence;
	completion->is_null = false;
	completion->response_model_id = spec->model_id;
	completion->finish_reason = (AiByteSlice) {(const uint8 *) "stop", 4};
	completion->prompt_tokens = completion->output_tokens = 0;
	return AI_PROVIDER_STATUS_OK;
}
