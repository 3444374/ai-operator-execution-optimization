#include "postgres.h"

#include <errno.h>
#include <sys/socket.h>

#include "common/cryptohash.h"
#include "common/sha2.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/fmgrprotos.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/wait_classes.h"

#include "provider_private.h"
#include "wire_v2.h"

#define SEMLOOM_SEMANTIC_SPEC_DIGEST_DOMAIN "semloom-semantic-spec-v1\0"
#define SEMLOOM_PHYSICAL_ALGORITHM_DIGEST_DOMAIN "semloom-physical-algorithm-v1\0"
#define SEMLOOM_PROVIDER_EXECUTION_DIGEST_DOMAIN "semloom-provider-execution-v1\0"
#define SEMLOOM_PAYLOAD_DIGEST_DOMAIN "semloom-payload-v1\0"
#define SEMLOOM_COMPLETION_DIGEST_DOMAIN "semloom-completion-v2\0"

static void semloom_hash_begin(pg_cryptohash_ctx **context);
static void semloom_hash_bytes(pg_cryptohash_ctx *context,
							   const void *data,
							   Size length);
static void semloom_hash_cstring(pg_cryptohash_ctx *context, const char *value);
static void semloom_hash_slice(pg_cryptohash_ctx *context, AiByteSlice value);
static void semloom_hash_uint32(pg_cryptohash_ctx *context, uint32 value);
static void semloom_hash_uint64(pg_cryptohash_ctx *context, uint64 value);
static void semloom_hash_finish(pg_cryptohash_ctx *context,
								char output[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1]);
static void semloom_semantic_spec_digest(
	const AiOpenSpec *spec,
	char output[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1]);
static void semloom_physical_algorithm_digest(
	const AiOpenSpec *spec,
	char output[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1]);
static void semloom_provider_execution_digest(
	const char *provider_execution_id,
	char output[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1]);
static void semloom_payload_digest(
	const AiPreparedTask *task,
	char output[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1]);
static void semloom_completion_digest(
	const SemloomWireV2Identity *identity,
	const char payload_digest[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1],
	uint64 sequence,
	bool is_null,
	AiByteSlice output_value,
	char output[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1]);
static AiProviderStatus semloom_send_frame(pgsocket socket_fd,
										   const char *payload,
										   Size payload_length,
										   AiProviderError *error);
static AiProviderStatus semloom_receive_frame(pgsocket socket_fd,
										  char **payload,
										  AiProviderError *error);
static AiProviderStatus semloom_socket_write_all(pgsocket socket_fd,
												 const char *data,
												 Size length,
												 AiProviderError *error);
static AiProviderStatus semloom_socket_read_all(pgsocket socket_fd,
												char *data,
												Size length,
												AiProviderError *error);
static void semloom_wait_for_socket(pgsocket socket_fd, int socket_event);
static AiProviderStatus semloom_parse_json(const char *payload,
										   Jsonb **message,
										   AiProviderError *error);
static bool semloom_json_value(Jsonb *message,
							   const char *key,
							   JsonbValue **value,
							   AiProviderError *error);
static bool semloom_json_string_equals(Jsonb *message,
									   const char *key,
									   const char *expected,
									   bool *matches,
									   AiProviderError *error);
static bool semloom_json_int32(Jsonb *message,
							   const char *key,
							   int32 *result,
							   AiProviderError *error);
static bool semloom_json_bool(Jsonb *message,
							  const char *key,
							  bool *result,
							  AiProviderError *error);
static bool semloom_validate_response_type(Jsonb *message,
										   const char *expected_type,
										   uint32 expected_fields,
										   AiProviderError *error);

void
semloom_wire_v2_identity_init(const AiOpenSpec *spec,
							  const char *provider_execution_id,
							  SemloomWireV2Identity *identity)
{
	Assert(spec != NULL);
	Assert(provider_execution_id != NULL);
	Assert(identity != NULL);
	identity->provider_execution_id = provider_execution_id;
	semloom_semantic_spec_digest(spec, identity->semantic_spec_digest);
	semloom_physical_algorithm_digest(spec, identity->physical_algorithm_digest);
	semloom_provider_execution_digest(provider_execution_id,
										  identity->provider_execution_digest);
}

AiProviderStatus
semloom_wire_v2_open(pgsocket socket_fd,
					 const AiOpenSpec *spec,
					 const SemloomWireV2Identity *identity,
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
					 SEMLOOM_WIRE_V2_PROTOCOL_VERSION,
					 identity->semantic_spec_digest,
					 identity->physical_algorithm_digest,
					 identity->provider_execution_digest);
	escape_json(&request, identity->provider_execution_id);
	appendStringInfoString(&request,
						 ",\"operator_kind\":\"SEM_MAP\",\"semantic_spec_id\":");
	escape_json_with_len(&request,
						 (const char *) spec->semantic_spec_id.data,
						 spec->semantic_spec_id.length);
	appendStringInfo(&request,
					 ",\"semantic_spec_version\":%u,\"physical_algorithm\":",
					 spec->semantic_spec_version);
	escape_json_with_len(&request,
						 (const char *) spec->physical_algorithm.data,
						 spec->physical_algorithm.length);
	appendStringInfoString(&request,
						 ",\"null_policy\":\"PROPAGATE_NULL\","
						 "\"error_policy\":\"FAIL_QUERY\","
						 "\"input_type\":\"text\",\"output_type\":\"text\"}");
	status = semloom_send_frame(socket_fd, request.data, request.len, error);
	pfree(request.data);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	status = semloom_receive_frame(socket_fd, &response, error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	status = semloom_parse_json(response, &message, error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	if (!semloom_validate_response_type(message, "opened", 8, error))
		return AI_PROVIDER_STATUS_ERROR;
	if (!semloom_json_int32(message, "protocol_version", &integer_value, error))
		return AI_PROVIDER_STATUS_ERROR;
	if (integer_value != SEMLOOM_WIRE_V2_PROTOCOL_VERSION)
		goto mismatch;
	if (!semloom_json_string_equals(message,
									"semantic_spec_digest",
									identity->semantic_spec_digest,
									&matches,
									error) || !matches)
		goto mismatch_or_error;
	if (!semloom_json_string_equals(message,
									"physical_algorithm_digest",
									identity->physical_algorithm_digest,
									&matches,
									error) || !matches)
		goto mismatch_or_error;
	if (!semloom_json_string_equals(message,
									"provider_execution_digest",
									identity->provider_execution_digest,
									&matches,
									error) || !matches)
		goto mismatch_or_error;
	if (!semloom_json_int32(message, "max_inflight_tasks", &integer_value, error))
		return AI_PROVIDER_STATUS_ERROR;
	if (integer_value != 1)
		goto mismatch;
	if (!semloom_json_int32(message, "max_frame_bytes", &integer_value, error))
		return AI_PROVIDER_STATUS_ERROR;
	if (integer_value != SEMLOOM_WIRE_V2_MAX_FRAME_BYTES)
		goto mismatch;
	if (!semloom_json_int32(message, "max_input_bytes", &integer_value, error))
		return AI_PROVIDER_STATUS_ERROR;
	if (integer_value != SEMLOOM_WIRE_V2_MAX_INPUT_BYTES)
		goto mismatch;
	return AI_PROVIDER_STATUS_OK;

mismatch_or_error:
	if (error->code != AI_PROVIDER_ERROR_NONE)
		return AI_PROVIDER_STATUS_ERROR;
mismatch:
	semloom_provider_error_set(error,
							   AI_PROVIDER_ERROR_PROTOCOL,
							   AI_PROVIDER_OPERATION_OPEN_RESPONSE,
							   0,
							   NULL);
	return AI_PROVIDER_STATUS_ERROR;
}

AiProviderStatus
semloom_wire_v2_drive(pgsocket socket_fd,
					  const AiPreparedTask *task,
					  const SemloomWireV2Identity *identity,
					  AiCompletion *completion,
					  AiProviderError *error)
{
	char payload_digest[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1];
	char expected_evidence_digest[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1];
	char sequence[32];
	StringInfoData request;
	char *response;
	Jsonb *message;
	JsonbValue *output_value;
	int32 protocol_version;
	bool output_is_null;
	bool matches;
	AiByteSlice output_slice = {0};
	AiProviderStatus status;

	semloom_payload_digest(task, payload_digest);
	pg_snprintf(sequence, sizeof(sequence), UINT64_FORMAT, task->sequence);
	initStringInfo(&request);
	appendStringInfo(&request,
					 "{\"type\":\"task\",\"protocol_version\":%d,"
					 "\"sequence\":\"%s\",\"semantic_spec_digest\":\"%s\","
					 "\"physical_algorithm_digest\":\"%s\","
					 "\"provider_execution_digest\":\"%s\","
					 "\"payload_digest\":\"%s\",\"is_null\":false,\"input\":",
					 SEMLOOM_WIRE_V2_PROTOCOL_VERSION,
					 sequence,
					 identity->semantic_spec_digest,
					 identity->physical_algorithm_digest,
					 identity->provider_execution_digest,
					 payload_digest);
	escape_json_with_len(&request,
						 (const char *) task->input.data,
						 task->input.length);
	appendStringInfoChar(&request, '}');
	status = semloom_send_frame(socket_fd, request.data, request.len, error);
	pfree(request.data);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	status = semloom_receive_frame(socket_fd, &response, error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	status = semloom_parse_json(response, &message, error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	if (!semloom_validate_response_type(message, "completion", 10, error))
		return AI_PROVIDER_STATUS_ERROR;
	if (!semloom_json_int32(message, "protocol_version", &protocol_version, error))
		return AI_PROVIDER_STATUS_ERROR;
	if (protocol_version != SEMLOOM_WIRE_V2_PROTOCOL_VERSION)
		goto identity_mismatch;
	if (!semloom_json_string_equals(message, "sequence", sequence, &matches, error) ||
		!matches)
		goto identity_mismatch_or_error;
	if (!semloom_json_string_equals(message,
									"semantic_spec_digest",
									identity->semantic_spec_digest,
									&matches,
									error) || !matches)
		goto identity_mismatch_or_error;
	if (!semloom_json_string_equals(message,
									"physical_algorithm_digest",
									identity->physical_algorithm_digest,
									&matches,
									error) || !matches)
		goto identity_mismatch_or_error;
	if (!semloom_json_string_equals(message,
									"provider_execution_digest",
									identity->provider_execution_digest,
									&matches,
									error) || !matches)
		goto identity_mismatch_or_error;
	if (!semloom_json_string_equals(message,
									"payload_digest",
									payload_digest,
									&matches,
									error) || !matches)
		goto identity_mismatch_or_error;

	if (!semloom_json_bool(message, "is_null", &output_is_null, error) ||
		!semloom_json_value(message, "output", &output_value, error))
		return AI_PROVIDER_STATUS_ERROR;
	if ((output_is_null && output_value->type != jbvNull) ||
		(!output_is_null && output_value->type != jbvString))
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   AI_PROVIDER_OPERATION_COMPLETION_OUTPUT,
								   0,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (!output_is_null)
	{
		output_slice.data = (const uint8 *) output_value->val.string.val;
		output_slice.length = (uint32) output_value->val.string.len;
	}
	semloom_completion_digest(identity,
							  payload_digest,
							  task->sequence,
							  output_is_null,
							  output_slice,
							  expected_evidence_digest);
	if (!semloom_json_string_equals(message,
									"evidence_digest",
									expected_evidence_digest,
									&matches,
									error))
		return AI_PROVIDER_STATUS_ERROR;
	if (!matches)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   AI_PROVIDER_OPERATION_COMPLETION_EVIDENCE,
								   0,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}

	completion->sequence = task->sequence;
	completion->is_null = output_is_null;
	completion->output = output_slice;
	return AI_PROVIDER_STATUS_OK;

identity_mismatch_or_error:
	if (error->code != AI_PROVIDER_ERROR_NONE)
		return AI_PROVIDER_STATUS_ERROR;
identity_mismatch:
	semloom_provider_error_set(error,
							   AI_PROVIDER_ERROR_PROTOCOL,
							   AI_PROVIDER_OPERATION_COMPLETION_IDENTITY,
							   0,
							   NULL);
	return AI_PROVIDER_STATUS_ERROR;
}

AiProviderStatus
semloom_wire_v2_wait_connected(pgsocket socket_fd, AiProviderError *error)
{
	int socket_error = 0;
	socklen_t option_length = sizeof(socket_error);

	semloom_wait_for_socket(socket_fd, WL_SOCKET_WRITEABLE);
	if (getsockopt(socket_fd,
				   SOL_SOCKET,
				   SO_ERROR,
				   &socket_error,
				   &option_length) != 0)
	{
		int saved_errno = errno;

		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_SYSTEM,
								   AI_PROVIDER_OPERATION_INSPECT_SOCKET,
								   saved_errno,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (socket_error != 0)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_SYSTEM,
								   AI_PROVIDER_OPERATION_CONNECT_SOCKET,
								   socket_error,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	return AI_PROVIDER_STATUS_OK;
}

void
semloom_wire_v2_wait_connect_retry(void)
{
	int events;

	events = WaitLatch(MyLatch,
					   WL_EXIT_ON_PM_DEATH | WL_LATCH_SET | WL_TIMEOUT,
					   10L,
					   PG_WAIT_EXTENSION);
	if (events & WL_LATCH_SET)
		ResetLatch(MyLatch);
	CHECK_FOR_INTERRUPTS();
}

static void
semloom_semantic_spec_digest(
	const AiOpenSpec *spec,
	char output[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1])
{
	pg_cryptohash_ctx *context;

	semloom_hash_begin(&context);
	semloom_hash_bytes(context,
					   SEMLOOM_SEMANTIC_SPEC_DIGEST_DOMAIN,
					   sizeof(SEMLOOM_SEMANTIC_SPEC_DIGEST_DOMAIN) - 1);
	semloom_hash_cstring(context, "SEM_MAP");
	semloom_hash_slice(context, spec->semantic_spec_id);
	semloom_hash_uint32(context, spec->semantic_spec_version);
	semloom_hash_cstring(context, "PROPAGATE_NULL");
	semloom_hash_cstring(context, "FAIL_QUERY");
	semloom_hash_cstring(context, "text");
	semloom_hash_cstring(context, "text");
	semloom_hash_finish(context, output);
}

static void
semloom_physical_algorithm_digest(
	const AiOpenSpec *spec,
	char output[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1])
{
	pg_cryptohash_ctx *context;

	semloom_hash_begin(&context);
	semloom_hash_bytes(context,
					   SEMLOOM_PHYSICAL_ALGORITHM_DIGEST_DOMAIN,
					   sizeof(SEMLOOM_PHYSICAL_ALGORITHM_DIGEST_DOMAIN) - 1);
	semloom_hash_slice(context, spec->physical_algorithm);
	semloom_hash_finish(context, output);
}

static void
semloom_provider_execution_digest(
	const char *provider_execution_id,
	char output[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1])
{
	pg_cryptohash_ctx *context;

	semloom_hash_begin(&context);
	semloom_hash_bytes(context,
					   SEMLOOM_PROVIDER_EXECUTION_DIGEST_DOMAIN,
					   sizeof(SEMLOOM_PROVIDER_EXECUTION_DIGEST_DOMAIN) - 1);
	semloom_hash_cstring(context, provider_execution_id);
	semloom_hash_finish(context, output);
}

static void
semloom_payload_digest(
	const AiPreparedTask *task,
	char output[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1])
{
	pg_cryptohash_ctx *context;
	uint8 null_flag = task->is_null ? 1 : 0;

	semloom_hash_begin(&context);
	semloom_hash_bytes(context,
					   SEMLOOM_PAYLOAD_DIGEST_DOMAIN,
					   sizeof(SEMLOOM_PAYLOAD_DIGEST_DOMAIN) - 1);
	semloom_hash_bytes(context, &null_flag, sizeof(null_flag));
	semloom_hash_uint64(context, task->is_null ? 0 : task->input.length);
	if (!task->is_null)
		semloom_hash_bytes(context, task->input.data, task->input.length);
	semloom_hash_finish(context, output);
}

static void
semloom_completion_digest(
	const SemloomWireV2Identity *identity,
	const char payload_digest[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1],
	uint64 sequence,
	bool is_null,
	AiByteSlice output_value,
	char output[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1])
{
	pg_cryptohash_ctx *context;
	uint8 null_flag = is_null ? 1 : 0;

	semloom_hash_begin(&context);
	semloom_hash_bytes(context,
					   SEMLOOM_COMPLETION_DIGEST_DOMAIN,
					   sizeof(SEMLOOM_COMPLETION_DIGEST_DOMAIN) - 1);
	semloom_hash_bytes(context,
					   identity->semantic_spec_digest,
					   SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH);
	semloom_hash_bytes(context,
					   identity->physical_algorithm_digest,
					   SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH);
	semloom_hash_bytes(context,
					   identity->provider_execution_digest,
					   SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH);
	semloom_hash_bytes(context, payload_digest, SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH);
	semloom_hash_uint64(context, sequence);
	semloom_hash_bytes(context, &null_flag, sizeof(null_flag));
	semloom_hash_uint64(context, is_null ? 0 : output_value.length);
	if (!is_null)
		semloom_hash_bytes(context, output_value.data, output_value.length);
	semloom_hash_finish(context, output);
}

static AiProviderStatus
semloom_send_frame(pgsocket socket_fd,
				   const char *payload,
				   Size payload_length,
				   AiProviderError *error)
{
	uint8 header[4];
	AiProviderStatus status;

	if (payload_length == 0 || payload_length > SEMLOOM_WIRE_V2_MAX_FRAME_BYTES)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_FRAME_LIMIT,
								   AI_PROVIDER_OPERATION_SEND_FRAME,
								   0,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	header[0] = (uint8) (payload_length >> 24);
	header[1] = (uint8) (payload_length >> 16);
	header[2] = (uint8) (payload_length >> 8);
	header[3] = (uint8) payload_length;
	status = semloom_socket_write_all(socket_fd,
									  (const char *) header,
									  sizeof(header),
									  error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	return semloom_socket_write_all(socket_fd, payload, payload_length, error);
}

static AiProviderStatus
semloom_receive_frame(pgsocket socket_fd, char **payload, AiProviderError *error)
{
	uint8 header[4];
	uint32 payload_length;
	AiProviderStatus status;

	status = semloom_socket_read_all(socket_fd, (char *) header, sizeof(header), error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	payload_length = ((uint32) header[0] << 24) |
		((uint32) header[1] << 16) |
		((uint32) header[2] << 8) |
		(uint32) header[3];
	if (payload_length == 0 || payload_length > SEMLOOM_WIRE_V2_MAX_FRAME_BYTES)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   AI_PROVIDER_OPERATION_RECEIVE_FRAME,
								   0,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	*payload = palloc(payload_length + 1);
	status = semloom_socket_read_all(socket_fd, *payload, payload_length, error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	(*payload)[payload_length] = '\0';
	return AI_PROVIDER_STATUS_OK;
}

static AiProviderStatus
semloom_socket_write_all(pgsocket socket_fd,
						 const char *data,
						 Size length,
						 AiProviderError *error)
{
	Size written = 0;

	while (written < length)
	{
		ssize_t result;
		int send_flags = 0;

		CHECK_FOR_INTERRUPTS();
#ifdef MSG_NOSIGNAL
		send_flags = MSG_NOSIGNAL;
#endif
		result = send(socket_fd, data + written, length - written, send_flags);
		if (result > 0)
		{
			written += result;
			continue;
		}
		if (result < 0 && errno == EINTR)
			continue;
		if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
		{
			semloom_wait_for_socket(socket_fd, WL_SOCKET_WRITEABLE);
			continue;
		}
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_SYSTEM,
								   AI_PROVIDER_OPERATION_WRITE_SOCKET,
								   errno,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	return AI_PROVIDER_STATUS_OK;
}

static AiProviderStatus
semloom_socket_read_all(pgsocket socket_fd,
						char *data,
						Size length,
						AiProviderError *error)
{
	Size received = 0;

	while (received < length)
	{
		ssize_t result;

		CHECK_FOR_INTERRUPTS();
		result = recv(socket_fd, data + received, length - received, 0);
		if (result > 0)
		{
			received += result;
			continue;
		}
		if (result == 0)
		{
			semloom_provider_error_set(error,
									   AI_PROVIDER_ERROR_CONNECTION_LOST,
									   AI_PROVIDER_OPERATION_READ_SOCKET,
									   0,
									   NULL);
			return AI_PROVIDER_STATUS_ERROR;
		}
		if (errno == EINTR)
			continue;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
		{
			semloom_wait_for_socket(socket_fd, WL_SOCKET_READABLE);
			continue;
		}
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_SYSTEM,
								   AI_PROVIDER_OPERATION_READ_SOCKET,
								   errno,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	return AI_PROVIDER_STATUS_OK;
}

static void
semloom_wait_for_socket(pgsocket socket_fd, int socket_event)
{
	for (;;)
	{
		int events;

		events = WaitLatchOrSocket(MyLatch,
								   WL_EXIT_ON_PM_DEATH | WL_LATCH_SET | socket_event,
								   socket_fd,
								   0,
								   PG_WAIT_EXTENSION);
		if (events & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}
		if (events & socket_event)
			return;
	}
}

static AiProviderStatus
semloom_parse_json(const char *payload, Jsonb **message, AiProviderError *error)
{
	bool expected_input_error = false;

	*message = NULL;
	PG_TRY();
	{
		*message = DatumGetJsonbP(DirectFunctionCall1(jsonb_in,
												 CStringGetDatum(payload)));
	}
	PG_CATCH();
	{
		ErrorData *error_data = CopyErrorData();

		if (error_data->sqlerrcode == ERRCODE_INVALID_TEXT_REPRESENTATION ||
			error_data->sqlerrcode == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
		{
			FlushErrorState();
			expected_input_error = true;
			FreeErrorData(error_data);
		}
		else
		{
			FreeErrorData(error_data);
			PG_RE_THROW();
		}
	}
	PG_END_TRY();
	if (expected_input_error)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   AI_PROVIDER_OPERATION_PARSE_JSON,
								   0,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (!JB_ROOT_IS_OBJECT(*message))
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   AI_PROVIDER_OPERATION_RESPONSE_OBJECT,
								   0,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	return AI_PROVIDER_STATUS_OK;
}

static bool
semloom_json_value(Jsonb *message,
				   const char *key,
				   JsonbValue **value,
				   AiProviderError *error)
{
	*value = getKeyJsonValueFromContainer(&message->root,
									  key,
									  strlen(key),
									  NULL);
	if (*value != NULL)
		return true;
	semloom_provider_error_set(error,
							   AI_PROVIDER_ERROR_PROTOCOL,
							   AI_PROVIDER_OPERATION_RESPONSE_FIELD,
							   0,
							   NULL);
	return false;
}

static bool
semloom_json_string_equals(Jsonb *message,
						   const char *key,
						   const char *expected,
						   bool *matches,
						   AiProviderError *error)
{
	JsonbValue *value;
	Size expected_length = strlen(expected);

	if (!semloom_json_value(message, key, &value, error))
		return false;
	*matches = value->type == jbvString &&
		value->val.string.len == expected_length &&
		memcmp(value->val.string.val, expected, expected_length) == 0;
	return true;
}

static bool
semloom_json_int32(Jsonb *message,
				   const char *key,
				   int32 *result,
				   AiProviderError *error)
{
	JsonbValue *value;
	bool expected_range_error = false;

	if (!semloom_json_value(message, key, &value, error))
		return false;
	if (value->type != jbvNumeric)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   AI_PROVIDER_OPERATION_RESPONSE_INTEGER,
								   0,
								   NULL);
		return false;
	}
	PG_TRY();
	{
		*result = DatumGetInt32(DirectFunctionCall1(numeric_int4,
											 NumericGetDatum(value->val.numeric)));
	}
	PG_CATCH();
	{
		ErrorData *error_data = CopyErrorData();

		if (error_data->sqlerrcode == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
		{
			FlushErrorState();
			expected_range_error = true;
			FreeErrorData(error_data);
		}
		else
		{
			FreeErrorData(error_data);
			PG_RE_THROW();
		}
	}
	PG_END_TRY();
	if (!expected_range_error)
		return true;
	semloom_provider_error_set(error,
							   AI_PROVIDER_ERROR_NUMERIC_RANGE,
							   AI_PROVIDER_OPERATION_RESPONSE_INTEGER,
							   0,
							   NULL);
	return false;
}

static bool
semloom_json_bool(Jsonb *message,
				  const char *key,
				  bool *result,
				  AiProviderError *error)
{
	JsonbValue *value;

	if (!semloom_json_value(message, key, &value, error))
		return false;
	if (value->type != jbvBool)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   AI_PROVIDER_OPERATION_RESPONSE_BOOLEAN,
								   0,
								   NULL);
		return false;
	}
	*result = value->val.boolean;
	return true;
}

static bool
semloom_validate_response_type(Jsonb *message,
							   const char *expected_type,
							   uint32 expected_fields,
							   AiProviderError *error)
{
	bool matches;

	if (!semloom_json_string_equals(message, "type", "error", &matches, error))
		return false;
	if (matches)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   AI_PROVIDER_OPERATION_PROVIDER_REJECTED,
								   0,
								   NULL);
		return false;
	}
	if (JsonContainerSize(&message->root) != expected_fields)
		goto unexpected;
	if (!semloom_json_string_equals(message, "type", expected_type, &matches, error))
		return false;
	if (matches)
		return true;

unexpected:
	semloom_provider_error_set(error,
							   AI_PROVIDER_ERROR_PROTOCOL,
							   AI_PROVIDER_OPERATION_UNEXPECTED_MESSAGE,
							   0,
							   NULL);
	return false;
}

static void
semloom_hash_begin(pg_cryptohash_ctx **context)
{
	*context = pg_cryptohash_create(PG_SHA256);
	if (*context == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not initialize SemLoom SHA-256 digest")));
	if (pg_cryptohash_init(*context) < 0)
	{
		pg_cryptohash_free(*context);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not initialize SemLoom SHA-256 digest")));
	}
}

static void
semloom_hash_bytes(pg_cryptohash_ctx *context, const void *data, Size length)
{
	if (length > 0 && pg_cryptohash_update(context, data, length) < 0)
	{
		pg_cryptohash_free(context);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not update SemLoom SHA-256 digest")));
	}
}

static void
semloom_hash_cstring(pg_cryptohash_ctx *context, const char *value)
{
	Size length = strlen(value);

	if (length > PG_UINT32_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("SemLoom digest field is too long")));
	semloom_hash_uint32(context, (uint32) length);
	semloom_hash_bytes(context, value, length);
}

static void
semloom_hash_slice(pg_cryptohash_ctx *context, AiByteSlice value)
{
	semloom_hash_uint32(context, value.length);
	semloom_hash_bytes(context, value.data, value.length);
}

static void
semloom_hash_uint32(pg_cryptohash_ctx *context, uint32 value)
{
	uint8 encoded[4];

	encoded[0] = (uint8) (value >> 24);
	encoded[1] = (uint8) (value >> 16);
	encoded[2] = (uint8) (value >> 8);
	encoded[3] = (uint8) value;
	semloom_hash_bytes(context, encoded, sizeof(encoded));
}

static void
semloom_hash_uint64(pg_cryptohash_ctx *context, uint64 value)
{
	uint8 encoded[8];
	int index;

	for (index = 0; index < lengthof(encoded); index++)
		encoded[index] = (uint8) (value >> (56 - index * 8));
	semloom_hash_bytes(context, encoded, sizeof(encoded));
}

static void
semloom_hash_finish(pg_cryptohash_ctx *context,
						char output[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1])
{
	static const char hex_digits[] = "0123456789abcdef";
	uint8 digest[PG_SHA256_DIGEST_LENGTH];
	int index;

	if (pg_cryptohash_final(context, digest, sizeof(digest)) < 0)
	{
		pg_cryptohash_free(context);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not finish SemLoom SHA-256 digest")));
	}
	pg_cryptohash_free(context);
	for (index = 0; index < lengthof(digest); index++)
	{
		output[index * 2] = hex_digits[digest[index] >> 4];
		output[index * 2 + 1] = hex_digits[digest[index] & 0x0f];
	}
	output[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH] = '\0';
}
