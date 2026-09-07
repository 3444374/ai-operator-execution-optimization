/* Shared bounded framing, socket waits, and PostgreSQL JSON primitives. */
#include "postgres.h"

#include <errno.h>
#include <sys/socket.h>

#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/fmgrprotos.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/memutils.h"
#include "utils/wait_classes.h"

#include "provider/provider_private.h"
#include "provider/wire/wire_common.h"

static AiProviderStatus semloom_wire_common_parse_json_internal(
	const char *payload, Jsonb **message, bool unique_keys, AiProviderError *error);

static AiProviderStatus semloom_wire_common_socket_write_all(
	pgsocket socket_fd,
	const char *data,
	Size length,
	AiProviderError *error);
static AiProviderStatus semloom_wire_common_socket_read_all(
	pgsocket socket_fd,
	char *data,
	Size length,
	AiProviderError *error);
static void semloom_wire_common_wait_for_socket(pgsocket socket_fd,
											 int socket_event);

AiProviderStatus
semloom_wire_common_send_frame(pgsocket socket_fd,
							   const char *payload,
							   Size payload_length,
							   AiProviderError *error)
{
	uint8 header[4];
	AiProviderStatus status;

	if (payload_length == 0 || payload_length > SEMLOOM_WIRE_COMMON_MAX_FRAME_BYTES)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_MESSAGE_TOO_LARGE,
								   0,
								   0,
								   "SemLoom provider frame length is outside the protocol limit");
		return AI_PROVIDER_STATUS_ERROR;
	}
	header[0] = (uint8) (payload_length >> 24);
	header[1] = (uint8) (payload_length >> 16);
	header[2] = (uint8) (payload_length >> 8);
	header[3] = (uint8) payload_length;
	status = semloom_wire_common_socket_write_all(socket_fd,
											  (const char *) header,
											  sizeof(header),
											  error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	return semloom_wire_common_socket_write_all(socket_fd,
											payload,
											payload_length,
											error);
}

AiProviderStatus
semloom_wire_common_receive_frame(pgsocket socket_fd,
								  char **payload,
								  AiProviderError *error)
{
	uint8 header[4];
	uint32 payload_length;
	AiProviderStatus status;

	status = semloom_wire_common_socket_read_all(socket_fd,
											 (char *) header,
											 sizeof(header),
											 error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	payload_length = ((uint32) header[0] << 24) |
		((uint32) header[1] << 16) |
		((uint32) header[2] << 8) |
		(uint32) header[3];
	if (payload_length == 0 || payload_length > SEMLOOM_WIRE_COMMON_MAX_FRAME_BYTES)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   0,
								   0,
								   "SemLoom provider returned an invalid frame length");
		return AI_PROVIDER_STATUS_ERROR;
	}
	*payload = palloc(payload_length + 1);
	status = semloom_wire_common_socket_read_all(socket_fd,
											 *payload,
											 payload_length,
											 error);
	if (status != AI_PROVIDER_STATUS_OK)
		return status;
	if (memchr(*payload, '\0', payload_length) != NULL)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   0,
								   0,
								   "SemLoom provider returned invalid JSON");
		return AI_PROVIDER_STATUS_ERROR;
	}
	(*payload)[payload_length] = '\0';
	return AI_PROVIDER_STATUS_OK;
}

AiProviderStatus
semloom_wire_common_wait_connected(pgsocket socket_fd, AiProviderError *error)
{
	int socket_error = 0;
	socklen_t option_length = sizeof(socket_error);

	semloom_wire_common_wait_for_socket(socket_fd, WL_SOCKET_WRITEABLE);
	if (getsockopt(socket_fd,
				   SOL_SOCKET,
				   SO_ERROR,
				   &socket_error,
				   &option_length) != 0)
	{
		int saved_errno = errno;

		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_SYSTEM,
								   saved_errno,
								   0,
								   "could not inspect SemLoom provider socket connection");
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (socket_error != 0)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_SYSTEM,
								   socket_error,
								   0,
								   "could not connect to SemLoom provider socket");
		return AI_PROVIDER_STATUS_ERROR;
	}
	return AI_PROVIDER_STATUS_OK;
}

void
semloom_wire_common_wait_connect_retry(void)
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

AiProviderStatus
semloom_wire_common_parse_json(const char *payload,
							   Jsonb **message,
							   AiProviderError *error)
{
	return semloom_wire_common_parse_json_internal(payload, message, false, error);
}

AiProviderStatus
semloom_wire_common_parse_json_unique(const char *payload, Jsonb **message,
	AiProviderError *error)
{
	return semloom_wire_common_parse_json_internal(payload, message, true, error);
}

static AiProviderStatus
semloom_wire_common_parse_json_internal(const char *payload, Jsonb **message,
	bool unique_keys, AiProviderError *error)
{
	MemoryContext parse_context = CurrentMemoryContext;
	bool expected_input_error = false;

	*message = NULL;
	PG_TRY();
	{
		if (unique_keys && !json_validate(cstring_to_text(payload), true, false))
			expected_input_error = true;
		else
			*message = DatumGetJsonbP(DirectFunctionCall1(jsonb_in,
													 CStringGetDatum(payload)));
	}
	PG_CATCH();
	{
		ErrorData *error_data;

		MemoryContextSwitchTo(parse_context);
		error_data = CopyErrorData();

		if (error_data->sqlerrcode == ERRCODE_INVALID_TEXT_REPRESENTATION ||
			error_data->sqlerrcode == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE ||
			error_data->sqlerrcode == ERRCODE_UNTRANSLATABLE_CHARACTER)
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
								   0,
								   0,
								   "SemLoom provider returned invalid JSON");
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (!JB_ROOT_IS_OBJECT(*message))
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   0,
								   0,
								   "SemLoom provider response must be a JSON object");
		return AI_PROVIDER_STATUS_ERROR;
	}
	return AI_PROVIDER_STATUS_OK;
}

bool
semloom_wire_common_json_value(Jsonb *message,
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
								   0,
								   0,
								   "SemLoom provider response is missing a required field");
	return false;
}

bool
semloom_wire_common_json_string_equals(Jsonb *message,
									   const char *key,
									   const char *expected,
									   bool *matches,
									   AiProviderError *error)
{
	JsonbValue *value;
	Size expected_length = strlen(expected);

	if (!semloom_wire_common_json_value(message, key, &value, error))
		return false;
	*matches = value->type == jbvString &&
		value->val.string.len == expected_length &&
		memcmp(value->val.string.val, expected, expected_length) == 0;
	return true;
}

bool
semloom_wire_common_json_int32(Jsonb *message,
							   const char *key,
							   int32 *result,
							   AiProviderError *error)
{
	JsonbValue *value;
	MemoryContext numeric_context = CurrentMemoryContext;
	bool expected_range_error = false;

	if (!semloom_wire_common_json_value(message, key, &value, error))
		return false;
	if (value->type != jbvNumeric)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   0,
								   0,
								   "SemLoom provider response has an invalid integer field");
		return false;
	}
	if (DatumGetInt32(DirectFunctionCall1(numeric_min_scale,
											 NumericGetDatum(value->val.numeric))) != 0)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   0,
								   0,
								   "SemLoom provider response has an invalid integer field");
		return false;
	}
	PG_TRY();
	{
		*result = DatumGetInt32(DirectFunctionCall1(numeric_int4,
											 NumericGetDatum(value->val.numeric)));
	}
	PG_CATCH();
	{
		ErrorData *error_data;

		MemoryContextSwitchTo(numeric_context);
		error_data = CopyErrorData();

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
								   0,
								   0,
								   "integer out of range");
	return false;
}

bool
semloom_wire_common_json_bool(Jsonb *message,
							  const char *key,
							  bool *result,
							  AiProviderError *error)
{
	JsonbValue *value;

	if (!semloom_wire_common_json_value(message, key, &value, error))
		return false;
	if (value->type != jbvBool)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   0,
								   0,
								   "SemLoom provider response has an invalid boolean field");
		return false;
	}
	*result = value->val.boolean;
	return true;
}

static AiProviderStatus
semloom_wire_common_socket_write_all(pgsocket socket_fd,
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
			semloom_wire_common_wait_for_socket(socket_fd, WL_SOCKET_WRITEABLE);
			continue;
		}
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_SYSTEM,
								   errno,
								   0,
								   "could not write to SemLoom provider socket");
		return AI_PROVIDER_STATUS_ERROR;
	}
	return AI_PROVIDER_STATUS_OK;
}

static AiProviderStatus
semloom_wire_common_socket_read_all(pgsocket socket_fd,
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
									   0,
									   0,
									   "SemLoom provider disconnected before completing a frame");
			return AI_PROVIDER_STATUS_ERROR;
		}
		if (errno == EINTR)
			continue;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
		{
			semloom_wire_common_wait_for_socket(socket_fd, WL_SOCKET_READABLE);
			continue;
		}
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_SYSTEM,
								   errno,
								   0,
								   "could not read from SemLoom provider socket");
		return AI_PROVIDER_STATUS_ERROR;
	}
	return AI_PROVIDER_STATUS_OK;
}

static void
semloom_wire_common_wait_for_socket(pgsocket socket_fd, int socket_event)
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
