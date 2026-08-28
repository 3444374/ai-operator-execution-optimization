#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "catalog/pg_type_d.h"
#include "lib/stringinfo.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"

#include "semloom_pg.h"

#define SEMLOOM_IN_PROCESS_PROVIDER_NAME "in-process-recording"
#define SEMLOOM_UDS_PROVIDER_NAME "uds-recording"

struct SemloomProviderSession
{
	SemloomSemanticPlanSpec plan_spec;
	bool closed;
	bool uses_uds;
	bool external_fd_acquired;
	pgsocket socket_fd;
	char plan_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
	uint64 accepted_rows;
	uint64 emitted_rows;
	MemoryContextCallback cleanup_callback;
};

static Datum semloom_record_text(Datum input, MemoryContext result_context);
static void semloom_provider_open_uds(SemloomProviderSession *session,
									 const char *socket_path);
static void semloom_provider_drive_uds(SemloomProviderSession *session,
									  const SemloomPreparedSemanticTask *task,
									  MemoryContext result_context,
									  SemloomCompletionRecord *completion);
static void semloom_provider_cleanup(void *argument);
static void semloom_provider_release_socket(SemloomProviderSession *session);
static Jsonb *semloom_parse_provider_json(const char *payload);
static JsonbValue *semloom_json_value(Jsonb *message, const char *key);
static bool semloom_json_string_equals(Jsonb *message,
									  const char *key,
									  const char *expected);
static int32 semloom_json_int32(Jsonb *message, const char *key);
static bool semloom_json_bool(Jsonb *message, const char *key);
static void semloom_validate_response_type(Jsonb *message,
									  const char *expected_type,
									  uint32 expected_fields);

SemloomProviderSession *
semloom_provider_open(const SemloomSemanticPlanSpec *plan_spec)
{
	SemloomProviderSession *session;
	const char *socket_path;

	if (plan_spec == NULL || plan_spec->mapped_column <= 0 ||
		plan_spec->input_type != TEXTOID || plan_spec->output_type != TEXTOID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid recording provider plan specification")));

	session = palloc0(sizeof(SemloomProviderSession));
	session->plan_spec = *plan_spec;
	session->socket_fd = PGINVALID_SOCKET;
	semloom_protocol_plan_digest(plan_spec, session->plan_digest);
	session->cleanup_callback.func = semloom_provider_cleanup;
	session->cleanup_callback.arg = session;
	MemoryContextRegisterResetCallback(CurrentMemoryContext,
									   &session->cleanup_callback);

	socket_path = semloom_gateway_socket_path();
	if (socket_path[0] != '\0')
	{
		PG_TRY();
		{
			semloom_provider_open_uds(session, socket_path);
		}
		PG_CATCH();
		{
			semloom_provider_release_socket(session);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	return session;
}

void
semloom_provider_drive(SemloomProviderSession *session,
					   const SemloomPreparedSemanticTask *task,
					   MemoryContext result_context,
					   SemloomCompletionRecord *completion)
{
	if (session == NULL || session->closed || task == NULL || completion == NULL ||
		result_context == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recording provider session is not open")));
	if (task->sequence != session->accepted_rows ||
		task->input_type != session->plan_spec.input_type)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("recording provider task does not match the open plan")));

	if (session->uses_uds)
	{
		PG_TRY();
		{
			semloom_provider_drive_uds(session, task, result_context, completion);
		}
		PG_CATCH();
		{
			semloom_provider_release_socket(session);
			session->closed = true;
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	else
	{
		completion->sequence = task->sequence;
		completion->output_type = session->plan_spec.output_type;
		completion->is_null = task->is_null;
		completion->output = task->is_null ? (Datum) 0 :
			semloom_record_text(task->input, result_context);
	}
	session->accepted_rows++;
	session->emitted_rows++;
}

void
semloom_provider_close(SemloomProviderSession *session)
{
	if (session != NULL && !session->closed)
	{
		semloom_provider_release_socket(session);
		session->closed = true;
	}
}

const char *
semloom_provider_name(const SemloomProviderSession *session)
{
	return session != NULL && session->uses_uds ?
		SEMLOOM_UDS_PROVIDER_NAME : SEMLOOM_IN_PROCESS_PROVIDER_NAME;
}

uint64
semloom_provider_accepted_rows(const SemloomProviderSession *session)
{
	return session == NULL ? 0 : session->accepted_rows;
}

uint64
semloom_provider_emitted_rows(const SemloomProviderSession *session)
{
	return session == NULL ? 0 : session->emitted_rows;
}

static void
semloom_provider_open_uds(SemloomProviderSession *session, const char *socket_path)
{
	struct sockaddr_un address;
	StringInfoData request;
	char *response;
	Jsonb *message;
	int socket_flags;

	if (strlen(socket_path) >= sizeof(address.sun_path))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SemLoom provider socket path is too long")));
	if (!AcquireExternalFD())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not reserve a file descriptor for the SemLoom provider")));
	session->external_fd_acquired = true;
	session->socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (session->socket_fd == PGINVALID_SOCKET)
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not create SemLoom provider socket: %m")));

	MemSet(&address, 0, sizeof(address));
	address.sun_family = AF_UNIX;
	strlcpy(address.sun_path, socket_path, sizeof(address.sun_path));
	if (connect(session->socket_fd,
				(struct sockaddr *) &address,
				sizeof(address)) != 0)
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not connect to SemLoom provider socket: %m")));
	socket_flags = fcntl(session->socket_fd, F_GETFL, 0);
	if (socket_flags < 0 ||
		fcntl(session->socket_fd, F_SETFL, socket_flags | O_NONBLOCK) < 0)
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not make SemLoom provider socket nonblocking: %m")));

	initStringInfo(&request);
	appendStringInfo(&request,
					 "{\"type\":\"open\",\"protocol_version\":%d,"
					 "\"plan_digest\":\"%s\",\"mapped_column\":%d,"
					 "\"input_type\":\"text\",\"output_type\":\"text\"}",
					 SEMLOOM_PROTOCOL_VERSION,
					 session->plan_digest,
					 session->plan_spec.mapped_column);
	semloom_protocol_send_frame(session->socket_fd, request.data, request.len);
	pfree(request.data);
	response = semloom_protocol_receive_frame(session->socket_fd);
	message = semloom_parse_provider_json(response);
	semloom_validate_response_type(message, "opened", 5);
	if (semloom_json_int32(message, "protocol_version") != SEMLOOM_PROTOCOL_VERSION ||
		!semloom_json_string_equals(message, "plan_digest", session->plan_digest) ||
		semloom_json_int32(message, "max_inflight_tasks") != 1 ||
		semloom_json_int32(message, "max_frame_bytes") != SEMLOOM_MAX_FRAME_BYTES)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("SemLoom provider open response does not match the requested protocol")));
	session->uses_uds = true;
}

static void
semloom_provider_drive_uds(SemloomProviderSession *session,
							  const SemloomPreparedSemanticTask *task,
							  MemoryContext result_context,
							  SemloomCompletionRecord *completion)
{
	text *input_text = NULL;
	const char *input_data = NULL;
	Size input_length = 0;
	char payload_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
	char expected_evidence_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
	char sequence[32];
	StringInfoData request;
	char *response;
	Jsonb *message;
	JsonbValue *output_value;
	bool output_is_null;
	MemoryContext previous_context;

	if (!task->is_null)
	{
		input_text = DatumGetTextPP(task->input);
		input_data = VARDATA_ANY(input_text);
		input_length = VARSIZE_ANY_EXHDR(input_text);
	}
	semloom_protocol_payload_digest(task->is_null,
								input_data,
								input_length,
								payload_digest);
	pg_snprintf(sequence, sizeof(sequence), UINT64_FORMAT, task->sequence);
	initStringInfo(&request);
	appendStringInfo(&request,
					 "{\"type\":\"task\",\"protocol_version\":%d,"
					 "\"sequence\":\"%s\",\"plan_digest\":\"%s\","
					 "\"payload_digest\":\"%s\",\"is_null\":%s,\"input\":",
					 SEMLOOM_PROTOCOL_VERSION,
					 sequence,
					 session->plan_digest,
					 payload_digest,
					 task->is_null ? "true" : "false");
	if (task->is_null)
		appendStringInfoString(&request, "null");
	else
		escape_json_with_len(&request, input_data, input_length);
	appendStringInfoChar(&request, '}');
	semloom_protocol_send_frame(session->socket_fd, request.data, request.len);
	pfree(request.data);

	response = semloom_protocol_receive_frame(session->socket_fd);
	message = semloom_parse_provider_json(response);
	semloom_validate_response_type(message, "completion", 8);
	if (semloom_json_int32(message, "protocol_version") != SEMLOOM_PROTOCOL_VERSION ||
		!semloom_json_string_equals(message, "sequence", sequence) ||
		!semloom_json_string_equals(message, "plan_digest", session->plan_digest) ||
		!semloom_json_string_equals(message, "payload_digest", payload_digest))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("SemLoom provider completion identity does not match the task")));

	output_is_null = semloom_json_bool(message, "is_null");
	output_value = semloom_json_value(message, "output");
	if ((output_is_null && output_value->type != jbvNull) ||
		(!output_is_null && output_value->type != jbvString))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("SemLoom provider completion has an invalid output")));
	semloom_protocol_completion_digest(session->plan_digest,
									payload_digest,
									task->sequence,
									output_is_null,
									output_is_null ? NULL : output_value->val.string.val,
									output_is_null ? 0 : output_value->val.string.len,
									expected_evidence_digest);
	if (!semloom_json_string_equals(message,
								"evidence_digest",
								expected_evidence_digest))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("SemLoom provider completion evidence digest does not match")));

	completion->sequence = task->sequence;
	completion->output_type = session->plan_spec.output_type;
	completion->is_null = output_is_null;
	if (output_is_null)
		completion->output = (Datum) 0;
	else
	{
		text *output_text;

		previous_context = MemoryContextSwitchTo(result_context);
		output_text = cstring_to_text_with_len(output_value->val.string.val,
										output_value->val.string.len);
		MemoryContextSwitchTo(previous_context);
		completion->output = PointerGetDatum(output_text);
	}
}

static void
semloom_provider_cleanup(void *argument)
{
	SemloomProviderSession *session = argument;

	semloom_provider_release_socket(session);
}

static void
semloom_provider_release_socket(SemloomProviderSession *session)
{
	if (session->socket_fd != PGINVALID_SOCKET)
	{
		closesocket(session->socket_fd);
		session->socket_fd = PGINVALID_SOCKET;
	}
	if (session->external_fd_acquired)
	{
		ReleaseExternalFD();
		session->external_fd_acquired = false;
	}
}

static Jsonb *
semloom_parse_provider_json(const char *payload)
{
	Jsonb *message = NULL;

	PG_TRY();
	{
		message = DatumGetJsonbP(DirectFunctionCall1(jsonb_in,
												 CStringGetDatum(payload)));
	}
	PG_CATCH();
	{
		FlushErrorState();
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("SemLoom provider returned invalid JSON")));
	}
	PG_END_TRY();
	if (!JB_ROOT_IS_OBJECT(message))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("SemLoom provider response must be a JSON object")));
	return message;
}

static JsonbValue *
semloom_json_value(Jsonb *message, const char *key)
{
	JsonbValue *value;

	value = getKeyJsonValueFromContainer(&message->root,
									 key,
									 strlen(key),
									 NULL);
	if (value == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("SemLoom provider response is missing a required field")));
	return value;
}

static bool
semloom_json_string_equals(Jsonb *message, const char *key, const char *expected)
{
	JsonbValue *value = semloom_json_value(message, key);
	Size expected_length = strlen(expected);

	return value->type == jbvString &&
		value->val.string.len == expected_length &&
		memcmp(value->val.string.val, expected, expected_length) == 0;
}

static int32
semloom_json_int32(Jsonb *message, const char *key)
{
	JsonbValue *value = semloom_json_value(message, key);

	if (value->type != jbvNumeric)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("SemLoom provider response has an invalid integer field")));
	return DatumGetInt32(DirectFunctionCall1(numeric_int4,
										NumericGetDatum(value->val.numeric)));
}

static bool
semloom_json_bool(Jsonb *message, const char *key)
{
	JsonbValue *value = semloom_json_value(message, key);

	if (value->type != jbvBool)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("SemLoom provider response has an invalid boolean field")));
	return value->val.boolean;
}

static void
semloom_validate_response_type(Jsonb *message,
								  const char *expected_type,
								  uint32 expected_fields)
{
	if (semloom_json_string_equals(message, "type", "error"))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("SemLoom provider rejected the protocol message")));
	if (JsonContainerSize(&message->root) != expected_fields ||
		!semloom_json_string_equals(message, "type", expected_type))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("SemLoom provider returned an unexpected message")));
}

static Datum
semloom_record_text(Datum input, MemoryContext result_context)
{
	text *input_text = DatumGetTextPP(input);
	Size prefix_length = strlen(SEMLOOM_RECORDING_PREFIX);
	Size input_length = VARSIZE_ANY_EXHDR(input_text);
	MemoryContext previous_context;
	text *output_text;

	previous_context = MemoryContextSwitchTo(result_context);
	output_text = (text *) palloc(VARHDRSZ + prefix_length + input_length);
	SET_VARSIZE(output_text, VARHDRSZ + prefix_length + input_length);
	memcpy(VARDATA(output_text), SEMLOOM_RECORDING_PREFIX, prefix_length);
	memcpy(VARDATA(output_text) + prefix_length, VARDATA_ANY(input_text), input_length);
	MemoryContextSwitchTo(previous_context);

	return PointerGetDatum(output_text);
}
