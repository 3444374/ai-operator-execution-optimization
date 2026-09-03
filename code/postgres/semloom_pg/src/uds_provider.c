/*
 * Query-scoped Unix-domain-socket AiProvider adapter.
 *
 * It snapshots an opaque path, lazily connects on the first drive, copies wire
 * output into session storage, and owns idempotent local FD accounting/cleanup.
 * Plan: experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md.
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/memutils.h"

#include "provider_private.h"
#include "wire_common.h"
#include "wire_v2.h"
#include "wire_v3.h"
#include "wire_v4.h"

typedef struct SemloomUdsProviderConfig
{
	char *socket_path;
	const char *semantic_execution_id;
	uint32 protocol_version;
} SemloomUdsProviderConfig;

struct AiProviderSession
{
	bool closed;
	bool external_fd_acquired;
	pgsocket socket_fd;
	const SemloomUdsProviderConfig *config;
	AiOpenSpec open_spec;
	SemloomWireV2Identity identity;
	SemloomWireSemanticIdentity semantic_identity;
	MemoryContext scratch_context;
	MemoryContext completion_context;
};

static AiProviderStatus semloom_uds_open(const void *config,
									  const AiOpenSpec *spec,
									  AiProviderSession **session_out,
									  AiProviderError *error);
static AiProviderStatus semloom_uds_drive(AiProviderSession *session,
									   const AiPreparedTask *task,
									   AiCompletion *completion,
									   AiProviderError *error);
static AiProviderStatus semloom_uds_drive_internal(AiProviderSession *session,
													const AiPreparedTask *task,
													AiCompletion *completion,
													AiProviderError *error);
static AiProviderStatus semloom_uds_connect(AiProviderSession *session,
											 AiProviderError *error);
static void semloom_uds_close(AiProviderSession *session);
static void semloom_uds_release_local(AiProviderSession *session);
static AiByteSlice semloom_uds_copy_slice(AiByteSlice source);

static const AiProviderOps semloom_uds_recording_ops = {
	.adapter_name = SEMLOOM_UDS_RECORDING_PROVIDER_NAME,
	.open = semloom_uds_open,
	.drive = semloom_uds_drive,
	.close = semloom_uds_close,
};

static const AiProviderOps semloom_uds_golden_ops = {
	.adapter_name = SEMLOOM_UDS_GOLDEN_PROVIDER_NAME,
	.open = semloom_uds_open,
	.drive = semloom_uds_drive,
	.close = semloom_uds_close,
};

static const AiProviderOps semloom_uds_fixed_ops = {
	.adapter_name = SEMLOOM_UDS_FIXED_PROVIDER_NAME,
	.open = semloom_uds_open,
	.drive = semloom_uds_drive,
	.close = semloom_uds_close,
};

void
semloom_uds_provider_select(MemoryContext owner_context,
									const char *socket_path,
									const AiOpenSpec *spec,
									SemloomProviderExecutionProfile profile,
									AiProvider *provider)
{
	SemloomUdsProviderConfig *config;
	Size path_length;

	Assert(owner_context != NULL);
	Assert(socket_path != NULL);
	Assert(spec != NULL);
	Assert(provider != NULL);
	path_length = strlen(socket_path);
	config = MemoryContextAllocZero(owner_context, sizeof(*config));
	config->socket_path = MemoryContextAlloc(owner_context, path_length + 1);
	memcpy(config->socket_path, socket_path, path_length + 1);
	config->protocol_version = semloom_provider_spec_is_recording(spec) ? 2 :
		(semloom_provider_spec_is_generate_map(spec) ? 5 : (spec->has_generation_profile ? 4 : 3));
	if (semloom_provider_spec_is_recording(spec))
	{
		provider->ops = &semloom_uds_recording_ops;
		config->semantic_execution_id = NULL;
	}
	else if (profile == SEMLOOM_PROVIDER_PROFILE_GOLDEN)
	{
		provider->ops = &semloom_uds_golden_ops;
		config->semantic_execution_id = config->protocol_version == 5 ? SEMLOOM_UDS_MAP_GOLDEN_EXECUTION_ID :
			(spec->has_generation_profile ? SEMLOOM_UDS_CHOICE_GOLDEN_EXECUTION_ID : SEMLOOM_UDS_GOLDEN_EXECUTION_ID);
	}
	else if (profile == SEMLOOM_PROVIDER_PROFILE_OPENAI_COMPATIBLE_FIXED)
	{
		provider->ops = &semloom_uds_fixed_ops;
		config->semantic_execution_id = config->protocol_version == 5 ? SEMLOOM_UDS_MAP_FIXED_EXECUTION_ID :
			(spec->has_generation_profile ? SEMLOOM_UDS_CHOICE_FIXED_EXECUTION_ID : SEMLOOM_UDS_FIXED_EXECUTION_ID);
	}
	else
		elog(ERROR, "unrecognized SemLoom provider execution profile: %d", profile);
	provider->config = config;
	provider->max_input_bytes = config->protocol_version != 2 ?
		SEMLOOM_WIRE_V3_MAX_INPUT_BYTES : SEMLOOM_WIRE_V2_MAX_INPUT_BYTES;
}

static AiProviderStatus
semloom_uds_open(const void *config_value,
				 const AiOpenSpec *spec,
				 AiProviderSession **session_out,
				 AiProviderError *error)
{
	const SemloomUdsProviderConfig *config = config_value;
	AiProviderSession *session;

	if (session_out != NULL)
		*session_out = NULL;
	if (config == NULL || session_out == NULL || error == NULL ||
		(!semloom_provider_spec_is_recording(spec) &&
		 !semloom_provider_spec_is_exact_filter(spec) &&
		 !semloom_provider_spec_is_generate_map(spec)))
	{
		if (error != NULL)
				semloom_provider_error_set(error,
									   AI_PROVIDER_ERROR_INVALID_SPEC,
									   0,
									   0,
									   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}

	session = palloc0(sizeof(*session));
	session->socket_fd = PGINVALID_SOCKET;
	session->config = config;
	*session_out = session;
	session->open_spec = *spec;
	session->open_spec.semantic_spec_id = semloom_uds_copy_slice(spec->semantic_spec_id);
	session->open_spec.physical_algorithm =
		semloom_uds_copy_slice(spec->physical_algorithm);
	session->open_spec.physical_role = semloom_uds_copy_slice(spec->physical_role);
	session->open_spec.prompt_program_digest =
		semloom_uds_copy_slice(spec->prompt_program_digest);
	session->open_spec.result_parser_digest =
		semloom_uds_copy_slice(spec->result_parser_digest);
	session->open_spec.model_id = semloom_uds_copy_slice(spec->model_id);
	session->open_spec.semantic_spec_digest =
		semloom_uds_copy_slice(spec->semantic_spec_digest);
	session->open_spec.physical_algorithm_digest =
		semloom_uds_copy_slice(spec->physical_algorithm_digest);
	session->open_spec.stop = semloom_uds_copy_slice(spec->stop);
	if (spec->has_generation_profile)
	{
		uint32 index;
		AiGenerationProfile *profile = &session->open_spec.generation_profile;

		profile->profile_id = semloom_uds_copy_slice(spec->generation_profile.profile_id);
		for (index = 0; index < profile->choice_count; index++)
			profile->choices[index] = semloom_uds_copy_slice(spec->generation_profile.choices[index]);
	}
	session->scratch_context = AllocSetContextCreate(CurrentMemoryContext,
												 "SemLoom UDS drive scratch",
												 ALLOCSET_DEFAULT_SIZES);
	session->completion_context = AllocSetContextCreate(CurrentMemoryContext,
													  "SemLoom UDS completion",
													  ALLOCSET_DEFAULT_SIZES);
	if (config->protocol_version != 2)
		semloom_wire_semantic_identity_init(&session->open_spec,
			config->semantic_execution_id, config->protocol_version, &session->semantic_identity);
	else
		semloom_wire_v2_identity_init(&session->open_spec,
										 SEMLOOM_UDS_RECORDING_EXECUTION_ID,
										 &session->identity);
	return AI_PROVIDER_STATUS_OK;
}

static AiProviderStatus
semloom_uds_drive(AiProviderSession *session,
				  const AiPreparedTask *task,
				  AiCompletion *completion,
				  AiProviderError *error)
{
	MemoryContext previous_context;
	AiCompletion scratch_completion = {0};
	AiProviderStatus status = AI_PROVIDER_STATUS_ERROR;

	if (session == NULL || session->closed || task == NULL || completion == NULL ||
		error == NULL)
	{
		if (error != NULL)
				semloom_provider_error_set(error,
									   AI_PROVIDER_ERROR_SESSION_CLOSED,
									   0,
									   0,
									   NULL);
		semloom_uds_close(session);
		return AI_PROVIDER_STATUS_ERROR;
	}

	MemoryContextReset(session->completion_context);
	MemoryContextReset(session->scratch_context);
	previous_context = MemoryContextSwitchTo(session->scratch_context);
	PG_TRY();
	{
		status = semloom_uds_drive_internal(session,
											 task,
											 &scratch_completion,
											 error);
		if (status == AI_PROVIDER_STATUS_OK)
		{
			uint8 *output = NULL;
			uint8 *response_model = NULL;
			uint8 *finish_reason = NULL;

			MemoryContextSwitchTo(session->completion_context);
			if (!scratch_completion.is_null)
			{
				Size allocation_length = scratch_completion.output.length > 0 ?
					scratch_completion.output.length : 1;

				output = palloc(allocation_length);
				if (scratch_completion.output.length > 0)
					memcpy(output,
						   scratch_completion.output.data,
						   scratch_completion.output.length);
			}
			if (scratch_completion.response_model_id.length > 0)
			{
				response_model = palloc(scratch_completion.response_model_id.length);
				memcpy(response_model,
					   scratch_completion.response_model_id.data,
					   scratch_completion.response_model_id.length);
			}
			if (scratch_completion.finish_reason.length > 0)
			{
				finish_reason = palloc(scratch_completion.finish_reason.length);
				memcpy(finish_reason,
					   scratch_completion.finish_reason.data,
					   scratch_completion.finish_reason.length);
			}
			MemoryContextSwitchTo(session->scratch_context);
			completion->sequence = scratch_completion.sequence;
			completion->is_null = scratch_completion.is_null;
			completion->output.data = output;
			completion->output.length = scratch_completion.output.length;
			completion->response_model_id.data = response_model;
			completion->response_model_id.length =
				scratch_completion.response_model_id.length;
			completion->finish_reason.data = finish_reason;
			completion->finish_reason.length = scratch_completion.finish_reason.length;
			completion->prompt_tokens = scratch_completion.prompt_tokens;
			completion->output_tokens = scratch_completion.output_tokens;
		}
		MemoryContextSwitchTo(previous_context);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(previous_context);
		MemoryContextReset(session->scratch_context);
		semloom_uds_close(session);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextReset(session->scratch_context);
	if (status != AI_PROVIDER_STATUS_OK)
		semloom_uds_close(session);
	return status;
}

static AiProviderStatus
semloom_uds_drive_internal(AiProviderSession *session,
						   const AiPreparedTask *task,
						   AiCompletion *completion,
						   AiProviderError *error)
{
	AiProviderStatus status;

	if (task->is_null)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_NULL_TASK,
								   0,
								   0,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (task->input.length > 0 && task->input.data == NULL)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_TASK_MISMATCH,
								   0,
								   0,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	if ((session->config->protocol_version == 2 &&
		 task->input.length > SEMLOOM_WIRE_V2_MAX_INPUT_BYTES) ||
		(session->config->protocol_version != 2 &&
		 task->input.length > (session->config->protocol_version == 5 ?
			session->open_spec.max_input_bytes : SEMLOOM_WIRE_V3_MAX_INPUT_BYTES)))
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_INPUT_TOO_LARGE,
								   0,
								   session->config->protocol_version != 2 ?
									   SEMLOOM_WIRE_V3_MAX_INPUT_BYTES :
									   SEMLOOM_WIRE_V2_MAX_INPUT_BYTES,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (GetDatabaseEncoding() != PG_UTF8)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_UNSUPPORTED_ENCODING,
								   0,
								   0,
								   session->config->protocol_version != 2 ?
									   "SemLoom exact semantic provider requires UTF8 database encoding" :
									   "SemLoom UDS recording provider requires UTF8 database encoding");
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (session->socket_fd == PGINVALID_SOCKET)
	{
		status = semloom_uds_connect(session, error);
		if (status != AI_PROVIDER_STATUS_OK)
			return status;
	}
	if (session->config->protocol_version == 5)
		return semloom_wire_semantic_drive(session->socket_fd, &session->open_spec,
			task, &session->semantic_identity, completion, error);
	if (session->config->protocol_version == 4)
		return semloom_wire_v4_drive(session->socket_fd, &session->open_spec,
			task, &session->semantic_identity, completion, error);
	if (session->config->protocol_version == 3)
		return semloom_wire_v3_drive(session->socket_fd,
									 &session->open_spec,
									 task,
									 &session->semantic_identity,
									 completion,
									 error);
	return semloom_wire_v2_drive(session->socket_fd,
								 task,
								 &session->identity,
								 completion,
								 error);
}

static AiProviderStatus
semloom_uds_connect(AiProviderSession *session, AiProviderError *error)
{
	const char *socket_path = session->config->socket_path;
	struct sockaddr_un address;
	int socket_flags;
	int connect_result;

	if (strlen(socket_path) >= sizeof(address.sun_path))
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_INVALID_SPEC,
								   0,
								   0,
								   "SemLoom provider socket path is too long");
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (socket_path[0] != '/')
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_INVALID_SPEC,
								   0,
								   0,
								   "SemLoom provider socket path must be absolute");
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (!AcquireExternalFD())
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_RESOURCE_EXHAUSTED,
								   0,
								   0,
								   "could not reserve a file descriptor for the SemLoom provider");
		return AI_PROVIDER_STATUS_ERROR;
	}
	session->external_fd_acquired = true;
	session->socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (session->socket_fd == PGINVALID_SOCKET)
	{
		int saved_errno = errno;

		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_SYSTEM,
								   saved_errno,
								   0,
								   "could not create SemLoom provider socket");
		return AI_PROVIDER_STATUS_ERROR;
	}

	socket_flags = fcntl(session->socket_fd, F_GETFL, 0);
	if (socket_flags < 0 ||
		fcntl(session->socket_fd, F_SETFL, socket_flags | O_NONBLOCK) < 0)
	{
		int saved_errno = errno;

		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_SYSTEM,
								   saved_errno,
								   0,
								   "could not make SemLoom provider socket nonblocking");
		return AI_PROVIDER_STATUS_ERROR;
	}

	MemSet(&address, 0, sizeof(address));
	address.sun_family = AF_UNIX;
	strlcpy(address.sun_path, socket_path, sizeof(address.sun_path));
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		connect_result = connect(session->socket_fd,
								 (struct sockaddr *) &address,
								 sizeof(address));
		if (connect_result == 0 || errno == EISCONN)
			break;
		if (errno == EINTR)
			continue;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
		{
			semloom_wire_common_wait_connect_retry();
			continue;
		}
		if (errno == EINPROGRESS || errno == EALREADY)
		{
			AiProviderStatus status =
				semloom_wire_common_wait_connected(session->socket_fd, error);

			if (status != AI_PROVIDER_STATUS_OK)
				return status;
			break;
		}
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_SYSTEM,
								   errno,
								   0,
								   "could not connect to SemLoom provider socket");
		return AI_PROVIDER_STATUS_ERROR;
	}

	if (session->config->protocol_version == 5)
		return semloom_wire_semantic_open(session->socket_fd, &session->open_spec,
			&session->semantic_identity, error);
	if (session->config->protocol_version == 4)
		return semloom_wire_v4_open(session->socket_fd, &session->open_spec,
			&session->semantic_identity, error);
	if (session->config->protocol_version == 3)
		return semloom_wire_v3_open(session->socket_fd,
								   &session->open_spec,
								   &session->semantic_identity,
								   error);
	return semloom_wire_v2_open(session->socket_fd,
								&session->open_spec,
								&session->identity,
								error);
}

static void
semloom_uds_close(AiProviderSession *session)
{
	if (session == NULL || session->closed)
		return;
	session->closed = true;
	session->scratch_context = NULL;
	session->completion_context = NULL;
	semloom_uds_release_local(session);
}

static void
semloom_uds_release_local(AiProviderSession *session)
{
	pgsocket socket_fd = session->socket_fd;
	bool external_fd_acquired = session->external_fd_acquired;

	session->socket_fd = PGINVALID_SOCKET;
	session->external_fd_acquired = false;
	if (socket_fd != PGINVALID_SOCKET)
		closesocket(socket_fd);
	if (external_fd_acquired)
		ReleaseExternalFD();
}

static AiByteSlice
semloom_uds_copy_slice(AiByteSlice source)
{
	AiByteSlice copy = {0};
	uint8 *data;

	if (source.length == 0)
		return copy;
	data = palloc(source.length);
	memcpy(data, source.data, source.length);
	copy.data = data;
	copy.length = source.length;
	return copy;
}
