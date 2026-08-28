#include "postgres.h"

#include "utils/memutils.h"

#include "provider_private.h"

#define SEMLOOM_RECORDING_PREFIX "recorded:"

struct AiProviderSession
{
	bool closed;
	MemoryContext completion_context;
};

static AiProviderStatus semloom_recording_open(const void *config,
											const AiOpenSpec *spec,
											AiProviderSession **session_out,
											AiProviderError *error);
static AiProviderStatus semloom_recording_drive(AiProviderSession *session,
											 const AiPreparedTask *task,
											 AiCompletion *completion,
											 AiProviderError *error);
static void semloom_recording_close(AiProviderSession *session);

static const AiProviderOps semloom_recording_ops = {
	.adapter_name = SEMLOOM_IN_PROCESS_PROVIDER_NAME,
	.open = semloom_recording_open,
	.drive = semloom_recording_drive,
	.close = semloom_recording_close,
};

void
semloom_recording_provider_select(AiProvider *provider)
{
	Assert(provider != NULL);
	provider->ops = &semloom_recording_ops;
	provider->config = NULL;
}

static AiProviderStatus
semloom_recording_open(const void *config,
					   const AiOpenSpec *spec,
					   AiProviderSession **session_out,
					   AiProviderError *error)
{
	AiProviderSession *session;

	(void) config;
	if (session_out == NULL || error == NULL ||
		!semloom_provider_spec_is_recording(spec))
	{
		if (error != NULL)
			semloom_provider_error_set(error,
									   AI_PROVIDER_ERROR_INVALID_SPEC,
									   AI_PROVIDER_OPERATION_OPEN_SPEC,
									   0,
									   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}

	session = palloc0(sizeof(*session));
	*session_out = session;
	session->completion_context = AllocSetContextCreate(CurrentMemoryContext,
													  "SemLoom recording completion",
													  ALLOCSET_DEFAULT_SIZES);
	return AI_PROVIDER_STATUS_OK;
}

static AiProviderStatus
semloom_recording_drive(AiProviderSession *session,
						const AiPreparedTask *task,
						AiCompletion *completion,
						AiProviderError *error)
{
	static const char prefix[] = SEMLOOM_RECORDING_PREFIX;
	MemoryContext previous_context;
	uint8 *output;
	Size output_length;

	if (session == NULL || session->closed || task == NULL || completion == NULL ||
		error == NULL)
	{
		if (error != NULL)
			semloom_provider_error_set(error,
									   AI_PROVIDER_ERROR_SESSION_CLOSED,
									   AI_PROVIDER_OPERATION_NONE,
									   0,
									   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (task->is_null)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_NULL_TASK,
								   AI_PROVIDER_OPERATION_NONE,
								   0,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (task->input.length > 0 && task->input.data == NULL)
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_TASK_MISMATCH,
								   AI_PROVIDER_OPERATION_NONE,
								   0,
								   NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}

	MemoryContextReset(session->completion_context);
	previous_context = MemoryContextSwitchTo(session->completion_context);
	output_length = sizeof(prefix) - 1 + task->input.length;
	output = palloc(output_length);
	memcpy(output, prefix, sizeof(prefix) - 1);
	if (task->input.length > 0)
		memcpy(output + sizeof(prefix) - 1, task->input.data, task->input.length);
	MemoryContextSwitchTo(previous_context);

	completion->sequence = task->sequence;
	completion->is_null = false;
	completion->output.data = output;
	completion->output.length = (uint32) output_length;
	return AI_PROVIDER_STATUS_OK;
}

static void
semloom_recording_close(AiProviderSession *session)
{
	MemoryContext completion_context;

	if (session == NULL || session->closed)
		return;
	session->closed = true;
	completion_context = session->completion_context;
	session->completion_context = NULL;
	if (completion_context != NULL)
		MemoryContextReset(completion_context);
}
