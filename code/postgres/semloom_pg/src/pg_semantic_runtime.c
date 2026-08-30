/* PostgreSQL-private provider lifecycle shared by semantic operators. */
#include "postgres.h"

#include <errno.h>

#include "commands/explain.h"
#include "commands/explain_format.h"
#include "utils/memutils.h"

#include "pg_semantic_runtime.h"
#include "provider_private.h"

typedef enum PgSemanticRuntimeState
{
	PG_SEMANTIC_RUNTIME_SELECTED_NOT_OPEN = 1,
	PG_SEMANTIC_RUNTIME_OPEN = 2,
	PG_SEMANTIC_RUNTIME_READY = 3,
	PG_SEMANTIC_RUNTIME_TERMINAL = 4,
	PG_SEMANTIC_RUNTIME_CLOSED = 5,
} PgSemanticRuntimeState;

struct PgSemanticRuntime
{
	SemloomPlanSpec plan_spec;
	AiOpenSpec open_spec;
	AiProvider provider;
	AiProviderSession *provider_session;
	MemoryContext owner_context;
	MemoryContextCallback cleanup_callback;
	PgSemanticRuntimeState state;
	uint64 next_sequence;
	uint64 accepted_rows;
	uint64 emitted_rows;
};

static AiByteSlice pg_semantic_runtime_copy_slice(MemoryContext owner_context,
											  AiByteSlice source);
static void pg_semantic_runtime_build_open_spec(
	const SemloomPlanSpec *plan_spec,
	AiOpenSpec *open_spec);
static void pg_semantic_runtime_cleanup(void *argument);
static void pg_semantic_runtime_release_session(PgSemanticRuntime *runtime);
static void pg_semantic_runtime_open_provider(PgSemanticRuntime *runtime);
static void pg_semantic_runtime_copy_completion(
	const AiCompletion *provider_completion,
	MemoryContext result_context,
	PgSemanticCompletion *completion);
pg_noreturn static void pg_semantic_runtime_fail(
	PgSemanticRuntime *runtime,
	const AiProviderError *error);
pg_noreturn static void semloom_raise_provider_error(
	const AiProviderError *error);
static const char *semloom_provider_error_detail(const AiProviderError *error);

PgSemanticRuntime *
pg_semantic_runtime_begin(MemoryContext owner_context,
						  const SemloomPlanSpec *plan_spec)
{
	PgSemanticRuntime *runtime;
	AiOpenSpec open_spec;

	Assert(owner_context != NULL);
	Assert(plan_spec != NULL);
	pg_semantic_runtime_build_open_spec(plan_spec, &open_spec);
	runtime = MemoryContextAllocZero(owner_context, sizeof(*runtime));
	runtime->owner_context = owner_context;
	runtime->plan_spec = *plan_spec;
	runtime->open_spec = open_spec;
	runtime->open_spec.semantic_spec_id = pg_semantic_runtime_copy_slice(
		owner_context,
		open_spec.semantic_spec_id);
	runtime->open_spec.physical_algorithm = pg_semantic_runtime_copy_slice(
		owner_context,
		open_spec.physical_algorithm);
	runtime->state = PG_SEMANTIC_RUNTIME_SELECTED_NOT_OPEN;
	runtime->cleanup_callback.func = pg_semantic_runtime_cleanup;
	runtime->cleanup_callback.arg = runtime;
	MemoryContextRegisterResetCallback(owner_context, &runtime->cleanup_callback);
	semloom_provider_select(owner_context, &runtime->provider);
	return runtime;
}

void
pg_semantic_runtime_drive(PgSemanticRuntime *runtime,
						  AiByteSlice input,
						  MemoryContext result_context,
						  PgSemanticCompletion *completion)
{
	AiPreparedTask task = {0};
	AiCompletion provider_completion = {0};
	AiProviderError error;
	AiProviderStatus status;

	Assert(runtime != NULL);
	Assert(result_context != NULL);
	Assert(completion != NULL);
	if (runtime->state == PG_SEMANTIC_RUNTIME_TERMINAL ||
		runtime->state == PG_SEMANTIC_RUNTIME_CLOSED)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recording provider session is not open")));
	if (runtime->provider_session == NULL)
		pg_semantic_runtime_open_provider(runtime);

	task.sequence = runtime->next_sequence;
	task.input = input;
	task.is_null = false;
	semloom_provider_error_clear(&error);
	status = runtime->provider.ops->drive(runtime->provider_session,
										  &task,
										  &provider_completion,
										  &error);
	if (status != AI_PROVIDER_STATUS_OK)
		pg_semantic_runtime_fail(runtime, &error);
	if (provider_completion.sequence != task.sequence ||
		(!provider_completion.is_null &&
		 provider_completion.output.length > 0 &&
		 provider_completion.output.data == NULL))
	{
		semloom_provider_error_set(&error,
								   AI_PROVIDER_ERROR_TASK_MISMATCH,
								   0,
								   0,
								   NULL);
		pg_semantic_runtime_fail(runtime, &error);
	}

	pg_semantic_runtime_copy_completion(&provider_completion,
										result_context,
										completion);
	runtime->next_sequence++;
	runtime->accepted_rows++;
	runtime->state = PG_SEMANTIC_RUNTIME_READY;
}

void
pg_semantic_runtime_record_emitted(PgSemanticRuntime *runtime)
{
	Assert(runtime != NULL);
	Assert(runtime->state == PG_SEMANTIC_RUNTIME_READY);
	runtime->emitted_rows++;
}

void
pg_semantic_runtime_close(PgSemanticRuntime *runtime)
{
	if (runtime == NULL)
		return;
	runtime->state = PG_SEMANTIC_RUNTIME_CLOSED;
	pg_semantic_runtime_release_session(runtime);
}

void
pg_semantic_runtime_explain(const PgSemanticRuntime *runtime,
							ExplainState *explain_state)
{
	Assert(runtime != NULL);
	Assert(runtime->provider.ops != NULL);
	ExplainPropertyText("Provider",
						runtime->provider.ops->adapter_name,
						explain_state);
	ExplainPropertyText("Physical Role",
						runtime->plan_spec.physical_role,
						explain_state);
}

void
pg_semantic_runtime_explain_counters(const PgSemanticRuntime *runtime,
									 ExplainState *explain_state)
{
	Assert(runtime != NULL);
	if (explain_state->analyze)
	{
		ExplainPropertyInteger("Accepted Rows", NULL,
							   runtime->accepted_rows,
							   explain_state);
		ExplainPropertyInteger("Emitted Rows", NULL,
							   runtime->emitted_rows,
							   explain_state);
	}
}

static AiByteSlice
pg_semantic_runtime_copy_slice(MemoryContext owner_context, AiByteSlice source)
{
	AiByteSlice copied = {
		.data = NULL,
		.length = source.length,
	};
	uint8 *data;

	if (source.length == 0 || source.data == NULL)
		return copied;
	data = MemoryContextAlloc(owner_context, source.length);
	memcpy(data, source.data, source.length);
	copied.data = data;
	return copied;
}

static void
pg_semantic_runtime_build_open_spec(const SemloomPlanSpec *plan_spec,
									AiOpenSpec *open_spec)
{
	MemSet(open_spec, 0, sizeof(*open_spec));
	switch (plan_spec->operator_kind)
	{
		case SEMLOOM_PLAN_OPERATOR_MAP:
			open_spec->operator_kind = AI_PROVIDER_OPERATOR_MAP;
			break;
		case SEMLOOM_PLAN_OPERATOR_FILTER:
			open_spec->operator_kind = AI_PROVIDER_OPERATOR_FILTER;
			break;
		default:
			elog(ERROR, "unsupported semantic plan operator kind");
	}
	switch (plan_spec->input_value_kind)
	{
		case SEMLOOM_PLAN_VALUE_TEXT:
			open_spec->input_value_kind = AI_PROVIDER_VALUE_TEXT;
			break;
		default:
			elog(ERROR, "unsupported semantic plan input value kind");
	}
	switch (plan_spec->output_value_kind)
	{
		case SEMLOOM_PLAN_VALUE_TEXT:
			open_spec->output_value_kind = AI_PROVIDER_VALUE_TEXT;
			break;
		case SEMLOOM_PLAN_VALUE_TRISTATE:
			open_spec->output_value_kind = AI_PROVIDER_VALUE_TRISTATE;
			break;
		default:
			elog(ERROR, "unsupported semantic plan output value kind");
	}
	if (plan_spec->null_policy != SEMLOOM_PLAN_NULL_PROPAGATE ||
		plan_spec->error_policy != SEMLOOM_PLAN_ERROR_FAIL_QUERY)
		elog(ERROR, "unsupported semantic plan execution policy");
	open_spec->null_policy = AI_PROVIDER_NULL_PROPAGATE;
	open_spec->error_policy = AI_PROVIDER_ERROR_FAIL_QUERY;
	open_spec->semantic_spec_version = plan_spec->semantic_spec_version;
	open_spec->semantic_spec_id.data = (const uint8 *) plan_spec->semantic_spec_id;
	open_spec->semantic_spec_id.length = plan_spec->semantic_spec_id_length;
	open_spec->physical_algorithm.data =
		(const uint8 *) plan_spec->physical_algorithm;
	open_spec->physical_algorithm.length = plan_spec->physical_algorithm_length;
}

static void
pg_semantic_runtime_cleanup(void *argument)
{
	pg_semantic_runtime_close((PgSemanticRuntime *) argument);
}

static void
pg_semantic_runtime_release_session(PgSemanticRuntime *runtime)
{
	AiProviderSession *session;

	if (runtime == NULL)
		return;
	session = runtime->provider_session;
	runtime->provider_session = NULL;
	if (session != NULL && runtime->provider.ops != NULL)
		runtime->provider.ops->close(session);
}

static void
pg_semantic_runtime_open_provider(PgSemanticRuntime *runtime)
{
	MemoryContext previous_context;
	AiProviderError error;
	AiProviderStatus status = AI_PROVIDER_STATUS_ERROR;

	Assert(runtime->state == PG_SEMANTIC_RUNTIME_SELECTED_NOT_OPEN);
	semloom_provider_error_clear(&error);
	previous_context = MemoryContextSwitchTo(runtime->owner_context);
	PG_TRY();
	{
		status = runtime->provider.ops->open(runtime->provider.config,
											 &runtime->open_spec,
											 &runtime->provider_session,
											 &error);
		MemoryContextSwitchTo(previous_context);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(previous_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (status != AI_PROVIDER_STATUS_OK || runtime->provider_session == NULL)
	{
		if (status == AI_PROVIDER_STATUS_OK)
			semloom_provider_error_set(&error,
									   AI_PROVIDER_ERROR_SESSION_CLOSED,
									   0,
									   0,
									   NULL);
		pg_semantic_runtime_fail(runtime, &error);
	}
	runtime->state = PG_SEMANTIC_RUNTIME_OPEN;
}

static void
pg_semantic_runtime_copy_completion(const AiCompletion *provider_completion,
									MemoryContext result_context,
									PgSemanticCompletion *completion)
{
	MemoryContext previous_context;
	uint8 *copied_data = NULL;

	previous_context = MemoryContextSwitchTo(result_context);
	PG_TRY();
	{
		if (!provider_completion->is_null &&
			provider_completion->output.length > 0)
		{
			copied_data = palloc(provider_completion->output.length);
			memcpy(copied_data,
				   provider_completion->output.data,
				   provider_completion->output.length);
		}
		MemoryContextSwitchTo(previous_context);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(previous_context);
		PG_RE_THROW();
	}
	PG_END_TRY();

	completion->data = copied_data;
	completion->length = provider_completion->output.length;
	completion->is_null = provider_completion->is_null;
}

static void
pg_semantic_runtime_fail(PgSemanticRuntime *runtime,
						 const AiProviderError *error)
{
	AiProviderError saved_error = *error;

	runtime->state = PG_SEMANTIC_RUNTIME_TERMINAL;
	pg_semantic_runtime_release_session(runtime);
	semloom_raise_provider_error(&saved_error);
}

static void
semloom_raise_provider_error(const AiProviderError *error)
{
	int sqlstate = ERRCODE_INTERNAL_ERROR;
	const char *message = "SemLoom provider returned an unknown error";
	const char *detail = semloom_provider_error_detail(error);

	switch (error->code)
	{
		case AI_PROVIDER_ERROR_INVALID_SPEC:
			sqlstate = ERRCODE_INVALID_PARAMETER_VALUE;
			message = "invalid recording provider plan specification";
			break;
		case AI_PROVIDER_ERROR_SESSION_CLOSED:
			sqlstate = ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE;
			message = "recording provider session is not open";
			break;
		case AI_PROVIDER_ERROR_TASK_MISMATCH:
			sqlstate = ERRCODE_DATA_EXCEPTION;
			message = "recording provider task does not match the open plan";
			break;
		case AI_PROVIDER_ERROR_NULL_TASK:
			sqlstate = ERRCODE_DATA_EXCEPTION;
			message = "PROPAGATE_NULL tasks must be completed by the PostgreSQL executor";
			break;
		case AI_PROVIDER_ERROR_INPUT_TOO_LARGE:
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("SemLoom provider input exceeds the %u byte limit",
							(unsigned int) error->limit_bytes)));
			pg_unreachable();
		case AI_PROVIDER_ERROR_UNSUPPORTED_ENCODING:
			sqlstate = ERRCODE_FEATURE_NOT_SUPPORTED;
			message = "SemLoom provider does not support the database encoding";
			break;
		case AI_PROVIDER_ERROR_RESOURCE_EXHAUSTED:
			sqlstate = ERRCODE_INSUFFICIENT_RESOURCES;
			message = "SemLoom provider could not reserve a required resource";
			break;
		case AI_PROVIDER_ERROR_CONNECTION_LOST:
			sqlstate = ERRCODE_CONNECTION_FAILURE;
			message = "SemLoom provider connection was lost";
			break;
		case AI_PROVIDER_ERROR_MESSAGE_TOO_LARGE:
			sqlstate = ERRCODE_PROGRAM_LIMIT_EXCEEDED;
			message = "SemLoom provider message exceeds its configured limit";
			break;
		case AI_PROVIDER_ERROR_NUMERIC_RANGE:
			sqlstate = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;
			message = "integer out of range";
			break;
		case AI_PROVIDER_ERROR_SYSTEM:
			message = detail != NULL ? detail : "SemLoom provider system operation failed";
			errno = error->system_errno;
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("%s: %m", message)));
			pg_unreachable();
		case AI_PROVIDER_ERROR_PROTOCOL:
			sqlstate = ERRCODE_PROTOCOL_VIOLATION;
			message = "SemLoom provider returned an unexpected message";
			break;
		default:
			break;
	}
	if (detail != NULL)
		message = detail;
	ereport(ERROR,
			(errcode(sqlstate),
			 errmsg("%s", message)));
	pg_unreachable();
}

static const char *
semloom_provider_error_detail(const AiProviderError *error)
{
	if (error->detail_length == 0 ||
		error->detail_length >= AI_PROVIDER_ERROR_DETAIL_CAPACITY ||
		error->detail[error->detail_length] != '\0' ||
		memchr(error->detail, '\0', error->detail_length) != NULL)
		return NULL;
	return error->detail;
}
