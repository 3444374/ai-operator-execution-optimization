/* PostgreSQL-private provider lifecycle shared by semantic operators. */
#include "postgres.h"

#include <errno.h>

#include "common/cryptohash.h"
#include "common/sha2.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "utils/memutils.h"

#include "executor/pg_semantic_runtime.h"
#include "provider/provider_private.h"
#include "semantics/semantic_map_contract.h"

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
	uint64 model_calls;
	uint64 prompt_tokens;
	uint64 output_tokens;
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
static void pg_semantic_runtime_payload_digest(
	const AiOpenSpec *open_spec,
	AiByteSlice input,
	AiByteSlice canonical_messages,
	char output[AI_PROVIDER_SHA256_HEX_LENGTH + 1]);
static void pg_semantic_runtime_hash_bytes(pg_cryptohash_ctx *context,
											 const void *data,
											 Size length);
static void pg_semantic_runtime_hash_uint64(pg_cryptohash_ctx *context,
											  uint64 value);
static bool pg_semantic_runtime_slice_equals(AiByteSlice actual,
											AiByteSlice expected);
static void pg_semantic_runtime_validate_map(PgSemanticRuntime *runtime,
	const AiCompletion *completion);

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
	runtime->open_spec.physical_role = pg_semantic_runtime_copy_slice(
		owner_context, open_spec.physical_role);
	runtime->open_spec.prompt_program_digest = pg_semantic_runtime_copy_slice(
		owner_context, open_spec.prompt_program_digest);
	runtime->open_spec.result_parser_digest = pg_semantic_runtime_copy_slice(
		owner_context, open_spec.result_parser_digest);
	runtime->open_spec.model_id = pg_semantic_runtime_copy_slice(
		owner_context, open_spec.model_id);
	runtime->open_spec.semantic_spec_digest = pg_semantic_runtime_copy_slice(
		owner_context, open_spec.semantic_spec_digest);
	runtime->open_spec.physical_algorithm_digest = pg_semantic_runtime_copy_slice(
		owner_context, open_spec.physical_algorithm_digest);
	runtime->open_spec.stop = pg_semantic_runtime_copy_slice(
		owner_context, open_spec.stop);
	if (open_spec.has_generation_profile)
	{
		uint32 index;
		AiGenerationProfile *profile = &runtime->open_spec.generation_profile;

		profile->profile_id = pg_semantic_runtime_copy_slice(owner_context,
			open_spec.generation_profile.profile_id);
		for (index = 0; index < profile->choice_count; index++)
			profile->choices[index] = pg_semantic_runtime_copy_slice(owner_context,
				open_spec.generation_profile.choices[index]);
	}
	runtime->state = PG_SEMANTIC_RUNTIME_SELECTED_NOT_OPEN;
	runtime->cleanup_callback.func = pg_semantic_runtime_cleanup;
	runtime->cleanup_callback.arg = runtime;
	MemoryContextRegisterResetCallback(owner_context, &runtime->cleanup_callback);
	semloom_provider_select(owner_context, &runtime->open_spec, &runtime->provider);
	return runtime;
}

void
pg_semantic_runtime_preflight_input(PgSemanticRuntime *runtime, AiByteSlice input)
{
	AiProviderError error;
	uint32 limit;

	Assert(runtime != NULL);
	limit = runtime->open_spec.max_input_bytes != 0 ?
		runtime->open_spec.max_input_bytes : runtime->provider.max_input_bytes;
	if (limit == 0 || input.length <= limit)
		return;
	semloom_provider_error_set(&error,
								   AI_PROVIDER_ERROR_INPUT_TOO_LARGE,
								   0,
								   limit,
								   NULL);
	pg_semantic_runtime_fail(runtime, &error);
}

void
pg_semantic_runtime_drive(PgSemanticRuntime *runtime,
						  AiByteSlice input,
						  AiByteSlice canonical_messages,
						  MemoryContext result_context,
						  PgSemanticCompletion *completion)
{
	AiPreparedTask task = {0};
	AiCompletion provider_completion = {0};
	AiProviderError error;
	AiProviderStatus status;
	char semantic_payload_digest[AI_PROVIDER_SHA256_HEX_LENGTH + 1];

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
	task.canonical_messages = canonical_messages;
	task.is_null = false;
	if (runtime->open_spec.semantic_spec_digest.length ==
		AI_PROVIDER_SHA256_HEX_LENGTH)
	{
		pg_semantic_runtime_payload_digest(&runtime->open_spec,
										   input,
										   canonical_messages,
										   semantic_payload_digest);
		task.semantic_payload_digest.data =
			(const uint8 *) semantic_payload_digest;
		task.semantic_payload_digest.length = AI_PROVIDER_SHA256_HEX_LENGTH;
	}
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
	if (runtime->open_spec.plan_schema_version == SEMLOOM_MAP_PLAN_SCHEMA_VERSION)
		pg_semantic_runtime_validate_map(runtime, &provider_completion);
	else if (runtime->open_spec.model_id.length > 0 &&
		(provider_completion.is_null ||
		 !pg_semantic_runtime_slice_equals(provider_completion.response_model_id,
										 runtime->open_spec.model_id) ||
		 provider_completion.finish_reason.length != 4 ||
		 provider_completion.finish_reason.data == NULL ||
		 memcmp(provider_completion.finish_reason.data, "stop", 4) != 0))
	{
		semloom_provider_error_set(&error,
								   AI_PROVIDER_ERROR_PROTOCOL,
								   0,
								   0,
								   "SemLoom provider completion metadata does not match the exact plan");
		pg_semantic_runtime_fail(runtime, &error);
	}
	if (runtime->open_spec.model_id.length > 0 &&
		(runtime->model_calls == PG_UINT64_MAX ||
		 runtime->prompt_tokens > PG_UINT64_MAX - provider_completion.prompt_tokens ||
		 runtime->output_tokens > PG_UINT64_MAX - provider_completion.output_tokens))
	{
		semloom_provider_error_set(&error,
								   AI_PROVIDER_ERROR_NUMERIC_RANGE,
								   0,
								   0,
								   "SemLoom provider usage counters exceed uint64 range");
		pg_semantic_runtime_fail(runtime, &error);
	}

	pg_semantic_runtime_copy_completion(&provider_completion,
										result_context,
										completion);
	runtime->next_sequence++;
	if (runtime->open_spec.model_id.length > 0)
	{
		runtime->model_calls++;
		runtime->prompt_tokens += provider_completion.prompt_tokens;
		runtime->output_tokens += provider_completion.output_tokens;
	}
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
	semloom_plan_spec_explain(&runtime->plan_spec, explain_state);
}

void
pg_semantic_runtime_explain_counters(const PgSemanticRuntime *runtime,
									 ExplainState *explain_state)
{
	Assert(runtime != NULL);
	if (explain_state->analyze)
	{
		if (runtime->plan_spec.model_id != NULL)
		{
			ExplainPropertyUInteger("Model Calls", NULL,
									 runtime->model_calls,
									 explain_state);
			ExplainPropertyUInteger("Prompt Tokens", NULL,
									 runtime->prompt_tokens,
									 explain_state);
			ExplainPropertyUInteger("Output Tokens", NULL,
									 runtime->output_tokens,
									 explain_state);
		}
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
	open_spec->order_policy = plan_spec->order_policy == SEMLOOM_PLAN_ORDER_INPUT ?
		AI_PROVIDER_ORDER_INPUT : 0;
	open_spec->plan_schema_version = plan_spec->schema_version;
	open_spec->semantic_spec_version = plan_spec->semantic_spec_version;
	open_spec->semantic_spec_id.data = (const uint8 *) plan_spec->semantic_spec_id;
	open_spec->semantic_spec_id.length = plan_spec->semantic_spec_id_length;
	open_spec->physical_algorithm.data =
		(const uint8 *) plan_spec->physical_algorithm;
	open_spec->physical_algorithm.length = plan_spec->physical_algorithm_length;
	if (plan_spec->physical_role != NULL)
	{
		open_spec->physical_role.data = (const uint8 *) plan_spec->physical_role;
		open_spec->physical_role.length = strlen(plan_spec->physical_role);
	}
	if (plan_spec->prompt_program_digest != NULL)
	{
		open_spec->prompt_program_digest.data =
			(const uint8 *) plan_spec->prompt_program_digest;
		open_spec->prompt_program_digest.length =
			strlen(plan_spec->prompt_program_digest);
	}
	if (plan_spec->result_parser_digest != NULL)
	{
		open_spec->result_parser_digest.data =
			(const uint8 *) plan_spec->result_parser_digest;
		open_spec->result_parser_digest.length =
			strlen(plan_spec->result_parser_digest);
	}
	if (plan_spec->model_id != NULL)
	{
		open_spec->model_id.data = (const uint8 *) plan_spec->model_id;
		open_spec->model_id.length = plan_spec->model_id_length;
	}
	if (plan_spec->semantic_spec_digest != NULL)
	{
		open_spec->semantic_spec_digest.data =
			(const uint8 *) plan_spec->semantic_spec_digest;
		open_spec->semantic_spec_digest.length =
			strlen(plan_spec->semantic_spec_digest);
	}
	if (plan_spec->physical_algorithm_digest != NULL)
	{
		open_spec->physical_algorithm_digest.data =
			(const uint8 *) plan_spec->physical_algorithm_digest;
		open_spec->physical_algorithm_digest.length =
			strlen(plan_spec->physical_algorithm_digest);
	}
	open_spec->temperature = plan_spec->temperature;
	open_spec->top_p = plan_spec->top_p;
	open_spec->max_tokens = plan_spec->max_tokens;
	open_spec->n = plan_spec->n;
	open_spec->stream = plan_spec->stream;
	open_spec->has_stop = plan_spec->stop != NULL;
	open_spec->max_input_bytes = plan_spec->max_input_bytes;
	open_spec->max_output_bytes = plan_spec->max_output_bytes;
	open_spec->has_generation_profile = plan_spec->generation_profile_digest != NULL;
	if (open_spec->has_generation_profile)
		open_spec->generation_profile = plan_spec->generation_profile;
	if (plan_spec->stop != NULL)
	{
		open_spec->stop.data = (const uint8 *) plan_spec->stop;
		open_spec->stop.length = strlen(plan_spec->stop);
	}
}

static void
pg_semantic_runtime_cleanup(void *argument)
{
	pg_semantic_runtime_close((PgSemanticRuntime *) argument);
}

static void
pg_semantic_runtime_validate_map(PgSemanticRuntime *runtime,
	const AiCompletion *completion)
{
	SemloomMapPlanValues values = {
		.instruction = {(const uint8 *) runtime->plan_spec.instruction,
			runtime->plan_spec.instruction_length, false},
		.model_id = {runtime->open_spec.model_id.data, runtime->open_spec.model_id.length, false},
		.max_tokens = runtime->open_spec.max_tokens,
	};
	SemloomMachineCompletion value = {
		.data = completion->output.data, .length = completion->output.length,
		.is_null = completion->is_null,
		.response_model_id = {completion->response_model_id.data, completion->response_model_id.length, false},
		.finish_reason = {completion->finish_reason.data, completion->finish_reason.length, false},
		.prompt_tokens = completion->prompt_tokens, .output_tokens = completion->output_tokens,
	};
	uint32 status = semloom_map_completion_status(&values, &value);
	AiProviderError error;

	if (status == SEMLOOM_MAP_COMPLETION_VALID)
		return;
	if (status == SEMLOOM_MAP_COMPLETION_INCOMPLETE)
	{
		runtime->state = PG_SEMANTIC_RUNTIME_TERMINAL;
		pg_semantic_runtime_release_session(runtime);
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
			errmsg("SemMap model completion must finish with stop")));
	}
	semloom_provider_error_clear(&error);
	semloom_provider_error_set(&error,
		status == SEMLOOM_MAP_COMPLETION_TOO_LARGE ?
		AI_PROVIDER_ERROR_MESSAGE_TOO_LARGE : AI_PROVIDER_ERROR_PROTOCOL,
		0, runtime->open_spec.max_output_bytes,
		status == SEMLOOM_MAP_COMPLETION_TOO_LARGE ? NULL :
		"SemMap provider returned invalid completion metadata");
	pg_semantic_runtime_fail(runtime, &error);
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
		case AI_PROVIDER_ERROR_REMOTE_UNAVAILABLE:
			sqlstate = ERRCODE_CONNECTION_FAILURE;
			message = "SemLoom model endpoint is unavailable";
			break;
		case AI_PROVIDER_ERROR_REMOTE_TIMEOUT:
			sqlstate = ERRCODE_CONNECTION_FAILURE;
			message = "SemLoom model endpoint timed out";
			break;
		case AI_PROVIDER_ERROR_REQUEST_REJECTED:
			sqlstate = ERRCODE_EXTERNAL_ROUTINE_EXCEPTION;
			message = "SemLoom model request was rejected";
			break;
		case AI_PROVIDER_ERROR_INVALID_RESPONSE:
			sqlstate = ERRCODE_PROTOCOL_VIOLATION;
			message = "SemLoom model endpoint returned an invalid response";
			break;
		case AI_PROVIDER_ERROR_ADAPTER_INTERNAL:
			sqlstate = ERRCODE_INTERNAL_ERROR;
			message = "SemLoom provider failed internally";
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

static void
pg_semantic_runtime_payload_digest(
	const AiOpenSpec *open_spec,
	AiByteSlice input,
	AiByteSlice canonical_messages,
	char output[AI_PROVIDER_SHA256_HEX_LENGTH + 1])
{
	const char *domain = open_spec->plan_schema_version == SEMLOOM_MAP_PLAN_SCHEMA_VERSION ?
		"semloom-payload-v5" : (open_spec->has_generation_profile ?
		"semloom-payload-v4" : "semloom-payload-v3");
	static const char hex[] = "0123456789abcdef";
	pg_cryptohash_ctx *context;
	uint8 digest[PG_SHA256_DIGEST_LENGTH];
	uint8 null_flag = 0;
	int index;

	Assert(open_spec->semantic_spec_digest.length == AI_PROVIDER_SHA256_HEX_LENGTH);
	context = pg_cryptohash_create(PG_SHA256);
	if (context == NULL || pg_cryptohash_init(context) < 0)
	{
		if (context != NULL)
			pg_cryptohash_free(context);
		elog(ERROR, "could not initialize SemLoom payload digest");
	}
	pg_semantic_runtime_hash_bytes(context, domain, strlen(domain) + 1);
	pg_semantic_runtime_hash_bytes(context,
								   open_spec->semantic_spec_digest.data,
								   open_spec->semantic_spec_digest.length);
	pg_semantic_runtime_hash_bytes(context, &null_flag, sizeof(null_flag));
	pg_semantic_runtime_hash_uint64(context, input.length);
	pg_semantic_runtime_hash_bytes(context, input.data, input.length);
	pg_semantic_runtime_hash_uint64(context, canonical_messages.length);
	pg_semantic_runtime_hash_bytes(context,
								   canonical_messages.data,
								   canonical_messages.length);
	if (pg_cryptohash_final(context, digest, sizeof(digest)) < 0)
	{
		pg_cryptohash_free(context);
		elog(ERROR, "could not finish SemLoom payload digest");
	}
	pg_cryptohash_free(context);
	for (index = 0; index < PG_SHA256_DIGEST_LENGTH; index++)
	{
		output[index * 2] = hex[digest[index] >> 4];
		output[index * 2 + 1] = hex[digest[index] & 0x0f];
	}
	output[AI_PROVIDER_SHA256_HEX_LENGTH] = '\0';
}

static void
pg_semantic_runtime_hash_bytes(pg_cryptohash_ctx *context,
								 const void *data,
								 Size length)
{
	if (length > 0 && pg_cryptohash_update(context, data, length) < 0)
	{
		pg_cryptohash_free(context);
		elog(ERROR, "could not update SemLoom payload digest");
	}
}

static void
pg_semantic_runtime_hash_uint64(pg_cryptohash_ctx *context, uint64 value)
{
	uint8 bytes[8];
	int shift;

	for (shift = 7; shift >= 0; shift--)
		bytes[7 - shift] = (uint8) (value >> (shift * 8));
	pg_semantic_runtime_hash_bytes(context, bytes, sizeof(bytes));
}

static bool
pg_semantic_runtime_slice_equals(AiByteSlice actual, AiByteSlice expected)
{
	return actual.length == expected.length &&
		(actual.length == 0 ||
		 (actual.data != NULL && expected.data != NULL &&
		  memcmp(actual.data, expected.data, actual.length) == 0));
}
