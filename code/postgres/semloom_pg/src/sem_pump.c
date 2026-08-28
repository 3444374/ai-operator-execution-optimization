#include "postgres.h"

#include <errno.h>

#include "catalog/pg_type_d.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "ai_provider_port.h"
#include "provider_private.h"
#include "sem_pump.h"

#define SEMLOOM_VISIBLE_UDS_INPUT_LIMIT_BYTES 174080

typedef enum SemloomPumpState
{
	SEMLOOM_PUMP_SELECTED_NOT_OPEN = 1,
	SEMLOOM_PUMP_OPEN = 2,
	SEMLOOM_PUMP_READY = 3,
	SEMLOOM_PUMP_TERMINAL = 4,
	SEMLOOM_PUMP_CLOSED = 5,
} SemloomPumpState;

struct SemloomExecPump
{
	PlanState *child_state;
	AttrNumber mapped_column;
	Oid input_type;
	Oid output_type;
	AiOpenSpec open_spec;
	AiProvider provider;
	AiProviderSession *provider_session;
	MemoryContext owner_context;
	MemoryContextCallback cleanup_callback;
	SemloomPumpState state;
	uint64 next_sequence;
	uint64 accepted_rows;
	uint64 emitted_rows;
};

static void semloom_pump_cleanup(void *argument);
static void semloom_pump_close_session(SemloomExecPump *pump);
static void semloom_pump_open_provider(SemloomExecPump *pump);
static void semloom_pump_fail(SemloomExecPump *pump,
							  const AiProviderError *error) pg_attribute_noreturn();
static void semloom_raise_provider_error(
	const AiProviderError *error) pg_attribute_noreturn();
static Datum semloom_copy_completion_text(const AiCompletion *completion,
										  MemoryContext result_context);

SemloomExecPump *
semloom_pump_begin(CustomScanState *node, EState *estate, int executor_flags)
{
	CustomScan *scan = castNode(CustomScan, node->ss.ps.plan);
	MemoryContext owner_context = estate->es_query_cxt;
	SemloomExecPump *pump;
	int unsupported_flags = EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK | EXEC_FLAG_REWIND;

	if ((executor_flags & unsupported_flags) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("SemMap capability supports forward execution only")));
	if (list_length(scan->custom_plans) != 1 || list_length(scan->custom_private) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid SemMap executor state")));

	pump = MemoryContextAllocZero(owner_context, sizeof(*pump));
	pump->owner_context = owner_context;
	pump->mapped_column = linitial_int(scan->custom_private);
	if (pump->mapped_column <= 0 ||
		pump->mapped_column > node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SemMap mapped output is outside the scan tuple")));
	pump->input_type = TEXTOID;
	pump->output_type = TEXTOID;
	pump->open_spec.operator_kind = AI_PROVIDER_OPERATOR_MAP;
	pump->open_spec.input_value_kind = AI_PROVIDER_VALUE_TEXT;
	pump->open_spec.output_value_kind = AI_PROVIDER_VALUE_TEXT;
	pump->open_spec.null_policy = AI_PROVIDER_NULL_PROPAGATE;
	pump->open_spec.error_policy = AI_PROVIDER_ERROR_FAIL_QUERY;
	pump->open_spec.semantic_spec_version = SEMLOOM_RECORDING_SPEC_VERSION;
	pump->open_spec.semantic_spec_id.data =
		(const uint8 *) SEMLOOM_RECORDING_SPEC_ID;
	pump->open_spec.semantic_spec_id.length = sizeof(SEMLOOM_RECORDING_SPEC_ID) - 1;
	pump->open_spec.physical_algorithm.data =
		(const uint8 *) SEMLOOM_RECORDING_ALGORITHM;
	pump->open_spec.physical_algorithm.length =
		sizeof(SEMLOOM_RECORDING_ALGORITHM) - 1;
	semloom_provider_select(owner_context, &pump->provider);
	pump->state = SEMLOOM_PUMP_SELECTED_NOT_OPEN;
	pump->cleanup_callback.func = semloom_pump_cleanup;
	pump->cleanup_callback.arg = pump;
	MemoryContextRegisterResetCallback(owner_context, &pump->cleanup_callback);

	pump->child_state =
		ExecInitNode(linitial_node(Plan, scan->custom_plans), estate, executor_flags);
	node->custom_ps = list_make1(pump->child_state);
	return pump;
}

TupleTableSlot *
semloom_pump_next(SemloomExecPump *pump, ScanState *scan_state)
{
	TupleTableSlot *child_slot = ExecProcNode(pump->child_state);
	TupleTableSlot *scan_slot = scan_state->ss_ScanTupleSlot;
	int attribute_index;

	if (TupIsNull(child_slot))
		return ExecClearTuple(scan_slot);
	slot_getallattrs(child_slot);
	if (child_slot->tts_tupleDescriptor->natts != scan_slot->tts_tupleDescriptor->natts)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SemMap child and scan tuple descriptors do not match")));

	ExecClearTuple(scan_slot);
	for (attribute_index = 0; attribute_index < scan_slot->tts_tupleDescriptor->natts;
		 attribute_index++)
	{
		bool is_null = child_slot->tts_isnull[attribute_index];

		if (attribute_index + 1 == pump->mapped_column)
		{
			AiPreparedTask task = {0};
			AiCompletion completion = {0};
			AiProviderError error;
			AiProviderStatus status;
			text *input_text;
			Size input_length;

			if (is_null)
			{
				if (pump->open_spec.null_policy != AI_PROVIDER_NULL_PROPAGATE)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("SemMap has an unsupported NULL policy")));
				scan_slot->tts_isnull[attribute_index] = true;
				scan_slot->tts_values[attribute_index] = (Datum) 0;
				continue;
			}
			if (pump->provider_session == NULL)
				semloom_pump_open_provider(pump);

			input_text = DatumGetTextPP(child_slot->tts_values[attribute_index]);
			input_length = VARSIZE_ANY_EXHDR(input_text);
			Assert(input_length <= PG_UINT32_MAX);
			task.sequence = pump->next_sequence;
			task.input.data = (const uint8 *) VARDATA_ANY(input_text);
			task.input.length = (uint32) input_length;
			task.is_null = false;
			semloom_provider_error_clear(&error);
			status = pump->provider.ops->drive(pump->provider_session,
													   &task,
													   &completion,
													   &error);
			if (status != AI_PROVIDER_STATUS_OK)
				semloom_pump_fail(pump, &error);
			if (completion.sequence != task.sequence ||
				(!completion.is_null && completion.output.length > 0 &&
				 completion.output.data == NULL))
			{
				semloom_pump_close_session(pump);
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("recording provider task does not match the open plan")));
			}
			scan_slot->tts_isnull[attribute_index] = completion.is_null;
			scan_slot->tts_values[attribute_index] = completion.is_null ?
				(Datum) 0 :
				semloom_copy_completion_text(
					&completion,
					scan_state->ps.ps_ExprContext->ecxt_per_tuple_memory);
			pump->next_sequence++;
			pump->accepted_rows++;
			pump->emitted_rows++;
			pump->state = SEMLOOM_PUMP_READY;
		}
		else
		{
			scan_slot->tts_isnull[attribute_index] = is_null;
			scan_slot->tts_values[attribute_index] =
				child_slot->tts_values[attribute_index];
		}
	}

	return ExecStoreVirtualTuple(scan_slot);
}

void
semloom_pump_stop(SemloomExecPump *pump, CustomScanState *node)
{
	if (pump == NULL)
		return;
	semloom_pump_close_session(pump);
	if (pump->child_state != NULL)
	{
		ExecEndNode(pump->child_state);
		pump->child_state = NULL;
	}
	pump->state = SEMLOOM_PUMP_CLOSED;
	node->custom_ps = NIL;
}

void
semloom_pump_explain(const SemloomExecPump *pump, ExplainState *explain_state)
{
	ExplainPropertyText("Provider", pump->provider.ops->adapter_name, explain_state);
	ExplainPropertyText("Physical Role", "reference", explain_state);
	ExplainPropertyInteger("Mapped Column", NULL, pump->mapped_column, explain_state);
	if (explain_state->analyze)
	{
		ExplainPropertyInteger("Accepted Rows", NULL, pump->accepted_rows, explain_state);
		ExplainPropertyInteger("Emitted Rows", NULL, pump->emitted_rows, explain_state);
	}
}

static void
semloom_pump_cleanup(void *argument)
{
	SemloomExecPump *pump = argument;

	semloom_pump_close_session(pump);
}

static void
semloom_pump_close_session(SemloomExecPump *pump)
{
	AiProviderSession *session;

	if (pump == NULL)
		return;
	session = pump->provider_session;
	pump->provider_session = NULL;
	if (session != NULL && pump->provider.ops != NULL)
		pump->provider.ops->close(session);
}

static void
semloom_pump_open_provider(SemloomExecPump *pump)
{
	MemoryContext previous_context;
	AiProviderError error;
	AiProviderStatus status = AI_PROVIDER_STATUS_ERROR;

	Assert(pump->state == SEMLOOM_PUMP_SELECTED_NOT_OPEN);
	semloom_provider_error_clear(&error);
	previous_context = MemoryContextSwitchTo(pump->owner_context);
	PG_TRY();
	{
		status = pump->provider.ops->open(pump->provider.config,
											  &pump->open_spec,
											  &pump->provider_session,
											  &error);
		MemoryContextSwitchTo(previous_context);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(previous_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (status != AI_PROVIDER_STATUS_OK || pump->provider_session == NULL)
	{
		if (status == AI_PROVIDER_STATUS_OK)
			semloom_provider_error_set(&error,
									   AI_PROVIDER_ERROR_SESSION_CLOSED,
									   AI_PROVIDER_OPERATION_NONE,
									   0,
									   NULL);
		semloom_pump_fail(pump, &error);
	}
	pump->state = SEMLOOM_PUMP_OPEN;
}

static void
semloom_pump_fail(SemloomExecPump *pump, const AiProviderError *error)
{
	AiProviderError saved_error = *error;

	pump->state = SEMLOOM_PUMP_TERMINAL;
	semloom_pump_close_session(pump);
	semloom_raise_provider_error(&saved_error);
}

static void
semloom_raise_provider_error(const AiProviderError *error)
{
	switch (error->code)
	{
		case AI_PROVIDER_ERROR_INVALID_SPEC:
			switch (error->operation)
			{
				case AI_PROVIDER_OPERATION_SOCKET_PATH_LENGTH:
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("SemLoom provider socket path is too long")));
				case AI_PROVIDER_OPERATION_SOCKET_PATH_ABSOLUTE:
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("SemLoom provider socket path must be absolute")));
				default:
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid recording provider plan specification")));
			}
		case AI_PROVIDER_ERROR_SESSION_CLOSED:
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("recording provider session is not open")));
		case AI_PROVIDER_ERROR_TASK_MISMATCH:
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("recording provider task does not match the open plan")));
		case AI_PROVIDER_ERROR_NULL_TASK:
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("PROPAGATE_NULL tasks must be completed by the PostgreSQL executor")));
		case AI_PROVIDER_ERROR_INPUT_TOO_LARGE:
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("SemLoom provider input exceeds the %d byte limit",
							SEMLOOM_VISIBLE_UDS_INPUT_LIMIT_BYTES)));
		case AI_PROVIDER_ERROR_UNSUPPORTED_ENCODING:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("SemLoom UDS recording provider requires UTF8 database encoding")));
		case AI_PROVIDER_ERROR_RESOURCE_EXHAUSTED:
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("could not reserve a file descriptor for the SemLoom provider")));
		case AI_PROVIDER_ERROR_CONNECTION_LOST:
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("SemLoom provider disconnected before completing a frame")));
		case AI_PROVIDER_ERROR_FRAME_LIMIT:
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("SemLoom provider frame length is outside the protocol limit")));
		case AI_PROVIDER_ERROR_NUMERIC_RANGE:
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("integer out of range")));
		case AI_PROVIDER_ERROR_SYSTEM:
			errno = error->system_errno;
			switch (error->operation)
			{
				case AI_PROVIDER_OPERATION_CREATE_SOCKET:
					ereport(ERROR,
							(errcode_for_socket_access(),
							 errmsg("could not create SemLoom provider socket: %m")));
				case AI_PROVIDER_OPERATION_CONFIGURE_SOCKET:
					ereport(ERROR,
							(errcode_for_socket_access(),
							 errmsg("could not make SemLoom provider socket nonblocking: %m")));
				case AI_PROVIDER_OPERATION_INSPECT_SOCKET:
					ereport(ERROR,
							(errcode_for_socket_access(),
							 errmsg("could not inspect SemLoom provider socket connection: %m")));
				case AI_PROVIDER_OPERATION_WRITE_SOCKET:
					ereport(ERROR,
							(errcode_for_socket_access(),
							 errmsg("could not write to SemLoom provider socket: %m")));
				case AI_PROVIDER_OPERATION_READ_SOCKET:
					ereport(ERROR,
							(errcode_for_socket_access(),
							 errmsg("could not read from SemLoom provider socket: %m")));
				default:
					ereport(ERROR,
							(errcode_for_socket_access(),
							 errmsg("could not connect to SemLoom provider socket: %m")));
			}
		case AI_PROVIDER_ERROR_PROTOCOL:
			switch (error->operation)
			{
				case AI_PROVIDER_OPERATION_RECEIVE_FRAME:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("SemLoom provider returned an invalid frame length")));
				case AI_PROVIDER_OPERATION_PARSE_JSON:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("SemLoom provider returned invalid JSON")));
				case AI_PROVIDER_OPERATION_RESPONSE_OBJECT:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("SemLoom provider response must be a JSON object")));
				case AI_PROVIDER_OPERATION_RESPONSE_FIELD:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("SemLoom provider response is missing a required field")));
				case AI_PROVIDER_OPERATION_RESPONSE_INTEGER:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("SemLoom provider response has an invalid integer field")));
				case AI_PROVIDER_OPERATION_RESPONSE_BOOLEAN:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("SemLoom provider response has an invalid boolean field")));
				case AI_PROVIDER_OPERATION_PROVIDER_REJECTED:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("SemLoom provider rejected the protocol message")));
				case AI_PROVIDER_OPERATION_OPEN_RESPONSE:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("SemLoom provider open response does not match the requested protocol")));
				case AI_PROVIDER_OPERATION_COMPLETION_IDENTITY:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("SemLoom provider completion identity does not match the task")));
				case AI_PROVIDER_OPERATION_COMPLETION_OUTPUT:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("SemLoom provider completion has an invalid output")));
				case AI_PROVIDER_OPERATION_COMPLETION_EVIDENCE:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("SemLoom provider completion evidence digest does not match")));
				default:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("SemLoom provider returned an unexpected message")));
			}
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("SemLoom provider returned an unknown error")));
	}
	pg_unreachable();
}

static Datum
semloom_copy_completion_text(const AiCompletion *completion,
							 MemoryContext result_context)
{
	const char *output_data = completion->output.length == 0 ?
		"" : (const char *) completion->output.data;
	MemoryContext previous_context;
	text *output_text = NULL;

	previous_context = MemoryContextSwitchTo(result_context);
	PG_TRY();
	{
		output_text = cstring_to_text_with_len(output_data,
											 completion->output.length);
		MemoryContextSwitchTo(previous_context);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(previous_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return PointerGetDatum(output_text);
}
