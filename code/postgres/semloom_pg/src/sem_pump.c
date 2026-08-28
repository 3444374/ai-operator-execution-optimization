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
pg_noreturn static void semloom_pump_fail(SemloomExecPump *pump,
									  const AiProviderError *error);
pg_noreturn static void semloom_raise_provider_error(
	const AiProviderError *error);
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
	int sqlstate = ERRCODE_INTERNAL_ERROR;
	const char *message = "SemLoom provider returned an unknown error";

	switch (error->code)
	{
		case AI_PROVIDER_ERROR_INVALID_SPEC:
			sqlstate = ERRCODE_INVALID_PARAMETER_VALUE;
			switch (error->operation)
			{
				case AI_PROVIDER_OPERATION_SOCKET_PATH_LENGTH:
					message = "SemLoom provider socket path is too long";
					break;
				case AI_PROVIDER_OPERATION_SOCKET_PATH_ABSOLUTE:
					message = "SemLoom provider socket path must be absolute";
					break;
				default:
					message = "invalid recording provider plan specification";
					break;
			}
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
					 errmsg("SemLoom provider input exceeds the %d byte limit",
							SEMLOOM_VISIBLE_UDS_INPUT_LIMIT_BYTES)));
			pg_unreachable();
		case AI_PROVIDER_ERROR_UNSUPPORTED_ENCODING:
			sqlstate = ERRCODE_FEATURE_NOT_SUPPORTED;
			message = "SemLoom UDS recording provider requires UTF8 database encoding";
			break;
		case AI_PROVIDER_ERROR_RESOURCE_EXHAUSTED:
			sqlstate = ERRCODE_INSUFFICIENT_RESOURCES;
			message = "could not reserve a file descriptor for the SemLoom provider";
			break;
		case AI_PROVIDER_ERROR_CONNECTION_LOST:
			sqlstate = ERRCODE_CONNECTION_FAILURE;
			message = "SemLoom provider disconnected before completing a frame";
			break;
		case AI_PROVIDER_ERROR_FRAME_LIMIT:
			sqlstate = ERRCODE_PROGRAM_LIMIT_EXCEEDED;
			message = "SemLoom provider frame length is outside the protocol limit";
			break;
		case AI_PROVIDER_ERROR_NUMERIC_RANGE:
			sqlstate = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;
			message = "integer out of range";
			break;
		case AI_PROVIDER_ERROR_SYSTEM:
			switch (error->operation)
			{
				case AI_PROVIDER_OPERATION_CREATE_SOCKET:
					message = "could not create SemLoom provider socket";
					break;
				case AI_PROVIDER_OPERATION_CONFIGURE_SOCKET:
					message = "could not make SemLoom provider socket nonblocking";
					break;
				case AI_PROVIDER_OPERATION_INSPECT_SOCKET:
					message = "could not inspect SemLoom provider socket connection";
					break;
				case AI_PROVIDER_OPERATION_WRITE_SOCKET:
					message = "could not write to SemLoom provider socket";
					break;
				case AI_PROVIDER_OPERATION_READ_SOCKET:
					message = "could not read from SemLoom provider socket";
					break;
				default:
					message = "could not connect to SemLoom provider socket";
					break;
			}
			errno = error->system_errno;
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("%s: %m", message)));
			pg_unreachable();
		case AI_PROVIDER_ERROR_PROTOCOL:
			sqlstate = ERRCODE_PROTOCOL_VIOLATION;
			switch (error->operation)
			{
				case AI_PROVIDER_OPERATION_RECEIVE_FRAME:
					message = "SemLoom provider returned an invalid frame length";
					break;
				case AI_PROVIDER_OPERATION_PARSE_JSON:
					message = "SemLoom provider returned invalid JSON";
					break;
				case AI_PROVIDER_OPERATION_RESPONSE_OBJECT:
					message = "SemLoom provider response must be a JSON object";
					break;
				case AI_PROVIDER_OPERATION_RESPONSE_FIELD:
					message = "SemLoom provider response is missing a required field";
					break;
				case AI_PROVIDER_OPERATION_RESPONSE_INTEGER:
					message = "SemLoom provider response has an invalid integer field";
					break;
				case AI_PROVIDER_OPERATION_RESPONSE_BOOLEAN:
					message = "SemLoom provider response has an invalid boolean field";
					break;
				case AI_PROVIDER_OPERATION_PROVIDER_REJECTED:
					message = "SemLoom provider rejected the protocol message";
					break;
				case AI_PROVIDER_OPERATION_OPEN_RESPONSE:
					message = "SemLoom provider open response does not match the requested protocol";
					break;
				case AI_PROVIDER_OPERATION_COMPLETION_IDENTITY:
					message = "SemLoom provider completion identity does not match the task";
					break;
				case AI_PROVIDER_OPERATION_COMPLETION_OUTPUT:
					message = "SemLoom provider completion has an invalid output";
					break;
				case AI_PROVIDER_OPERATION_COMPLETION_EVIDENCE:
					message = "SemLoom provider completion evidence digest does not match";
					break;
				default:
					message = "SemLoom provider returned an unexpected message";
					break;
			}
			break;
		default:
			break;
	}
	ereport(ERROR,
			(errcode(sqlstate),
			 errmsg("%s", message)));
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
