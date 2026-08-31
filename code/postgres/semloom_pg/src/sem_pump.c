/*
 * PostgreSQL tuple pump shared by the current unary semantic operators.
 *
 * The pump owns child-slot flow only.  PgSemanticRuntime owns provider
 * lifecycle, sequence, completion memory, cleanup, and neutral-error mapping;
 * each operator machine owns NULL and completion interpretation.
 */
#include "postgres.h"

#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/builtins.h"

#include "pg_semantic_runtime.h"
#include "sem_operator_machine.h"
#include "sem_plan_spec.h"
#include "sem_pump.h"

struct SemloomExecPump
{
	PlanState *child_state;
	SemloomOperatorMachine machine;
	PgSemanticRuntime *runtime;
	AttrNumber input_column;
};

static AiByteSlice semloom_pump_bind_text(Datum input,
										 MemoryContext task_context);
static void semloom_pump_store_completion(TupleTableSlot *slot,
										 AttrNumber input_column,
										 const PgSemanticCompletion *completion,
										 MemoryContext result_context);

SemloomExecPump *
semloom_pump_begin(CustomScanState *node, EState *estate, int executor_flags)
{
	CustomScan *scan = castNode(CustomScan, node->ss.ps.plan);
	MemoryContext owner_context = estate->es_query_cxt;
	SemloomExecPump *pump;
	SemloomPlanSpec plan_spec;
	AttrNumber input_column;
	int unsupported_flags = EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK | EXEC_FLAG_REWIND;

	if ((executor_flags & unsupported_flags) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("semantic operator capability supports forward execution only")));
	if (list_length(scan->custom_plans) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid semantic operator executor state")));

	semloom_plan_spec_decode(scan->custom_private,
		owner_context,
		&plan_spec,
		&input_column);
	if (input_column <= 0 ||
		input_column > node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("semantic operator input is outside the scan tuple")));

	pump = MemoryContextAllocZero(owner_context, sizeof(*pump));
	if (!semloom_operator_machine_init(&pump->machine,
									   (uint32) plan_spec.operator_kind,
									   plan_spec.schema_version,
									   (const uint8 *) plan_spec.instruction,
									   plan_spec.instruction_length))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown semantic operator machine")));
	pump->input_column = input_column;
	pump->runtime = pg_semantic_runtime_begin(owner_context, &plan_spec);
	pump->child_state =
		ExecInitNode(linitial_node(Plan, scan->custom_plans), estate, executor_flags);
	node->custom_ps = list_make1(pump->child_state);
	return pump;
}

TupleTableSlot *
semloom_pump_next(SemloomExecPump *pump, ScanState *scan_state)
{
	TupleTableSlot *scan_slot = scan_state->ss_ScanTupleSlot;

	for (;;)
	{
		TupleTableSlot *child_slot = ExecProcNode(pump->child_state);
		MemoryContext tuple_context =
			scan_state->ps.ps_ExprContext->ecxt_per_tuple_memory;
		AttrNumber input_column = pump->input_column;
		SemloomTupleDisposition disposition;
		int attribute_index;

		if (TupIsNull(child_slot))
			return ExecClearTuple(scan_slot);
		slot_getallattrs(child_slot);
		if (child_slot->tts_tupleDescriptor->natts !=
			scan_slot->tts_tupleDescriptor->natts)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("semantic child and scan tuple descriptors do not match")));

		ExecClearTuple(scan_slot);
		for (attribute_index = 0;
			 attribute_index < scan_slot->tts_tupleDescriptor->natts;
			 attribute_index++)
		{
			scan_slot->tts_isnull[attribute_index] =
				child_slot->tts_isnull[attribute_index];
			scan_slot->tts_values[attribute_index] =
				child_slot->tts_values[attribute_index];
		}

		if (scan_slot->tts_isnull[input_column - 1])
		{
			disposition = semloom_operator_machine_handle_null(&pump->machine);
			if (disposition == SEMLOOM_TUPLE_EMIT)
				return ExecStoreVirtualTuple(scan_slot);
			Assert(disposition == SEMLOOM_TUPLE_DROP);
			ExecClearTuple(scan_slot);
			ResetExprContext(scan_state->ps.ps_ExprContext);
			CHECK_FOR_INTERRUPTS();
			continue;
		}
		else
		{
			AiByteSlice input = semloom_pump_bind_text(
				scan_slot->tts_values[input_column - 1],
				tuple_context);
			SemloomBoundValue bound_input = {
				.data = input.data,
				.length = input.length,
				.is_null = false,
			};
			size_t task_length;
			uint8 *task_data = NULL;
			PgSemanticCompletion completion = {0};
			SemloomMachineCompletion machine_completion;

			pg_semantic_runtime_preflight_input(pump->runtime, input);
			task_length = semloom_operator_machine_task_size(
				&pump->machine,
				&bound_input);
			if (task_length > 0)
			{
				task_data = MemoryContextAlloc(tuple_context, task_length);
				if (!semloom_operator_machine_write_task(&pump->machine,
												  &bound_input,
												  task_data,
												  task_length))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("could not prepare semantic operator task")));
			}

			pg_semantic_runtime_drive(pump->runtime,
									  input,
									  (AiByteSlice) {
										  .data = task_data,
										  .length = (uint32) task_length,
									  },
									  tuple_context,
									  &completion);
			machine_completion.data = completion.data;
			machine_completion.length = completion.length;
			machine_completion.is_null = completion.is_null;
			disposition = semloom_operator_machine_apply_completion(
				&pump->machine,
				&machine_completion);
			if (disposition == SEMLOOM_TUPLE_EMIT_COMPLETION)
			{
				semloom_pump_store_completion(scan_slot,
									  input_column,
									  &completion,
									  tuple_context);
				disposition = SEMLOOM_TUPLE_EMIT;
			}
		}

		if (disposition == SEMLOOM_TUPLE_INVALID_COMPLETION)
		{
			pg_semantic_runtime_close(pump->runtime);
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("%s",
							semloom_operator_machine_invalid_message(&pump->machine))));
		}
		if (disposition == SEMLOOM_TUPLE_EMIT)
		{
			pg_semantic_runtime_record_emitted(pump->runtime);
			return ExecStoreVirtualTuple(scan_slot);
		}
		Assert(disposition == SEMLOOM_TUPLE_DROP);
		ExecClearTuple(scan_slot);
		ResetExprContext(scan_state->ps.ps_ExprContext);
		CHECK_FOR_INTERRUPTS();
	}
}

void
semloom_pump_stop(SemloomExecPump *pump, CustomScanState *node)
{
	if (pump == NULL)
		return;
	pg_semantic_runtime_close(pump->runtime);
	if (pump->child_state != NULL)
	{
		ExecEndNode(pump->child_state);
		pump->child_state = NULL;
	}
	node->custom_ps = NIL;
}

void
semloom_pump_explain(const SemloomExecPump *pump, ExplainState *explain_state)
{
	pg_semantic_runtime_explain(pump->runtime, explain_state);
	ExplainPropertyInteger(
		semloom_operator_machine_explain_property(&pump->machine),
		NULL,
		pump->input_column,
		explain_state);
	pg_semantic_runtime_explain_counters(pump->runtime, explain_state);
}

static AiByteSlice
semloom_pump_bind_text(Datum input, MemoryContext task_context)
{
	MemoryContext previous_context;
	text *input_text = NULL;
	Size input_length;
	AiByteSlice input_slice;

	previous_context = MemoryContextSwitchTo(task_context);
	PG_TRY();
	{
		input_text = DatumGetTextPP(input);
		MemoryContextSwitchTo(previous_context);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(previous_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	input_length = VARSIZE_ANY_EXHDR(input_text);
	Assert(input_length <= PG_UINT32_MAX);
	input_slice.data = (const uint8 *) VARDATA_ANY(input_text);
	input_slice.length = (uint32) input_length;
	return input_slice;
}

static void
semloom_pump_store_completion(TupleTableSlot *slot,
							  AttrNumber input_column,
							  const PgSemanticCompletion *completion,
							  MemoryContext result_context)
{
	const char *output_data;
	MemoryContext previous_context;
	text *output_text = NULL;

	if (completion->is_null)
	{
		slot->tts_isnull[input_column - 1] = true;
		slot->tts_values[input_column - 1] = (Datum) 0;
		return;
	}
	output_data = completion->length == 0 ? "" : (const char *) completion->data;
	previous_context = MemoryContextSwitchTo(result_context);
	PG_TRY();
	{
		output_text = cstring_to_text_with_len(output_data, completion->length);
		MemoryContextSwitchTo(previous_context);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(previous_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	slot->tts_isnull[input_column - 1] = false;
	slot->tts_values[input_column - 1] = PointerGetDatum(output_text);
}
