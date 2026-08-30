/*
 * PostgreSQL tuple pump shared by the current unary semantic operators.
 *
 * The pump owns child-slot flow only.  PgSemanticRuntime owns provider
 * lifecycle, sequence, completion memory, cleanup, and neutral-error mapping;
 * each operator machine owns NULL and completion interpretation.
 */
#include "postgres.h"

#include "executor/executor.h"
#include "miscadmin.h"

#include "pg_semantic_runtime.h"
#include "sem_operator_machine.h"
#include "sem_plan_spec.h"
#include "sem_pump.h"

struct SemloomExecPump
{
	PlanState *child_state;
	SemloomOperatorMachine machine;
	PgSemanticRuntime *runtime;
};

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
	semloom_operator_machine_init(&pump->machine,
									  &plan_spec,
									  input_column);
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
		AttrNumber input_column = pump->machine.input_column;
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
			disposition = semloom_operator_machine_handle_null(&pump->machine,
																 scan_slot);
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
			AiByteSlice input = semloom_operator_machine_bind_text(
				&pump->machine,
				scan_slot->tts_values[input_column - 1],
				tuple_context);
			PgSemanticCompletion completion = {0};

			pg_semantic_runtime_drive(pump->runtime,
									  input,
									  tuple_context,
									  &completion);
			disposition = semloom_operator_machine_apply_completion(
				&pump->machine,
				scan_slot,
				&completion,
				tuple_context);
		}

		if (disposition == SEMLOOM_TUPLE_INVALID_COMPLETION)
		{
			pg_semantic_runtime_close(pump->runtime);
			semloom_operator_machine_raise_invalid_completion(&pump->machine);
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
	semloom_operator_machine_explain(&pump->machine, explain_state);
	pg_semantic_runtime_explain_counters(pump->runtime, explain_state);
}
