/*
 * PostgreSQL tuple pump shared by the current unary semantic operators.
 *
 * The pump owns child-slot flow only.  PgSemanticRuntime owns provider
 * lifecycle, sequence, completion memory, cleanup, and neutral-error mapping;
 * each operator machine owns NULL and completion interpretation.
 */
#include "postgres.h"

#include "catalog/objectaccess.h"
#include "catalog/pg_proc_d.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "executor/pg_semantic_runtime.h"
#include "planner/sem_filter_cost.h"
#include "semantics/sem_operator_machine.h"
#include "planner/sem_plan_spec.h"
#include "planner/semantic_binding.h"
#include "planner/semantic_carrier.h"
#include "catalog/pg_type_d.h"
#include "extension_config.h"
#include "utils/memutils.h"
#include "optimizer/optimizer.h"
#include "nodes/nodeFuncs.h"
#include "semantics/semantic_filter_contract.h"
#include "semantics/semantic_map_contract.h"
#include "executor/sem_pump.h"
#include "planner/marker_identity.h"

#define SEMLOOM_TRACE_ROW_ID_MAX_BYTES 256

typedef struct SemloomWindowRow
{
	MemoryContext context;
	TupleTableSlot *slot;
	AiByteSlice input;
	AiByteSlice messages;
	char *trace_row_id;
	uint64 sequence;
	bool sent;
	bool ready;
} SemloomWindowRow;

struct SemloomExecPump
{
	PlanState *child_state;
	SemloomOperatorMachine machine;
	PgSemanticRuntime *runtime;
	SemloomFilterCostEstimate filter_cost;
	bool has_filter_cost;
	SemloomTupleBinding *binding;
	AttrNumber trace_id_column;
	ExprState *input_expression;
	SemloomWindowRow *rows;
	uint32 window;
	uint32 head;
	uint32 count;
	bool returned;
	bool exhausted;
	bool offer_blocked;
	Size window_bytes;
	MemoryContext receive_context;
	MemoryContext owner_context;
};

static AiByteSlice semloom_pump_bind_text(Datum input,
										 MemoryContext task_context);
static void semloom_pump_store_completion(TupleTableSlot *slot,
										 AttrNumber result_column,
										 const PgSemanticCompletion *completion,
										 MemoryContext result_context);

static TupleTableSlot *semloom_pump_window_next(SemloomExecPump *, ScanState *);

static AttrNumber
semloom_trace_column(CustomScan *scan, const SemloomTupleBinding *binding,
	TupleDesc child_descriptor, const char *name)
{
	AttrNumber found = 0;
	ListCell *cell;
	if (name[0] == '\0') return 0;
	foreach(cell, scan->scan.plan.targetlist)
	{
		TargetEntry *entry = lfirst_node(TargetEntry, cell);
		if (entry->resname != NULL && strcmp(entry->resname, name) == 0)
		{
			Var *variable;
			AttrNumber source;
			if (found || !IsA(entry->expr, Var))
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("Map trace ID must be an unambiguous projected text column")));
			variable = (Var *) entry->expr;
			if (variable->varno != INDEX_VAR || variable->varattno < 1 ||
				variable->varattno > binding->scan_natts)
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Map trace ID lacks a scan binding")));
			source = binding->child_columns[variable->varattno - 1];
			if (source < 1 || source > child_descriptor->natts ||
				TupleDescAttr(child_descriptor, source - 1)->atttypid != TEXTOID)
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Map trace ID lacks a text child binding")));
			found = source;
		}
	}
	if (!found)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Map trace ID column is absent from child output")));
	return found;
}

static char *
semloom_trace_id(SemloomExecPump *pump, TupleTableSlot *slot, MemoryContext context)
{
	MemoryContext previous;
	Datum value;
	bool is_null;
	char *identity;
	if (pump->trace_id_column == 0) return NULL;
	value = slot_getattr(slot, pump->trace_id_column, &is_null);
	if (is_null)
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("Map trace row ID cannot be NULL")));
	previous = MemoryContextSwitchTo(context);
	identity = TextDatumGetCString(value);
	if (!identity[0] || strlen(identity) > SEMLOOM_TRACE_ROW_ID_MAX_BYTES)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Map trace row ID must contain 1 to 256 bytes")));
	MemoryContextSwitchTo(previous);
	return identity;
}

/* A first multi-in-flight path only reads plain columns/constants ahead. Other
 * expressions keep window one, so side effects and row errors are not moved. */
static bool
semloom_pump_plain_child(Plan *plan)
{
	ListCell *cell;
	if (plan == NULL) return true;
	if (IsA(plan, CustomScan) || plan->qual != NIL) return false;
	foreach(cell, plan->targetlist)
	{
		Node *expr = (Node *) lfirst_node(TargetEntry, cell)->expr;
		if (!IsA(expr, Var) && !IsA(expr, Const)) return false;
	}
	return semloom_pump_plain_child(plan->lefttree) && semloom_pump_plain_child(plan->righttree);
}

SemloomExecPump *
semloom_pump_begin(CustomScanState *node, EState *estate, int executor_flags)
{
	CustomScan *scan = castNode(CustomScan, node->ss.ps.plan);
	MemoryContext owner_context = estate->es_query_cxt;
	SemloomExecPump *pump;
	SemloomPlanSpec plan_spec;
	SemloomPlanCarrier carrier;
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

	pump = MemoryContextAllocZero(owner_context, sizeof(*pump));
	semloom_carrier_decode(scan->custom_private, owner_context, &carrier);
	plan_spec = carrier.spec;
	input_column = carrier.input_column;
	pump->has_filter_cost = carrier.has_cost;
	pump->filter_cost = carrier.cost;
	if (pump->has_filter_cost &&
		(plan_spec.operator_kind != SEMLOOM_PLAN_OPERATOR_FILTER ||
		 plan_spec.model_id == NULL ||
		 strcmp(pump->filter_cost.model_role, plan_spec.physical_role) != 0))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SemFilter cost does not match its semantic plan")));
	if (!carrier.projected_input && (input_column <= 0 ||
		input_column > node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("semantic operator input is outside the scan tuple")));

	/* Native expression setup checks input functions; the marker is checked below. */
	if (carrier.projected_input)
	{
		if (list_length(scan->custom_exprs) != 1 ||
			exprType(linitial(scan->custom_exprs)) != TEXTOID)
			elog(ERROR, "invalid projected Map input");
		pump->input_expression = ExecInitExpr(linitial(scan->custom_exprs), &node->ss.ps);
	}
	else
		ExecInitExprList(scan->custom_exprs, &node->ss.ps);
	if (plan_spec.schema_version == SEMLOOM_MAP_PLAN_SCHEMA_VERSION)
	{
		AclResult aclresult;

		if (plan_spec.marker_function_oid != semloom_generate_map_function_oid())
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("invalid generative SemMap function binding")));
		aclresult = object_aclcheck(ProcedureRelationId, plan_spec.marker_function_oid,
			GetUserId(), ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(plan_spec.marker_function_oid));
		InvokeFunctionExecuteHook(plan_spec.marker_function_oid);
	}
	if (!semloom_operator_machine_init(&pump->machine,
										   (uint32) plan_spec.operator_kind,
										   plan_spec.schema_version,
										   (const uint8 *) plan_spec.instruction,
										   plan_spec.instruction_length))
		ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unknown semantic operator machine")));
	pump->runtime = pg_semantic_runtime_begin(owner_context, &plan_spec);
	pump->child_state =
		ExecInitNode(linitial_node(Plan, scan->custom_plans), estate, executor_flags);
	pump->binding = carrier.projected_input ?
		semloom_binding_projected(carrier.binding_fields, ExecGetResultType(pump->child_state),
			node->ss.ss_ScanTupleSlot->tts_tupleDescriptor) :
		semloom_binding_legacy(input_column,
		plan_spec.operator_kind == SEMLOOM_PLAN_OPERATOR_MAP,
		ExecGetResultType(pump->child_state),
		node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
	if (plan_spec.schema_version == SEMLOOM_MAP_PLAN_SCHEMA_VERSION)
		pump->trace_id_column = semloom_trace_column(scan, pump->binding,
			ExecGetResultType(pump->child_state), semloom_test_map_binding_column());
	pump->owner_context = owner_context;
	pump->window = pg_semantic_runtime_window(pump->runtime);
	if (pump->window > 1 &&
		(!semloom_pump_plain_child(pump->child_state->plan) ||
		 (pump->input_expression != NULL && !IsA(linitial(scan->custom_exprs), Var) && !IsA(linitial(scan->custom_exprs), Const))))
		pump->window = 1;
	if (pump->window > 1)
	{
		pump->window_bytes = semloom_provider_window_bytes();
		/* Reserve row metadata and one result per slot before reading the child. */
		if (pump->window > pump->window_bytes /
			(sizeof(SemloomWindowRow) + SEMLOOM_MAP_MAX_OUTPUT_BYTES))
			ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				errmsg("semantic Map task window exceeds byte budget")));
		pump->rows = MemoryContextAllocZero(owner_context, pump->window * sizeof(SemloomWindowRow));
		pump->window_bytes -= pump->window * sizeof(SemloomWindowRow);
		pump->receive_context = AllocSetContextCreate(owner_context, "SemLoom receive", ALLOCSET_DEFAULT_SIZES);
	}
	node->custom_ps = list_make1(pump->child_state);
	return pump;
}

TupleTableSlot *
semloom_pump_next(SemloomExecPump *pump, ScanState *scan_state)
{
	TupleTableSlot *scan_slot = scan_state->ss_ScanTupleSlot;

	if (pump->rows != NULL) return semloom_pump_window_next(pump, scan_state);
	for (;;)
	{
		TupleTableSlot *child_slot = ExecProcNode(pump->child_state);
		MemoryContext tuple_context =
			scan_state->ps.ps_ExprContext->ecxt_per_tuple_memory;
		AttrNumber input_column = pump->binding->input_column;
		SemloomTupleDisposition disposition;
		Datum input_value;
		bool input_null;

		if (TupIsNull(child_slot))
			return ExecClearTuple(scan_slot);
		semloom_binding_store(pump->binding, child_slot, scan_slot);

		if (pump->input_expression != NULL)
		{
			ExecStoreVirtualTuple(scan_slot);
			scan_state->ps.ps_ExprContext->ecxt_scantuple = scan_slot;
			input_value = ExecEvalExprSwitchContext(pump->input_expression,
				scan_state->ps.ps_ExprContext, &input_null);
		}
		else
		{
			input_value = child_slot->tts_values[input_column - 1];
			input_null = child_slot->tts_isnull[input_column - 1];
		}
		if (input_null)
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
				input_value,
				tuple_context);
			SemloomBoundValue bound_input = {
				.data = input.data,
				.length = input.length,
				.is_null = false,
			};
			size_t task_length;
			uint8 *task_data = NULL;
			PgSemanticCompletion completion = {0};
			SemloomMachineCompletion machine_completion = {0};

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
									  &completion,
									  semloom_trace_id(pump, child_slot, tuple_context));
			machine_completion.data = completion.data;
			machine_completion.length = completion.length;
			machine_completion.is_null = completion.is_null;
			disposition = semloom_operator_machine_apply_completion(
				&pump->machine,
				&machine_completion);
			if (disposition == SEMLOOM_TUPLE_EMIT_COMPLETION)
			{
				semloom_pump_store_completion(scan_slot,
									  pump->binding->result_column,
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

static void
semloom_window_drop(SemloomWindowRow *row)
{
	if (row->slot != NULL) ExecDropSingleTupleTableSlot(row->slot);
	if (row->context != NULL) MemoryContextDelete(row->context);
	memset(row, 0, sizeof(*row));
}

static bool
semloom_window_read(SemloomExecPump *pump, ScanState *scan, SemloomWindowRow *row)
{
	TupleTableSlot *child = ExecProcNode(pump->child_state);
	MemoryContext previous;
	bool is_null;
	Datum value;
	SemloomBoundValue bound;
	size_t length;
	uint8 *messages;
	if (TupIsNull(child)) { pump->exhausted = true; return false; }
	row->context = AllocSetContextCreate(pump->owner_context, "SemLoom retained row", ALLOCSET_DEFAULT_SIZES);
	previous = MemoryContextSwitchTo(row->context);
	row->slot = MakeSingleTupleTableSlot(scan->ss_ScanTupleSlot->tts_tupleDescriptor, &TTSOpsVirtual);
	semloom_binding_store(pump->binding, child, row->slot);
	ExecStoreVirtualTuple(row->slot);
	if (pump->input_expression != NULL)
	{
		scan->ps.ps_ExprContext->ecxt_scantuple = row->slot;
		value = ExecEvalExprSwitchContext(pump->input_expression, scan->ps.ps_ExprContext, &is_null);
	}
	else
	{
		value = child->tts_values[pump->binding->input_column - 1];
		is_null = child->tts_isnull[pump->binding->input_column - 1];
	}
	/* Materialize before the next child pull can invalidate any borrowed Datum. */
	ExecMaterializeSlot(row->slot);
	row->ready = is_null;
	if (!is_null)
	{
		AiByteSlice borrowed = semloom_pump_bind_text(value, row->context);
		uint8 *owned;
		pg_semantic_runtime_preflight_input(pump->runtime, borrowed);
		owned = palloc(borrowed.length ? borrowed.length : 1);
		if (borrowed.length) memcpy(owned, borrowed.data, borrowed.length);
		row->input = (AiByteSlice){owned, borrowed.length};
		bound = (SemloomBoundValue){.data=row->input.data, .length=row->input.length, .is_null=false};
		length = semloom_operator_machine_task_size(&pump->machine, &bound);
		messages = palloc(length);
		if (!semloom_operator_machine_write_task(&pump->machine, &bound, messages, length))
			elog(ERROR, "could not prepare semantic operator task");
		row->messages = (AiByteSlice){messages, length};
		row->trace_row_id = semloom_trace_id(pump, child, row->context);
	}
	/* Split the configured window budget into bounded per-row reservations. */
	if (MemoryContextMemAllocated(row->context, true) + SEMLOOM_MAP_MAX_OUTPUT_BYTES > pump->window_bytes / pump->window)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("semantic row exceeds its window byte reservation")));
	MemoryContextSwitchTo(previous);
	return true;
}

static TupleTableSlot *
semloom_pump_window_next(SemloomExecPump *pump, ScanState *scan)
{
	uint32 index;
	if (pump->returned)
	{
		semloom_window_drop(&pump->rows[pump->head]);
		pump->head = (pump->head + 1) % pump->window;
		pump->count--;
		pump->returned = false;
	}
	for (;;)
	{
		uint32 in_flight = 0;
		while (!pump->exhausted && pump->count < pump->window)
		{
			SemloomWindowRow *row = &pump->rows[(pump->head + pump->count) % pump->window];
			CHECK_FOR_INTERRUPTS();
			if (!semloom_window_read(pump, scan, row)) break;
			pump->count++;
		}
		for (index = 0; index < pump->count; index++)
		{
			SemloomWindowRow *row = &pump->rows[(pump->head + index) % pump->window];
			if (!row->ready && !row->sent && !pump->offer_blocked)
			{
				row->sent = pg_semantic_runtime_offer(pump->runtime, row->input, row->messages,
					&row->sequence, row->trace_row_id);
				/* This fixed-reservation v6 path regains storage when receive
				 * transfers and releases one accepted result.  Unrelated wakes,
				 * output-slot movement and later rows cannot change that fact. */
				pump->offer_blocked = !row->sent;
			}
			if (row->sent && !row->ready) in_flight++;
		}
		if (!pump->count) return ExecClearTuple(scan->ss_ScanTupleSlot);
		if (pump->rows[pump->head].ready)
		{
			pump->returned = true;
			pg_semantic_runtime_record_emitted(pump->runtime);
			return pump->rows[pump->head].slot;
		}
		if (!in_flight)
			ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("provider rejected the entire semantic window")));
		{
			PgSemanticCompletion completion;
			uint64 sequence;
			MemoryContextReset(pump->receive_context);
			sequence = pg_semantic_runtime_receive(pump->runtime, pump->receive_context, &completion);
			pump->offer_blocked = false;
			for (index = 0; index < pump->count; index++)
			{
				SemloomWindowRow *row = &pump->rows[(pump->head + index) % pump->window];
				if (row->sent && !row->ready && row->sequence == sequence)
				{
					semloom_pump_store_completion(row->slot, pump->binding->result_column, &completion, row->context);
					row->ready = true;
					break;
				}
			}
			if (index == pump->count) elog(ERROR, "unmatched semantic completion");
		}
	}
}

void
semloom_pump_stop(SemloomExecPump *pump, CustomScanState *node)
{
	if (pump == NULL)
		return;
	pg_semantic_runtime_close(pump->runtime);
	if (pump->rows != NULL)
	{
		uint32 index;
		for (index = 0; index < pump->window; index++) semloom_window_drop(&pump->rows[index]);
	}
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
	if (pg_semantic_runtime_window(pump->runtime))
		ExplainPropertyInteger("Semantic Input Window", NULL, pump->window, explain_state);
	if (pump->has_filter_cost)
		semloom_filter_cost_explain(&pump->filter_cost, explain_state);
	if (pump->input_expression != NULL)
	{
		ExplainPropertyText("Input Binding", "expression", explain_state);
		ExplainPropertyInteger("Result Column", NULL, pump->binding->result_column, explain_state);
	}
	else
		ExplainPropertyInteger(
		semloom_operator_machine_explain_property(&pump->machine),
		NULL,
		pump->binding->input_column,
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
							  AttrNumber result_column,
							  const PgSemanticCompletion *completion,
							  MemoryContext result_context)
{
	const char *output_data;
	MemoryContext previous_context;
	text *output_text = NULL;

	if (completion->is_null)
	{
		slot->tts_isnull[result_column - 1] = true;
		slot->tts_values[result_column - 1] = (Datum) 0;
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
	slot->tts_isnull[result_column - 1] = false;
	slot->tts_values[result_column - 1] = PointerGetDatum(output_text);
}
