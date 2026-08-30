/* Operator-specific tuple semantics above PgSemanticRuntime. */
#ifndef SEMLOOM_SEM_OPERATOR_MACHINE_H
#define SEMLOOM_SEM_OPERATOR_MACHINE_H

#include "postgres.h"

#include "commands/explain_state.h"
#include "executor/tuptable.h"

#include "ai_provider_port.h"
#include "pg_semantic_runtime.h"
#include "sem_plan_spec.h"

typedef enum SemloomTupleDisposition
{
	SEMLOOM_TUPLE_EMIT = 1,
	SEMLOOM_TUPLE_DROP = 2,
	SEMLOOM_TUPLE_INVALID_COMPLETION = 3,
} SemloomTupleDisposition;

typedef struct SemloomOperatorMachineMethods
{
	const char *input_explain_property;
	const char *invalid_completion_message;
	SemloomTupleDisposition (*handle_null)(TupleTableSlot *slot,
												 AttrNumber input_column);
	SemloomTupleDisposition (*apply_completion)(
		TupleTableSlot *slot,
		AttrNumber input_column,
		const PgSemanticCompletion *completion,
		MemoryContext result_context);
} SemloomOperatorMachineMethods;

typedef struct SemloomOperatorMachine
{
	const SemloomOperatorMachineMethods *methods;
	AttrNumber input_column;
} SemloomOperatorMachine;

extern const SemloomOperatorMachineMethods semloom_map_machine_methods;
extern const SemloomOperatorMachineMethods semloom_filter_machine_methods;

extern void semloom_operator_machine_init(SemloomOperatorMachine *machine,
										  const SemloomPlanSpec *plan_spec,
										  AttrNumber input_column);
extern AiByteSlice semloom_operator_machine_bind_text(
	const SemloomOperatorMachine *machine,
	Datum input,
	MemoryContext task_context);
extern SemloomTupleDisposition semloom_operator_machine_handle_null(
	const SemloomOperatorMachine *machine,
	TupleTableSlot *slot);
extern SemloomTupleDisposition semloom_operator_machine_apply_completion(
	const SemloomOperatorMachine *machine,
	TupleTableSlot *slot,
	const PgSemanticCompletion *completion,
	MemoryContext result_context);
extern void semloom_operator_machine_explain(
	const SemloomOperatorMachine *machine,
	ExplainState *explain_state);
pg_noreturn extern void semloom_operator_machine_raise_invalid_completion(
	const SemloomOperatorMachine *machine);

#endif
