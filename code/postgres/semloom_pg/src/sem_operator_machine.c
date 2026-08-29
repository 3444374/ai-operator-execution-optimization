/* Shared binding and dispatch for the proven SemMap/SemFilter machines. */
#include "postgres.h"

#include "commands/explain_format.h"
#include "utils/builtins.h"

#include "provider_private.h"
#include "sem_operator_machine.h"

void
semloom_operator_machine_init(SemloomOperatorMachine *machine,
							  uint32 operator_kind,
							  AttrNumber input_column)
{
	Assert(machine != NULL);
	MemSet(machine, 0, sizeof(*machine));
	if (operator_kind == AI_PROVIDER_OPERATOR_MAP)
		machine->methods = &semloom_map_machine_methods;
	else if (operator_kind == AI_PROVIDER_OPERATOR_FILTER)
		machine->methods = &semloom_filter_machine_methods;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown semantic operator machine")));

	machine->input_column = input_column;
	machine->open_spec.operator_kind = machine->methods->operator_kind;
	machine->open_spec.input_value_kind = AI_PROVIDER_VALUE_TEXT;
	machine->open_spec.output_value_kind = machine->methods->output_value_kind;
	machine->open_spec.null_policy = AI_PROVIDER_NULL_PROPAGATE;
	machine->open_spec.error_policy = AI_PROVIDER_ERROR_FAIL_QUERY;
	machine->open_spec.semantic_spec_version = SEMLOOM_RECORDING_SPEC_VERSION;
	machine->open_spec.semantic_spec_id.data =
		(const uint8 *) machine->methods->semantic_spec_id;
	machine->open_spec.semantic_spec_id.length =
		machine->methods->semantic_spec_id_length;
	machine->open_spec.physical_algorithm.data =
		(const uint8 *) SEMLOOM_RECORDING_ALGORITHM;
	machine->open_spec.physical_algorithm.length =
		sizeof(SEMLOOM_RECORDING_ALGORITHM) - 1;
}

const AiOpenSpec *
semloom_operator_machine_open_spec(const SemloomOperatorMachine *machine)
{
	Assert(machine != NULL);
	Assert(machine->methods != NULL);
	return &machine->open_spec;
}

AiByteSlice
semloom_operator_machine_bind_text(const SemloomOperatorMachine *machine,
								   Datum input,
								   MemoryContext task_context)
{
	MemoryContext previous_context;
	text *input_text = NULL;
	Size input_length;
	AiByteSlice input_slice;

	Assert(machine != NULL);
	Assert(machine->methods != NULL);
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

SemloomTupleDisposition
semloom_operator_machine_handle_null(const SemloomOperatorMachine *machine,
									 TupleTableSlot *slot)
{
	Assert(machine != NULL);
	Assert(machine->methods != NULL);
	return machine->methods->handle_null(slot, machine->input_column);
}

SemloomTupleDisposition
semloom_operator_machine_apply_completion(
	const SemloomOperatorMachine *machine,
	TupleTableSlot *slot,
	const PgSemanticCompletion *completion,
	MemoryContext result_context)
{
	Assert(machine != NULL);
	Assert(machine->methods != NULL);
	return machine->methods->apply_completion(slot,
											 machine->input_column,
											 completion,
											 result_context);
}

void
semloom_operator_machine_explain(const SemloomOperatorMachine *machine,
								 ExplainState *explain_state)
{
	Assert(machine != NULL);
	Assert(machine->methods != NULL);
	ExplainPropertyInteger(machine->methods->input_explain_property,
							   NULL,
							   machine->input_column,
							   explain_state);
}

void
semloom_operator_machine_raise_invalid_completion(
	const SemloomOperatorMachine *machine)
{
	Assert(machine != NULL);
	Assert(machine->methods != NULL);
	Assert(machine->methods->invalid_completion_message != NULL);
	ereport(ERROR,
			(errcode(ERRCODE_DATA_EXCEPTION),
			 errmsg("%s", machine->methods->invalid_completion_message)));
	pg_unreachable();
}
