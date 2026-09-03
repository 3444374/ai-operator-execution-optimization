/* PostgreSQL-independent unary operator result interpretation. */
#ifndef SEMLOOM_SEM_OPERATOR_MACHINE_H
#define SEMLOOM_SEM_OPERATOR_MACHINE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum SemloomTupleDisposition
{
	SEMLOOM_TUPLE_EMIT = 1,
	SEMLOOM_TUPLE_DROP = 2,
	SEMLOOM_TUPLE_EMIT_COMPLETION = 3,
	SEMLOOM_TUPLE_INVALID_COMPLETION = 4,
} SemloomTupleDisposition;

typedef struct SemloomMachineCompletion
{
	const uint8_t *data;
	uint32_t length;
	bool is_null;
} SemloomMachineCompletion;

typedef struct SemloomBoundValue
{
	const uint8_t *data;
	uint32_t length;
	bool is_null;
} SemloomBoundValue;

struct SemloomOperatorMachine;

typedef struct SemloomOperatorMachineMethods
{
	const char *input_explain_property;
	SemloomTupleDisposition (*handle_null)(void);
	SemloomTupleDisposition (*apply_completion)(
		const SemloomMachineCompletion *completion);
	bool (*build_task)(const struct SemloomOperatorMachine *machine,
					   const SemloomBoundValue *input,
					   uint8_t *destination,
					   size_t destination_length,
					   size_t *written_length);
} SemloomOperatorMachineMethods;

typedef struct SemloomOperatorMachine
{
	const SemloomOperatorMachineMethods *methods;
	uint32_t plan_schema_version;
	const uint8_t *instruction;
	uint32_t instruction_length;
	const char *invalid_completion_message;
} SemloomOperatorMachine;

extern const SemloomOperatorMachineMethods semloom_map_machine_methods;
extern const SemloomOperatorMachineMethods semloom_filter_recording_machine_methods;
extern const SemloomOperatorMachineMethods semloom_filter_exact_machine_methods;

/* Compile a Map task only; does not select or enable an execution machine.
 * Inputs are borrowed, length-delimited text with separate SQL NULL flags.
 * Size is zero for invalid/NULL input or instruction. Write requires an exact
 * non-overlapping destination; failure leaves it unchanged. No terminator is
 * appended. The caller retains all input and destination storage.
 */
extern size_t semloom_map_task_size(const SemloomBoundValue *instruction,
								   const SemloomBoundValue *input);
extern bool semloom_map_write_task(const SemloomBoundValue *instruction,
								  const SemloomBoundValue *input,
								  uint8_t *destination,
								  size_t destination_length);

extern bool semloom_operator_machine_init(SemloomOperatorMachine *machine,
										 uint32_t operator_kind,
										 uint32_t plan_schema_version,
										 const uint8_t *instruction,
										 uint32_t instruction_length);
extern size_t semloom_operator_machine_task_size(
	const SemloomOperatorMachine *machine,
	const SemloomBoundValue *input);
extern bool semloom_operator_machine_write_task(
	const SemloomOperatorMachine *machine,
	const SemloomBoundValue *input,
	uint8_t *destination,
	size_t destination_length);
extern SemloomTupleDisposition semloom_operator_machine_handle_null(
	const SemloomOperatorMachine *machine);
extern SemloomTupleDisposition semloom_operator_machine_apply_completion(
	const SemloomOperatorMachine *machine,
	const SemloomMachineCompletion *completion);
extern const char *semloom_operator_machine_explain_property(
	const SemloomOperatorMachine *machine);
extern const char *semloom_operator_machine_invalid_message(
	const SemloomOperatorMachine *machine);

#endif
