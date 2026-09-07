/* Pure values and canonical encoding for the fixed generative SemMap contract. */
#ifndef SEMLOOM_SEMANTIC_MAP_CONTRACT_H
#define SEMLOOM_SEMANTIC_MAP_CONTRACT_H

#include "semantics/sem_operator_machine.h"

#define SEMLOOM_MAP_MAX_INSTRUCTION_BYTES 4096
#define SEMLOOM_MAP_MAX_INPUT_BYTES 163840
#define SEMLOOM_MAP_MAX_OUTPUT_BYTES 65536
#define SEMLOOM_MAP_MAX_MODEL_BYTES 128
#define SEMLOOM_MAP_MAX_GENERATION_TOKENS 4096
#define SEMLOOM_MAP_MAX_FINISH_REASON_BYTES 32
#define SEMLOOM_MAP_PLAN_SCHEMA_VERSION 4

#define SEMLOOM_MAP_SPEC_ID "semloom.semantic.sem_map.generate.v1"
#define SEMLOOM_MAP_PROMPT_PROGRAM_ID "semloom.sem_map.chat.v1"
#define SEMLOOM_MAP_RESULT_PARSER_ID "semloom.sem_map.utf8_text.v1"
#define SEMLOOM_MAP_PROMPT_PROGRAM_DIGEST \
	"72bbbd2abec0c7167158200281b7a88c44b94cd949f8b63f398a9101f8826afb"
#define SEMLOOM_MAP_RESULT_PARSER_DIGEST \
	"540ea50c27d6f2d6800146b3b26404b4a5a64c6debef02e5501e67a829caec07"

typedef struct SemloomMapPlanValues
{
	SemloomBoundValue instruction;
	SemloomBoundValue model_id;
	uint32_t max_tokens;
} SemloomMapPlanValues;

typedef enum SemloomMapCompletionStatus
{
	SEMLOOM_MAP_COMPLETION_VALID = 0,
	SEMLOOM_MAP_COMPLETION_INVALID = 1,
	SEMLOOM_MAP_COMPLETION_TOO_LARGE = 2,
	SEMLOOM_MAP_COMPLETION_INCOMPLETE = 3,
} SemloomMapCompletionStatus;

/* Canonical bytes are hashed by the caller; no cryptographic library here.
 * NULL output with zero capacity measures the complete value. Otherwise capacity
 * must hold the entire encoding; failure leaves output unchanged and written zero.
 * Borrowed input, output and written storage must not overlap. No terminator.
 */
extern bool semloom_map_plan_encode(const SemloomMapPlanValues *plan,
								  uint8_t *output, size_t capacity, size_t *written);

/* Representation/model/usage precede the length and finish policies. */
extern uint32_t semloom_map_completion_status(const SemloomMapPlanValues *plan,
											const SemloomMachineCompletion *completion);

#endif
