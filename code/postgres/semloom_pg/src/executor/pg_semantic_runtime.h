/*
 * PostgreSQL-private lifecycle shared by unary semantic operators.
 *
 * The runtime fixes one provider for a query, opens it lazily, owns task
 * sequence and cleanup, copies session-owned completions into a caller-owned
 * result context, and maps neutral provider failures to PostgreSQL errors.
 */
#ifndef SEMLOOM_PG_SEMANTIC_RUNTIME_H
#define SEMLOOM_PG_SEMANTIC_RUNTIME_H

#include "postgres.h"

#include "commands/explain_state.h"
#include "utils/memutils.h"

#include "provider/ai_provider_port.h"
#include "planner/sem_plan_spec.h"

typedef struct PgSemanticRuntime PgSemanticRuntime;

typedef struct PgSemanticCompletion
{
	const uint8 *data;
	uint32 length;
	bool is_null;
} PgSemanticCompletion;

extern PgSemanticRuntime *pg_semantic_runtime_begin(
	MemoryContext owner_context,
	const SemloomPlanSpec *plan_spec);
extern void pg_semantic_runtime_preflight_input(PgSemanticRuntime *runtime,
												 AiByteSlice input);
extern void pg_semantic_runtime_drive(PgSemanticRuntime *runtime,
										  AiByteSlice input,
										  AiByteSlice canonical_messages,
										  MemoryContext result_context,
										  PgSemanticCompletion *completion,
										  const char *trace_row_id);
extern void pg_semantic_runtime_record_emitted(PgSemanticRuntime *runtime);
extern void pg_semantic_runtime_trace_filter_result(PgSemanticRuntime *runtime, bool kept);
extern void pg_semantic_runtime_close(PgSemanticRuntime *runtime);
extern void pg_semantic_runtime_explain(const PgSemanticRuntime *runtime,
										ExplainState *explain_state);
extern void pg_semantic_runtime_explain_counters(
	const PgSemanticRuntime *runtime,
	ExplainState *explain_state);

extern uint32 pg_semantic_runtime_window(const PgSemanticRuntime *runtime);
extern bool pg_semantic_runtime_offer(PgSemanticRuntime *runtime, AiByteSlice input,
	AiByteSlice messages, uint64 *sequence, const char *trace_row_id);
extern uint64 pg_semantic_runtime_receive(PgSemanticRuntime *runtime, MemoryContext context,
	PgSemanticCompletion *completion);
#endif
