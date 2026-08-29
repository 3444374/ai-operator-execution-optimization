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

#include "ai_provider_port.h"

typedef struct PgSemanticRuntime PgSemanticRuntime;

typedef struct PgSemanticCompletion
{
	const uint8 *data;
	uint32 length;
	bool is_null;
} PgSemanticCompletion;

extern PgSemanticRuntime *pg_semantic_runtime_begin(
	MemoryContext owner_context,
	const AiOpenSpec *open_spec);
extern void pg_semantic_runtime_drive(PgSemanticRuntime *runtime,
										  AiByteSlice input,
										  MemoryContext result_context,
										  PgSemanticCompletion *completion);
extern void pg_semantic_runtime_record_emitted(PgSemanticRuntime *runtime);
extern void pg_semantic_runtime_close(PgSemanticRuntime *runtime);
extern void pg_semantic_runtime_explain(const PgSemanticRuntime *runtime,
										ExplainState *explain_state);
extern void pg_semantic_runtime_explain_counters(
	const PgSemanticRuntime *runtime,
	ExplainState *explain_state);

#endif
