/*
 * PostgreSQL-private provider factory and shared adapter helpers.
 *
 * It snapshots PG configuration into an AiProvider and validates the current
 * recording spec; transport work remains in adapter-private modules.
 * Plan: experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md.
 */
#ifndef SEMLOOM_PROVIDER_PRIVATE_H
#define SEMLOOM_PROVIDER_PRIVATE_H

#include "postgres.h"

#include "utils/memutils.h"

#include "ai_provider_port.h"

#define SEMLOOM_MAP_RECORDING_SPEC_ID "semloom.recording.sem_map.text"
#define SEMLOOM_FILTER_RECORDING_SPEC_ID "semloom.recording.sem_filter.tristate"
#define SEMLOOM_RECORDING_ALGORITHM "RECORDING"
#define SEMLOOM_RECORDING_SPEC_VERSION 1
#define SEMLOOM_IN_PROCESS_PROVIDER_NAME "in-process-recording"
#define SEMLOOM_UDS_PROVIDER_NAME "uds-recording"

extern void semloom_provider_select(MemoryContext owner_context,
									AiProvider *provider);
extern bool semloom_provider_spec_is_recording(const AiOpenSpec *spec);
extern void semloom_provider_error_clear(AiProviderError *error);
extern void semloom_provider_error_set(AiProviderError *error,
									uint32 code,
									uint32 operation,
									int system_errno,
									const char *detail);
extern void semloom_recording_provider_select(AiProvider *provider);
extern void semloom_uds_provider_select(MemoryContext owner_context,
									const char *socket_path,
									AiProvider *provider);

#endif
