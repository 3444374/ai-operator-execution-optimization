/*
 * UDS-private interface for recording wire version 2.
 *
 * The adapter supplies a connected PostgreSQL socket and neutral values; this
 * layer returns validated completions/errors under the fixed frame limits.
 * Plan: experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md.
 */
#ifndef SEMLOOM_WIRE_V2_H
#define SEMLOOM_WIRE_V2_H

#include "postgres.h"

#include "provider/ai_provider_port.h"
#include "provider/wire/wire_common.h"

#define SEMLOOM_WIRE_V2_PROTOCOL_VERSION 2
#define SEMLOOM_WIRE_V2_MAX_FRAME_BYTES SEMLOOM_WIRE_COMMON_MAX_FRAME_BYTES
#define SEMLOOM_WIRE_V2_MAX_INPUT_BYTES ((SEMLOOM_WIRE_V2_MAX_FRAME_BYTES - 4096) / 6)
#define SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH 64

typedef struct SemloomWireV2Identity
{
	const char *provider_execution_id;
	char semantic_spec_digest[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1];
	char physical_algorithm_digest[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1];
	char provider_execution_digest[SEMLOOM_WIRE_V2_SHA256_HEX_LENGTH + 1];
} SemloomWireV2Identity;

extern void semloom_wire_v2_identity_init(const AiOpenSpec *spec,
										  const char *provider_execution_id,
										  SemloomWireV2Identity *identity);
extern AiProviderStatus semloom_wire_v2_open(pgsocket socket_fd,
										 const AiOpenSpec *spec,
										 const SemloomWireV2Identity *identity,
										 AiProviderError *error);
extern AiProviderStatus semloom_wire_v2_drive(pgsocket socket_fd,
										  const AiPreparedTask *task,
										  const SemloomWireV2Identity *identity,
										  AiCompletion *completion,
										  AiProviderError *error);
#endif
