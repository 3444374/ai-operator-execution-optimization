/* UDS-private interface for exact semantic wire version 3. */
#ifndef SEMLOOM_WIRE_V3_H
#define SEMLOOM_WIRE_V3_H

#include "postgres.h"

#include "ai_provider_port.h"
#include "semantic_filter_contract.h"
#include "wire_common.h"
#include "wire_semantic.h"

#define SEMLOOM_WIRE_V3_PROTOCOL_VERSION 3
#define SEMLOOM_WIRE_V3_MAX_FRAME_BYTES SEMLOOM_WIRE_COMMON_MAX_FRAME_BYTES
#define SEMLOOM_WIRE_V3_MAX_INPUT_BYTES 163840

typedef SemloomWireSemanticIdentity SemloomWireV3Identity;

extern void semloom_wire_v3_identity_init(const AiOpenSpec *spec,
										  const char *provider_execution_id,
										  SemloomWireV3Identity *identity);
extern AiProviderStatus semloom_wire_v3_open(
	pgsocket socket_fd,
	const AiOpenSpec *spec,
	const SemloomWireV3Identity *identity,
	AiProviderError *error);
extern AiProviderStatus semloom_wire_v3_drive(
	pgsocket socket_fd,
	const AiOpenSpec *spec,
	const AiPreparedTask *task,
	const SemloomWireV3Identity *identity,
	AiCompletion *completion,
	AiProviderError *error);

#endif
