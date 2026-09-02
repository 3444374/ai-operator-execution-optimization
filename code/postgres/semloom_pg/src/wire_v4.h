/* UDS-private interface for choice wire version 4. */
#ifndef SEMLOOM_WIRE_V4_H
#define SEMLOOM_WIRE_V4_H
#include "wire_semantic.h"
#define SEMLOOM_WIRE_V4_PROTOCOL_VERSION 4
#define SEMLOOM_WIRE_V4_MAX_FRAME_BYTES SEMLOOM_WIRE_SEMANTIC_MAX_FRAME_BYTES
#define SEMLOOM_WIRE_V4_MAX_INPUT_BYTES SEMLOOM_WIRE_SEMANTIC_MAX_INPUT_BYTES
typedef SemloomWireSemanticIdentity SemloomWireV4Identity;
extern void semloom_wire_v4_identity_init(const AiOpenSpec *spec,
	const char *provider_execution_id, SemloomWireV4Identity *identity);
extern AiProviderStatus semloom_wire_v4_open(pgsocket socket_fd,
	const AiOpenSpec *spec, const SemloomWireV4Identity *identity,
	AiProviderError *error);
extern AiProviderStatus semloom_wire_v4_drive(pgsocket socket_fd,
	const AiOpenSpec *spec, const AiPreparedTask *task,
	const SemloomWireV4Identity *identity, AiCompletion *completion,
	AiProviderError *error);
#endif
