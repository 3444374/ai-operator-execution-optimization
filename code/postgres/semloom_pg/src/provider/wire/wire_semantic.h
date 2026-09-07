/* UDS-private fixed exact-Filter codecs above shared framing. */
#ifndef SEMLOOM_WIRE_SEMANTIC_H
#define SEMLOOM_WIRE_SEMANTIC_H
#include "postgres.h"
#include "provider/ai_provider_port.h"
#include "semantics/semantic_filter_contract.h"
#include "provider/wire/wire_common.h"
#define SEMLOOM_WIRE_SEMANTIC_MAX_FRAME_BYTES SEMLOOM_WIRE_COMMON_MAX_FRAME_BYTES
#define SEMLOOM_WIRE_SEMANTIC_MAX_INPUT_BYTES 163840
typedef struct SemloomWireSemanticIdentity
{
	uint32 protocol_version;
	const char *provider_execution_id;
	char semantic_spec_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
	char physical_algorithm_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
	char provider_execution_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
	char generation_profile_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
} SemloomWireSemanticIdentity;
extern void semloom_wire_semantic_identity_init(const AiOpenSpec *spec,
	const char *provider_execution_id, uint32 protocol_version,
	SemloomWireSemanticIdentity *identity);
extern AiProviderStatus semloom_wire_semantic_open(pgsocket socket_fd,
	const AiOpenSpec *spec, const SemloomWireSemanticIdentity *identity,
	AiProviderError *error);
extern AiProviderStatus semloom_wire_semantic_drive(pgsocket socket_fd,
	const AiOpenSpec *spec, const AiPreparedTask *task,
	const SemloomWireSemanticIdentity *identity, AiCompletion *completion,
	AiProviderError *error);
#endif
