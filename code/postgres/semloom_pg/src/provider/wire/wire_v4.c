/* Version-4 entry points for the fixed exact-Filter wire contract. */
#include "postgres.h"
#include "provider/provider_private.h"
#include "provider/wire/wire_v4.h"
void
semloom_wire_v4_identity_init(const AiOpenSpec *spec,
	const char *provider_execution_id, SemloomWireV4Identity *identity)
{
	semloom_wire_semantic_identity_init(spec, provider_execution_id,
		SEMLOOM_WIRE_V4_PROTOCOL_VERSION, identity);
}
AiProviderStatus
semloom_wire_v4_open(pgsocket socket_fd, const AiOpenSpec *spec,
	const SemloomWireV4Identity *identity, AiProviderError *error)
{
	if (spec->has_generation_profile != true ||
		identity->protocol_version != SEMLOOM_WIRE_V4_PROTOCOL_VERSION)
	{
		semloom_provider_error_set(error, AI_PROVIDER_ERROR_INVALID_SPEC, 0, 0, NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	return semloom_wire_semantic_open(socket_fd, spec, identity, error);
}
AiProviderStatus
semloom_wire_v4_drive(pgsocket socket_fd, const AiOpenSpec *spec,
	const AiPreparedTask *task, const SemloomWireV4Identity *identity,
	AiCompletion *completion, AiProviderError *error)
{
	if (spec->has_generation_profile != true ||
		identity->protocol_version != SEMLOOM_WIRE_V4_PROTOCOL_VERSION)
	{
		semloom_provider_error_set(error, AI_PROVIDER_ERROR_INVALID_SPEC, 0, 0, NULL);
		return AI_PROVIDER_STATUS_ERROR;
	}
	return semloom_wire_semantic_drive(socket_fd, spec, task, identity, completion, error);
}
