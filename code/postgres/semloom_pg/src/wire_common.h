/* Shared bounded frame and PostgreSQL JSON primitives for versioned UDS wires. */
#ifndef SEMLOOM_WIRE_COMMON_H
#define SEMLOOM_WIRE_COMMON_H

#include "postgres.h"

#include "utils/jsonb.h"

#include "ai_provider_port.h"

extern AiProviderStatus semloom_wire_common_send_frame(
	pgsocket socket_fd,
	const char *payload,
	Size payload_length,
	AiProviderError *error);
extern AiProviderStatus semloom_wire_common_receive_frame(
	pgsocket socket_fd,
	char **payload,
	AiProviderError *error);
extern AiProviderStatus semloom_wire_common_parse_json(
	const char *payload,
	Jsonb **message,
	AiProviderError *error);
extern bool semloom_wire_common_json_value(Jsonb *message,
										   const char *key,
										   JsonbValue **value,
										   AiProviderError *error);
extern bool semloom_wire_common_json_string_equals(Jsonb *message,
											   const char *key,
											   const char *expected,
											   bool *matches,
											   AiProviderError *error);
extern bool semloom_wire_common_json_int32(Jsonb *message,
										   const char *key,
										   int32 *result,
										   AiProviderError *error);
extern bool semloom_wire_common_validate_response_type(
	Jsonb *message,
	const char *expected_type,
	uint32 expected_fields,
	AiProviderError *error);

#endif
