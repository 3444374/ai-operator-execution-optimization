/* Shared bounded frame and PostgreSQL JSON primitives for versioned UDS wires. */
#ifndef SEMLOOM_WIRE_COMMON_H
#define SEMLOOM_WIRE_COMMON_H

#include "postgres.h"

#include "utils/jsonb.h"

#include "provider/ai_provider_port.h"

#define SEMLOOM_WIRE_COMMON_MAX_FRAME_BYTES (1024 * 1024)

extern AiProviderStatus semloom_wire_common_send_frame(
	pgsocket socket_fd,
	const char *payload,
	Size payload_length,
	AiProviderError *error);
extern AiProviderStatus semloom_wire_common_receive_frame(
	pgsocket socket_fd,
	char **payload,
	AiProviderError *error);
extern AiProviderStatus semloom_wire_common_wait_connected(
	pgsocket socket_fd,
	AiProviderError *error);
extern void semloom_wire_common_wait_connect_retry(void);
extern AiProviderStatus semloom_wire_common_parse_json(
	const char *payload,
	Jsonb **message,
	AiProviderError *error);
extern AiProviderStatus semloom_wire_common_parse_json_unique(
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
extern bool semloom_wire_common_json_bool(Jsonb *message,
										  const char *key,
										  bool *result,
										  AiProviderError *error);

#endif
