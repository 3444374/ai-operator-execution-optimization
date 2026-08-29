/*
 * Provider-neutral synchronous execution contract.
 *
 * Inputs are fixed-width open specs and borrowed task bytes; outputs are
 * session-owned completions or caller-owned errors.  This header passes its
 * boundary check only while it has no PostgreSQL types or headers.
 * Plan: experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md.
 */
#ifndef AI_PROVIDER_PORT_H
#define AI_PROVIDER_PORT_H

#include <stdbool.h>
#include <stdint.h>

#define AI_PROVIDER_ERROR_DETAIL_CAPACITY 160

typedef struct AiProviderSession AiProviderSession;

typedef enum AiProviderStatus
{
	AI_PROVIDER_STATUS_OK = 0,
	AI_PROVIDER_STATUS_ERROR = 1,
} AiProviderStatus;

typedef enum AiProviderErrorCode
{
	AI_PROVIDER_ERROR_NONE = 0,
	AI_PROVIDER_ERROR_INVALID_SPEC = 1,
	AI_PROVIDER_ERROR_SESSION_CLOSED = 2,
	AI_PROVIDER_ERROR_TASK_MISMATCH = 3,
	AI_PROVIDER_ERROR_NULL_TASK = 4,
	AI_PROVIDER_ERROR_INPUT_TOO_LARGE = 5,
	AI_PROVIDER_ERROR_UNSUPPORTED_ENCODING = 6,
	AI_PROVIDER_ERROR_RESOURCE_EXHAUSTED = 7,
	AI_PROVIDER_ERROR_SYSTEM = 8,
	AI_PROVIDER_ERROR_CONNECTION_LOST = 9,
	AI_PROVIDER_ERROR_FRAME_LIMIT = 10,
	AI_PROVIDER_ERROR_PROTOCOL = 11,
	AI_PROVIDER_ERROR_NUMERIC_RANGE = 12,
} AiProviderErrorCode;

typedef enum AiProviderOperation
{
	AI_PROVIDER_OPERATION_NONE = 0,
	AI_PROVIDER_OPERATION_OPEN_SPEC = 1,
	AI_PROVIDER_OPERATION_SOCKET_PATH_LENGTH = 2,
	AI_PROVIDER_OPERATION_SOCKET_PATH_ABSOLUTE = 3,
	AI_PROVIDER_OPERATION_RESERVE_EXTERNAL_FD = 4,
	AI_PROVIDER_OPERATION_CREATE_SOCKET = 5,
	AI_PROVIDER_OPERATION_CONFIGURE_SOCKET = 6,
	AI_PROVIDER_OPERATION_CONNECT_SOCKET = 7,
	AI_PROVIDER_OPERATION_INSPECT_SOCKET = 8,
	AI_PROVIDER_OPERATION_WRITE_SOCKET = 9,
	AI_PROVIDER_OPERATION_READ_SOCKET = 10,
	AI_PROVIDER_OPERATION_SEND_FRAME = 11,
	AI_PROVIDER_OPERATION_RECEIVE_FRAME = 12,
	AI_PROVIDER_OPERATION_PARSE_JSON = 13,
	AI_PROVIDER_OPERATION_RESPONSE_OBJECT = 14,
	AI_PROVIDER_OPERATION_RESPONSE_FIELD = 15,
	AI_PROVIDER_OPERATION_RESPONSE_INTEGER = 16,
	AI_PROVIDER_OPERATION_RESPONSE_BOOLEAN = 17,
	AI_PROVIDER_OPERATION_PROVIDER_REJECTED = 18,
	AI_PROVIDER_OPERATION_UNEXPECTED_MESSAGE = 19,
	AI_PROVIDER_OPERATION_OPEN_RESPONSE = 20,
	AI_PROVIDER_OPERATION_COMPLETION_IDENTITY = 21,
	AI_PROVIDER_OPERATION_COMPLETION_OUTPUT = 22,
	AI_PROVIDER_OPERATION_COMPLETION_EVIDENCE = 23,
} AiProviderOperation;

typedef enum AiProviderOperatorKind
{
	AI_PROVIDER_OPERATOR_MAP = 1,
	AI_PROVIDER_OPERATOR_FILTER = 2,
} AiProviderOperatorKind;

typedef enum AiProviderValueKind
{
	AI_PROVIDER_VALUE_TEXT = 1,
	AI_PROVIDER_VALUE_TRISTATE = 2,
} AiProviderValueKind;

typedef enum AiProviderNullPolicy
{
	AI_PROVIDER_NULL_PROPAGATE = 1,
} AiProviderNullPolicy;

typedef enum AiProviderErrorPolicy
{
	AI_PROVIDER_ERROR_FAIL_QUERY = 1,
} AiProviderErrorPolicy;

typedef struct AiByteSlice
{
	const uint8_t *data;
	uint32_t length;
} AiByteSlice;

typedef struct AiOpenSpec
{
	uint32_t operator_kind;
	uint32_t input_value_kind;
	uint32_t output_value_kind;
	uint32_t null_policy;
	uint32_t error_policy;
	uint32_t semantic_spec_version;
	AiByteSlice semantic_spec_id;
	AiByteSlice physical_algorithm;
} AiOpenSpec;

typedef struct AiPreparedTask
{
	uint64_t sequence;
	AiByteSlice input;
	bool is_null;
} AiPreparedTask;

typedef struct AiCompletion
{
	uint64_t sequence;
	AiByteSlice output;
	bool is_null;
} AiCompletion;

typedef struct AiProviderError
{
	uint32_t code;
	uint32_t operation;
	int32_t system_errno;
	uint32_t limit_bytes;
	uint16_t detail_length;
	char detail[AI_PROVIDER_ERROR_DETAIL_CAPACITY];
} AiProviderError;

typedef struct AiProviderOps
{
	const char *adapter_name;
	AiProviderStatus (*open)(const void *config,
							 const AiOpenSpec *spec,
							 AiProviderSession **session,
							 AiProviderError *error);
	AiProviderStatus (*drive)(AiProviderSession *session,
							  const AiPreparedTask *task,
							  AiCompletion *completion,
							  AiProviderError *error);
	void (*close)(AiProviderSession *session);
} AiProviderOps;

typedef struct AiProvider
{
	const AiProviderOps *ops;
	const void *config;
} AiProvider;

/*
 * Task input is borrowed until drive returns.  Completion output is owned by
 * the session and remains valid until the next drive or close.  SQL NULL is
 * represented only by is_null; an empty non-NULL value has length zero.  Any
 * non-OK open or drive result is terminal: open may publish a partial session,
 * the caller closes it, and no later drive may continue that session.  Close
 * accepts NULL and repeated calls.  limit_bytes is a code-specific fixed-width
 * parameter and is zero when the error does not report a byte limit.
 */

#endif
