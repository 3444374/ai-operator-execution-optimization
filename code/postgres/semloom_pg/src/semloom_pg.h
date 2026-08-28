#ifndef SEMLOOM_PG_H
#define SEMLOOM_PG_H

#include "postgres.h"

#include "nodes/extensible.h"
#include "nodes/pathnodes.h"

#define SEMLOOM_CUSTOM_SCAN_NAME "SemLoom SemMap"
#define SEMLOOM_RECORDING_PREFIX "recorded:"
#define SEMLOOM_PROTOCOL_VERSION 2
#define SEMLOOM_MAX_FRAME_BYTES (1024 * 1024)
#define SEMLOOM_MAX_INPUT_BYTES ((SEMLOOM_MAX_FRAME_BYTES - 4096) / 6)
#define SEMLOOM_SHA256_HEX_LENGTH 64
#define SEMLOOM_RECORDING_SPEC_ID "semloom.recording.sem_map.text"
#define SEMLOOM_RECORDING_ALGORITHM "RECORDING"
#define SEMLOOM_RECORDING_SPEC_VERSION 1
#define SEMLOOM_IN_PROCESS_EXECUTION_ID "semloom.provider.recording.in_process.v1"
#define SEMLOOM_UDS_EXECUTION_ID "semloom.provider.recording.uds.v2"

typedef struct SemloomProviderSession SemloomProviderSession;

typedef enum SemloomOperatorKind
{
	SEMLOOM_OPERATOR_MAP = 1,
} SemloomOperatorKind;

typedef enum SemloomNullPolicy
{
	SEMLOOM_NULL_PROPAGATE = 1,
} SemloomNullPolicy;

typedef enum SemloomErrorPolicy
{
	SEMLOOM_ERROR_FAIL_QUERY = 1,
} SemloomErrorPolicy;

typedef struct SemloomSemanticPlanSpec
{
	SemloomOperatorKind operator_kind;
	Oid input_type;
	Oid output_type;
	SemloomNullPolicy null_policy;
	SemloomErrorPolicy error_policy;
	uint32 semantic_spec_version;
	const char *semantic_spec_id;
	const char *physical_algorithm;
} SemloomSemanticPlanSpec;

typedef struct SemloomPreparedSemanticTask
{
	uint64 sequence;
	Oid input_type;
	Datum input;
	bool is_null;
} SemloomPreparedSemanticTask;

typedef struct SemloomCompletionRecord
{
	uint64 sequence;
	Oid output_type;
	Datum output;
	bool is_null;
} SemloomCompletionRecord;

extern const CustomScanMethods semloom_scan_methods;

extern const char *semloom_gateway_socket_path(void);
extern Oid semloom_map_function_oid(void);
extern bool semloom_is_map_function(Oid function_oid);
extern void semloom_protocol_semantic_spec_digest(
	const SemloomSemanticPlanSpec *plan_spec,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
extern void semloom_protocol_physical_algorithm_digest(
	const SemloomSemanticPlanSpec *plan_spec,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
extern void semloom_protocol_provider_execution_digest(
	const char *provider_execution_id,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
extern void semloom_protocol_payload_digest(bool is_null,
										const char *payload,
										Size payload_length,
										char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
extern void semloom_protocol_completion_digest(
	const char semantic_spec_digest[SEMLOOM_SHA256_HEX_LENGTH + 1],
	const char physical_algorithm_digest[SEMLOOM_SHA256_HEX_LENGTH + 1],
	const char provider_execution_digest[SEMLOOM_SHA256_HEX_LENGTH + 1],
	const char payload_digest[SEMLOOM_SHA256_HEX_LENGTH + 1],
	uint64 sequence,
	bool is_null,
	const char *output_payload,
	Size output_length,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
extern void semloom_protocol_send_frame(pgsocket socket_fd,
										const char *payload,
										Size payload_length);
extern char *semloom_protocol_receive_frame(pgsocket socket_fd);
extern void semloom_protocol_wait_connected(pgsocket socket_fd);
extern void semloom_protocol_wait_connect_retry(void);
extern SemloomProviderSession *semloom_provider_open(const SemloomSemanticPlanSpec *plan_spec);
extern void semloom_provider_drive(SemloomProviderSession *session,
								   const SemloomPreparedSemanticTask *task,
								   MemoryContext result_context,
								   SemloomCompletionRecord *completion);
extern void semloom_provider_close(SemloomProviderSession *session);
extern const char *semloom_provider_name(const SemloomProviderSession *session);
extern uint64 semloom_provider_accepted_rows(const SemloomProviderSession *session);
extern uint64 semloom_provider_emitted_rows(const SemloomProviderSession *session);
extern void semloom_add_sem_map_paths(PlannerInfo *root,
									 UpperRelationKind stage,
									 RelOptInfo *input_rel,
									 RelOptInfo *output_rel);

#endif
