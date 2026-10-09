// SemLoom batch adapter for duckdb-ai 0.4.14. See LICENSE.upstream and README.md.
#pragma once
#include <stdint.h>

#if defined(_WIN32)
#define SEMLOOM_EXPORT __declspec(dllexport)
#else
#define SEMLOOM_EXPORT __attribute__((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint64_t row;
    const char *query_id;
    const char *call_id;
    const char *model;
    const char *endpoint;
    const char *payload;
    uint64_t payload_size;
    const char *const *headers;
    uint64_t header_count;
    int64_t estimated_tokens;
    int64_t timeout_seconds;
    int64_t connect_timeout_seconds;
    int64_t ready_ns;
} SemLoomDuckDBCallV1;

typedef struct {
    uint64_t row;
    const char *body;
    uint64_t body_size;
    int64_t http_status;
    int64_t elapsed_ms;
    const char *error;
    uint64_t error_size;
} SemLoomDuckDBResponseV1;

typedef int (*SemLoomDuckDBCancelledV1)(void *context);
// Consume a settled full response with the native parser. Return 1 to stop on a
// fatal native error, 2 for invalid response metadata, 0 to continue.
typedef int (*SemLoomDuckDBConsumeV1)(uint64_t index, const SemLoomDuckDBResponseV1 *response, void *context);
// Return 0 for complete responses, 1 for cancellation, 2 for executor failure,
// 3 after the native parser requests a stop. Never parse a model response in Python.
// Pointers remain borrowed until release(batch_id), including after an error.
typedef int (*SemLoomDuckDBBatchV1)(uint64_t batch_id, const SemLoomDuckDBCallV1 *calls,
                                  uint64_t count, SemLoomDuckDBResponseV1 *responses,
                                  SemLoomDuckDBCancelledV1 cancelled, SemLoomDuckDBConsumeV1 consume, void *context);
typedef void (*SemLoomDuckDBReleaseV1)(uint64_t batch_id);

SEMLOOM_EXPORT int duckdb_ai_semloom_abi_v1(void);
SEMLOOM_EXPORT int duckdb_ai_semloom_register_v1(SemLoomDuckDBBatchV1 batch, SemLoomDuckDBReleaseV1 release);
SEMLOOM_EXPORT int duckdb_ai_semloom_unregister_v1(SemLoomDuckDBBatchV1 batch);

#ifdef __cplusplus
}
#endif
