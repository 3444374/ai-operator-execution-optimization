// SemLoom batch adapter for duckdb-ai 0.4.14. See LICENSE.upstream and README.md.
#include "semloom_batch.h"
#include "semloom_batch.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include <atomic>
#include <chrono>
#include <mutex>

namespace {
std::mutex callback_mutex;
SemLoomDuckDBBatchV1 batch_callback = nullptr;
SemLoomDuckDBReleaseV1 release_callback = nullptr;
uint64_t active_batches = 0;
std::atomic<uint64_t> next_batch_id {1};
constexpr uint64_t MAX_BATCH_ROWS = 2048;
constexpr uint64_t MAX_BATCH_BYTES = 32 * 1024 * 1024;
constexpr uint64_t MAX_RESPONSE_BYTES = 8 * 1024 * 1024;

struct CallbackLease {
    CallbackLease() {
        std::lock_guard<std::mutex> lock(callback_mutex);
        if (!batch_callback) {
            throw duckdb::InvalidInputException("SemLoom DuckDB batch callback is not registered");
        }
        batch = batch_callback;
        release = release_callback;
        active_batches++;
    }
    ~CallbackLease() {
        std::lock_guard<std::mutex> lock(callback_mutex);
        active_batches--;
    }
    SemLoomDuckDBBatchV1 batch;
    SemLoomDuckDBReleaseV1 release;
};

struct ResponseLease {
    ~ResponseLease() { release(batch_id); }
    SemLoomDuckDBReleaseV1 release;
    uint64_t batch_id;
};

struct DispatchContext {
    duckdb::ClientContext *client;
    const std::vector<uint64_t> &rows;
    const std::function<bool(uint64_t, const duckdb::duckdb_ai::CompletionResponse &)> &consume;
    std::vector<bool> received;
    uint64_t bytes = 0;
    std::exception_ptr error;
    bool native_stop = false;
};

int IsCancelled(void *opaque) {
    auto context = static_cast<DispatchContext *>(opaque);
    return context->client && context->client->IsInterrupted() ? 1 : 0;
}

int ConsumeResponse(uint64_t index, const SemLoomDuckDBResponseV1 *response, void *opaque) {
    auto &context = *static_cast<DispatchContext *>(opaque);
    try {
        if (!response || index >= context.rows.size() || context.received[index]) {
            throw duckdb::IOException("SemLoom DuckDB returned invalid or duplicate response identity");
        }
        context.bytes += response->body_size + response->error_size;
        if (response->row != context.rows[index] || response->body_size > MAX_RESPONSE_BYTES ||
            response->error_size > MAX_RESPONSE_BYTES || context.bytes > MAX_BATCH_BYTES ||
            (response->body_size && !response->body) || (response->error_size && !response->error) ||
            response->elapsed_ms < -1 || response->http_status < 0 || response->http_status > 599) {
            throw duckdb::IOException("SemLoom DuckDB returned invalid response size or metadata");
        }
        duckdb::duckdb_ai::CompletionResponse value;
        if (response->body_size) { value.body.assign(response->body, response->body_size); }
        if (response->error_size) { value.transport_error.assign(response->error, response->error_size); }
        value.status = static_cast<long>(response->http_status);
        value.elapsed_ms = response->elapsed_ms;
        context.received[index] = true;
        context.native_stop = context.consume(index, value);
        return context.native_stop ? 1 : 0;
    } catch (...) {
        context.error = std::current_exception();
        return 2;
    }
}
} // namespace

extern "C" {
SEMLOOM_EXPORT int duckdb_ai_semloom_abi_v1(void) { return 1; }

SEMLOOM_EXPORT int duckdb_ai_semloom_register_v1(SemLoomDuckDBBatchV1 batch, SemLoomDuckDBReleaseV1 release) {
    std::lock_guard<std::mutex> lock(callback_mutex);
    if (!batch || !release || batch_callback || active_batches) { return 0; }
    batch_callback = batch;
    release_callback = release;
    return 1;
}

SEMLOOM_EXPORT int duckdb_ai_semloom_unregister_v1(SemLoomDuckDBBatchV1 batch) {
    std::lock_guard<std::mutex> lock(callback_mutex);
    if (active_batches || batch != batch_callback) { return 0; }
    batch_callback = nullptr;
    release_callback = nullptr;
    return 1;
}
}

namespace duckdb {
namespace duckdb_ai {
void DispatchSemLoomBatch(const std::vector<PreparedCompletion> &prepared, const std::vector<uint64_t> &rows,
                         const std::function<bool(uint64_t, const CompletionResponse &)> &consume) {
    if (prepared.empty()) { return; }
    if (prepared.size() != rows.size() || prepared.size() > MAX_BATCH_ROWS) {
        throw InvalidInputException("SemLoom DuckDB batch exceeds its row limit");
    }
    auto context = prepared[0].options.client_context;
    std::vector<SemLoomDuckDBCallV1> calls;
    std::vector<std::vector<const char *>> headers(prepared.size());
    uint64_t input_bytes = 0;
    for (uint64_t i = 0; i < prepared.size(); i++) {
        const auto &call = prepared[i];
        if (call.provider.model != prepared[0].provider.model || call.endpoint != prepared[0].endpoint ||
            call.headers != prepared[0].headers || call.options.client_context != context) {
            throw InvalidInputException("SemLoom DuckDB requires one model, endpoint and credential per batch");
        }
        input_bytes += call.payload.size();
        for (const auto &header : call.headers) {
            input_bytes += header.size();
            headers[i].push_back(header.c_str());
        }
        if (input_bytes > MAX_BATCH_BYTES) {
            throw InvalidInputException("SemLoom DuckDB batch exceeds its request byte limit");
        }
        calls.push_back({rows[i], call.options.query_id.c_str(), call.options.operation_id.c_str(),
                         call.provider.model.c_str(), call.endpoint.c_str(), call.payload.data(), call.payload.size(),
                         headers[i].data(), headers[i].size(), call.estimated_tokens, call.timeout_seconds,
                         call.connect_timeout_seconds, call.ready_ns});
    }
    DispatchContext dispatch {context, rows, consume, std::vector<bool>(rows.size())};
    if (IsCancelled(&dispatch)) { throw InterruptException(); }
    CallbackLease callback;
    auto batch_id = next_batch_id.fetch_add(1);
    std::vector<SemLoomDuckDBResponseV1> responses(prepared.size());
    ResponseLease buffers {callback.release, batch_id};
    auto status = callback.batch(batch_id, calls.data(), calls.size(), responses.data(), IsCancelled, ConsumeResponse, &dispatch);
    if (dispatch.error) { std::rethrow_exception(dispatch.error); }
    if (status == 1 || IsCancelled(&dispatch)) { throw InterruptException(); }
    if (status == 3 && dispatch.native_stop) { return; }
    if (status != 0) { throw IOException("SemLoom DuckDB batch executor failed"); }
    for (bool received : dispatch.received) {
        if (!received) { throw IOException("SemLoom DuckDB returned an incomplete batch"); }
    }
}
} // namespace duckdb_ai
} // namespace duckdb
