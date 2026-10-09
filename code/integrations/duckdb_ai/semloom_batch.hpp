// SemLoom batch adapter for duckdb-ai 0.4.14. See LICENSE.upstream and README.md.
#pragma once
#include "duckdb_ai_provider.hpp"
#include <functional>

namespace duckdb {
namespace duckdb_ai {
// Only complete, already-prepared single-model calls enter the external executor.
void DispatchSemLoomBatch(const std::vector<PreparedCompletion> &prepared, const std::vector<uint64_t> &rows,
                         const std::function<bool(uint64_t, const CompletionResponse &)> &consume);
} // namespace duckdb_ai
} // namespace duckdb
