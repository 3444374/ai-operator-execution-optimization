#pragma once
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <exception>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include "dbend/cpp/imlane_dbend.hpp"

namespace imlane_records {
using Table = std::shared_ptr<arrow::Table>;
using Json = nlohmann::json;
namespace dbend = IMLane::DBEnd;
inline void check(const arrow::Status& status) {
    if (!status.ok()) throw std::runtime_error(status.ToString());
}
inline void check(const dbend::ContextStatus& status) {
    if (status.kind == dbend::StatusKind::ERROR) throw std::runtime_error(status.info);
}
template<class T> T take(arrow::Result<T> result) {
    if (!result.ok()) throw std::runtime_error(result.status().ToString());
    return std::move(result).ValueOrDie();
}
inline void require(bool value, const char* message) {
    if (!value) throw std::invalid_argument(message);
}
struct Limits {
    int64_t max_block_rows = 16;
    size_t max_pending_blocks = 2, max_query_rows = 4096, max_queries = 64;
    size_t max_input_bytes = 65536, max_output_bytes = 1048576;
    void validate() const {
        require(max_block_rows > 0 && max_block_rows <= 4096 && max_pending_blocks > 0 &&
            max_pending_blocks <= 128 && max_query_rows > 0 && max_query_rows <= 1048576 &&
            max_queries > 0 && max_queries <= 4096 && max_input_bytes > 0 &&
            max_input_bytes <= 16777216 && max_output_bytes > 0 && max_output_bytes <= 16777216,
            "records capacities must be positive and finite");
    }
};
inline void schema(const Table& table, const std::vector<std::string>& names,
                   const std::vector<std::shared_ptr<arrow::DataType>>& types) {
    require(bool(table), "missing records table");
    check(table->ValidateFull());
    require(table->ColumnNames() == names, "records columns differ");
    require(!table->schema()->HasMetadata(), "records schema metadata is unsupported");
    for (int i = 0; i < table->num_columns(); ++i) {
        require(table->field(i)->type()->Equals(types.at(i)), "records types differ");
        require(!table->field(i)->HasMetadata(), "records field metadata is unsupported");
        require(table->column(i)->null_count() == 0, "NULL records field");
    }
}
inline std::string string_at(const Table& table, int col, int64_t row) {
    return std::static_pointer_cast<arrow::StringScalar>(take(table->column(col)->GetScalar(row)))->ToString();
}
inline int64_t row_at(const Table& table, int64_t row) {
    return std::static_pointer_cast<arrow::Int64Scalar>(take(table->column(1)->GetScalar(row)))->value;
}
inline void bounded_buffers(const Table& table, size_t cap) {
    size_t bytes = 0;
    for (const auto& column : table->columns()) {
        require(column->num_chunks() <= table->num_rows() + 1, "too many records chunks");
        for (const auto& chunk : column->chunks()) {
            for (const auto& buffer : chunk->data()->buffers) {
                if (!buffer) continue;
                require(buffer->size() >= 0 && static_cast<uint64_t>(buffer->size()) <= cap - bytes,
                    "records buffers exceed byte allowance before copy");
                bytes += static_cast<size_t>(buffer->size());
            }
        }
    }
}
inline void identity(const std::string& value) {
    require(!value.empty() && value.size() <= 4096, "records identity is empty or oversized");
}
inline std::vector<std::string> validate_input(const Table& input, const std::string& query,
                                               const Limits& limits) {
    schema(input, {"call_id", "row_id", "query_id", "stage_id", "record_json"},
        {arrow::utf8(), arrow::int64(), arrow::utf8(), arrow::utf8(), arrow::utf8()});
    require(input->num_rows() > 0 && input->num_rows() <= limits.max_block_rows,
        "empty or oversized records block");
    bounded_buffers(input, limits.max_input_bytes);
    std::unordered_set<std::string> seen;
    std::vector<std::string> ids;
    const auto stage = string_at(input, 3, 0);
    identity(stage);
    for (int64_t i = 0; i < input->num_rows(); ++i) {
        auto id = string_at(input, 0, i);
        identity(id);
        require(seen.insert(id).second, "records call_id repeats inside block");
        require(row_at(input, i) >= 0, "negative records row_id");
        require(string_at(input, 2, i) == query && string_at(input, 3, i) == stage,
            "records query or stage differs inside block");
        auto record = Json::parse(string_at(input, 4, i));
        require(record.is_object() && !record.empty(), "record_json must be a nonempty object");
        for (auto& value : record.items())
            require(value.value().is_string(), "record_json values must be lightweight strings");
        ids.push_back(std::move(id));
    }
    return ids;
}
inline Json validate_outcome(const std::string& wire, const Table& input, int64_t i) {
    auto value = Json::parse(wire);
    require(value.is_object() && value.size() == 9, "records outcome fields differ");
    for (const char* key : {"query_id", "stage_id", "status", "result", "error_type", "error",
                           "model_entry_attempted", "completion_known", "worker"})
        require(value.contains(key), "records outcome field is missing");
    require(value.at("query_id") == string_at(input, 2, i) &&
        value.at("stage_id") == string_at(input, 3, i), "records outcome query/stage differs");
    require(value.at("model_entry_attempted").is_boolean() && value.at("completion_known").is_boolean(),
        "records completion evidence must be boolean");
    const auto& worker = value.at("worker");
    require(worker.is_object() && worker.contains("worker_pid") &&
        worker.at("worker_pid").is_number_integer() && worker.at("worker_pid").get<int64_t>() > 0,
        "records worker identity is missing");
    if (value.at("status") == "ok") {
        const auto& result = value.at("result");
        require(result.is_object() && value.at("error_type").is_null() && value.at("error").is_null() &&
            value.at("completion_known") == true, "records success/error fields are inconsistent");
        require(result.contains("_occurrence_id") && result.at("_occurrence_id") == string_at(input, 0, i) &&
            result.contains("_position") && result.at("_position").is_number_integer() &&
            result.at("_position").get<int64_t>() == row_at(input, i), "records result occurrence differs");
    } else {
        require(value.at("status") == "error" && value.at("result").is_null() &&
            value.at("error_type").is_string() && !value.at("error_type").get<std::string>().empty() &&
            value.at("error").is_string() && !value.at("error").get<std::string>().empty(),
            "records failure/success fields are inconsistent");
    }
    return value;
}
struct ExchangeResult {
    Table input, output;
    size_t max_output_bytes = 0;
    std::exception_ptr conversion_error;
    bool conversion_entered = false;
};
inline void convert_output(const Table& table, ExchangeResult& result) {
    schema(table, {"call_id", "row_id", "outcome_json"}, {arrow::utf8(), arrow::int64(), arrow::utf8()});
    require(table->num_rows() == result.input->num_rows(), "records result cardinality differs");
    bounded_buffers(table, result.max_output_bytes);
    for (int64_t i = 0; i < table->num_rows(); ++i) {
        require(string_at(table, 0, i) == string_at(result.input, 0, i) &&
            row_at(table, i) == row_at(result.input, i), "records occurrence/order differs");
        validate_outcome(string_at(table, 2, i), result.input, i);
    }
    auto sink = take(arrow::io::BufferOutputStream::Create());
    auto writer = take(arrow::ipc::MakeStreamWriter(sink, table->schema()));
    check(writer->WriteTable(*table));
    check(writer->Close());
    auto bytes = take(sink->Finish());
    require(static_cast<uint64_t>(bytes->size()) <= result.max_output_bytes + 65536,
        "records IPC copy exceeds its finite allowance");
    auto reader = take(arrow::ipc::RecordBatchStreamReader::Open(std::make_shared<arrow::io::BufferReader>(bytes)));
    result.output = take(reader->ToTable());
}
} // namespace imlane_records

namespace IMLane::DBEnd {
template<> class TableConverter<arrow::Table, imlane_records::ExchangeResult> {
public:
    std::shared_ptr<arrow::Table> ConvertToExchange(arrow::Table* input) {
        return std::shared_ptr<arrow::Table>(input, [](arrow::Table*) {});
    }
    void ConvertFromExchange(std::shared_ptr<arrow::Table> table,
                             imlane_records::ExchangeResult& result) noexcept {
        result.conversion_entered = true;
        try { imlane_records::convert_output(table, result); }
        catch (...) {
            result.output.reset();
            result.conversion_error = std::current_exception();
        }
    }
};
} // namespace IMLane::DBEnd

namespace imlane_records {
struct Pending {
    enum class State { submitted, collected, failed };
    std::shared_ptr<ExchangeResult> result;
    std::unique_ptr<dbend::ExecAsyncFuncContext> future;
    State state = State::submitted;
};
inline bool ready(const Pending& pending) {
    require(pending.state == Pending::State::submitted && bool(pending.future), "records pending already consumed");
    return pending.future->ExecuteAsyncFuncTryPull();
}
inline Table collect_ready(Pending& pending) {
    require(ready(pending), "records pending is not ready");
    // Set terminal state before get or any later validation can throw.
    pending.state = Pending::State::failed;
    pending.future->task.get();
    pending.future.reset();
    if (pending.result->conversion_error) std::rethrow_exception(pending.result->conversion_error);
    require(pending.result->conversion_entered && bool(pending.result->output), "records output was not converted");
    auto result = pending.result->output;
    for (int64_t i = 0; i < result->num_rows(); ++i)
        require(validate_outcome(string_at(result, 2, i), pending.result->input, i).at("status") == "ok",
            "records worker reported an error; owner must stop");
    pending.result.reset();
    pending.state = Pending::State::collected;
    return result;
}
class Session {
    std::shared_ptr<dbend::DBEndContext> context_;
    std::string alias_, query_;
    Limits limits_;
    std::unordered_set<std::string> calls_, blocks_, queries_;
    std::map<std::string, Pending> pending_;
    bool poisoned_ = false;
public:
    Session(std::shared_ptr<dbend::DBEndContext> context, std::string alias, Limits limits = {})
        : context_(std::move(context)), alias_(std::move(alias)), limits_(limits) { limits_.validate(); }
    bool poisoned() const { return poisoned_; }
    void mark_terminal() noexcept { poisoned_ = true; }
    size_t pending_count() const { return pending_.size(); }
    void begin_query(const std::string& query) {
        require(!poisoned_ && query_.empty(), "records owner is terminal or a query is active");
        identity(query);
        require(queries_.size() < limits_.max_queries && queries_.insert(query).second,
            "records query repeats or exceeds session capacity");
        query_ = query;
    }
    void submit(const std::string& block, Table input) {
        require(!poisoned_ && !query_.empty(), "records owner is terminal or no query is active");
        identity(block);
        require(pending_.size() < limits_.max_pending_blocks, "records pending capacity reached");
        require(!blocks_.count(block), "records block identity repeats");
        auto ids = validate_input(input, query_, limits_);
        require(calls_.size() + ids.size() <= limits_.max_query_rows, "records query row allowance exceeded");
        for (const auto& id : ids) require(!calls_.count(id), "records call_id repeats across blocks");
        auto result = std::make_shared<ExchangeResult>();
        result->input = std::move(input);
        result->max_output_bytes = limits_.max_output_bytes;
        try {
            // Allocate host bookkeeping before enqueueing an author task.
            auto inserted = pending_.emplace(block, Pending{result, nullptr});
            for (const auto& id : ids) calls_.insert(id);
            blocks_.insert(block);
            inserted.first->second.future = context_->ExecuteAsyncFuncPush([context = context_, result, alias = alias_, query = query_] {
                check(context->ExecuteFunction(alias, *result->input, *result, nullptr, query));
            });
        } catch (...) { poisoned_ = true; throw; }
    }
    std::vector<std::string> ready_blocks() const {
        require(!poisoned_, "records owner is terminal");
        std::vector<std::string> result;
        for (const auto& item : pending_) if (ready(item.second)) result.push_back(item.first);
        return result;
    }
    Table collect(const std::string& block) {
        require(!poisoned_, "records owner is terminal");
        auto it = pending_.find(block);
        require(it != pending_.end(), "records pending is unknown or already collected");
        require(ready(it->second), "records pending is not ready");
        try {
            auto result = collect_ready(it->second);
            pending_.erase(it);
            return result;
        } catch (...) { poisoned_ = true; throw; }
    }
    void end_query() {
        require(!poisoned_ && !query_.empty() && pending_.empty(), "records query has pending work or owner is terminal");
        query_.clear(); calls_.clear(); blocks_.clear();
    }
};
} // namespace imlane_records
