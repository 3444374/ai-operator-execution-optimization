// CPU qualification of the actual shared DuckDB converter and prediction SQL.
// Do not include prediction/datachunk_converter.hpp: that defines the converter
// in this driver and would invalidate the DSO evidence.
#include "duckdb.hpp"
#include "dbend/c/imlane_dbend.hpp"
#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "nlohmann/json.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <ctime>
#include <dlfcn.h>
#include <fcntl.h>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

namespace duckdb {
void ConverArrowAarrayToVector(::ArrowArray *, ::ArrowSchema *, Vector &);
}

using Json = nlohmann::json;
using Texts = std::vector<std::optional<std::string>>;

static int64_t now_ns() {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value)) throw std::runtime_error("monotonic clock failed");
    return int64_t(value.tv_sec) * 1000000000 + value.tv_nsec;
}

static void save(const std::string &path, const Json &report) {
    const auto payload = report.dump(2) + "\n";
    const int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0) throw std::runtime_error("cannot create fresh qualification result");
    size_t offset = 0;
    while (offset != payload.size()) {
        const auto count = write(fd, payload.data() + offset, payload.size() - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) { close(fd); throw std::runtime_error("qualification result write failed"); }
        offset += static_cast<size_t>(count);
    }
    if (close(fd)) throw std::runtime_error("qualification result close failed");
}

static Json json_texts(const Texts &values) {
    Json result = Json::array();
    for (const auto &value : values) result.push_back(value ? Json(*value) : Json(nullptr));
    return result;
}

static Texts inputs(const std::string &name) {
    if (name == "empty") return {};
    if (name == "null") return {"before", std::nullopt, "after"};
    if (name == "ownership") return {std::string(8192, 'L') + "中文-tail"};
    if (name == "strings" || name == "offset") return {"", "hello", "quote'\"\\\n中文"};
    throw std::invalid_argument("unknown VARCHAR qualification case");
}

static void check(const arrow::Status &status) {
    if (!status.ok()) throw std::runtime_error(status.ToString());
}

static Json read_vector(duckdb::Vector &result, size_t size) {
    Json values = Json::array();
    for (size_t index = 0; index < size; ++index) {
        const auto value = result.GetValue(index);
        values.push_back(value.IsNull() ? Json(nullptr) : Json(value.ToString()));
    }
    return values;
}

static void converter(const std::string &name, Json &report) {
    const auto expected = inputs(name);
    arrow::StringBuilder builder;
    if (name == "offset") check(builder.Append("offset sentinel"));
    for (const auto &value : expected) {
        if (value) check(builder.Append(*value));
        else check(builder.AppendNull());
    }
    std::shared_ptr<arrow::Array> values;
    check(builder.Finish(&values));
    if (name == "offset") values = values->Slice(1);
    ::ArrowArray array{};
    ::ArrowSchema schema{};
    check(arrow::ExportArray(*values, &array, &schema));
    duckdb::Vector result(duckdb::LogicalType::VARCHAR);
    try {
        report.update(Json{{"array_offset", array.offset}, {"array_null_count", array.null_count},
            {"schema_format", schema.format}, {"expected", json_texts(expected)}});
        duckdb::ConverArrowAarrayToVector(&array, &schema, result);
        report["observed_before_release"] = read_vector(result, expected.size());
        const auto begin = reinterpret_cast<std::uintptr_t>(array.buffers[2]);
        const auto offsets = static_cast<const int32_t *>(array.buffers[1]);
        const auto end = begin + (expected.empty() ? 0 : offsets[array.offset + array.length]);
        bool borrowed = false;
        for (size_t index = 0; index < expected.size(); ++index) {
            if (duckdb::FlatVector::IsNull(result, index)) continue;
            const auto &text = duckdb::FlatVector::GetData<duckdb::string_t>(result)[index];
            const auto pointer = reinterpret_cast<std::uintptr_t>(text.GetData());
            if (!text.IsInlined() && pointer >= begin && pointer < end) borrowed = true;
        }
        report["borrows_arrow_value_buffer"] = borrowed;
        array.release(&array);
        schema.release(&schema);
        values.reset();
        report["arrow_owners_released"] = true;
        // Never dereference a proven dangling pointer just to obtain a crash.
        if (borrowed) report["post_release_read"] = "not_attempted_borrowed_arrow_storage";
        else report["observed_after_release"] = read_vector(result, expected.size());
        const bool equal = report["observed_before_release"] == json_texts(expected);
        report["passed"] = equal && (name != "ownership" ||
            (!borrowed && report["observed_after_release"] == json_texts(expected)));
    } catch (...) {
        if (array.release) array.release(&array);
        if (schema.release) schema.release(&schema);
        throw;
    }
}

static std::string literal(const std::string &value) {
    std::string result = "'";
    for (const char item : value) result += item == '\'' ? "''" : std::string(1, item);
    return result + "'";
}

static auto query(duckdb::Connection &connection, const std::string &sql) {
    auto result = connection.Query(sql);
    if (result->HasError()) result->ThrowError();
    return result;
}

static void unexpected_local_call(duckdb::DataChunk &, duckdb::ExpressionState &, duckdb::Vector &) {
    throw std::runtime_error("prediction used the forbidden local C++ callback");
}

static void sql(const std::string &name, const std::string &kind, const std::string &module,
                const std::string &output, Json &report) {
    const auto expected = inputs(name);
    const auto function = name == "offset" ? "echo_sliced" : name == "null" ? "return_null" : "echo_strings";
    IMLane::DBEnd::DBEndContext registration;
    const auto check_registration = [](const IMLane::DBEnd::ContextStatus &status) {
        if (status.kind != IMLane::DBEnd::StatusKind::OK) throw std::runtime_error(status.info);
    };
    check_registration(registration.Setup());
    check_registration(registration.RegisterFunction(module, function, "varchar_cpu"));
    duckdb::DuckDB database(nullptr);
    duckdb::Connection connection(database);
    connection.CreateVectorizedFunction<duckdb::string_t, duckdb::string_t, duckdb::string_t>(
        "varchar_cpu", unexpected_local_call, duckdb::LogicalType::INVALID,
        kind == "thread" ? duckdb::FunctionKind::THREAD_SCHEDULE_PREDICTION : duckdb::FunctionKind::PROCESS_PREDICTION,
        DEFAULT_PREDICTION_BATCH_SIZE);
    query(connection, "CREATE TABLE strings(rid VARCHAR, value VARCHAR)");
    for (size_t index = 0; index < expected.size(); ++index) {
        query(connection, "INSERT INTO strings VALUES (" + literal(std::to_string(index)) + "," +
            literal(expected[index].value_or("null sentinel")) + ")");
    }
    const std::string statement = "SELECT rid, varchar_cpu(rid,value) AS returned_value FROM strings";
    report["sql"] = statement;
    report["explain"] = query(connection, "EXPLAIN " + statement)->ToString();
    report["query_start_monotonic_ns"] = now_ns();
    auto result = query(connection, statement);
    report["actual_rows"] = result->RowCount();
    report["actual_columns"] = result->ColumnCount();
    Json expected_rows = Json::array(), observed = Json::array();
    for (size_t index = 0; index < expected.size(); ++index) {
        expected_rows.push_back(Json::array({std::to_string(index), expected[index] ? Json(*expected[index]) : Json(nullptr)}));
    }
    for (duckdb::idx_t index = 0; index < result->RowCount(); ++index) {
        if (result->ColumnCount() != 2) throw std::runtime_error("prediction projection changed column count");
        const auto value = result->GetValue(1, index);
        observed.push_back(Json::array({result->GetValue(0, index).ToString(), value.IsNull() ? Json(nullptr) : Json(value.ToString())}));
    }
    report["result_ready_monotonic_ns"] = now_ns();
    std::sort(observed.begin(), observed.end());
    report["expected"] = expected_rows;
    report["observed"] = observed;
    report["passed"] = observed == expected_rows && result->ColumnCount() == 2;
    report["status"] = "query_returned";
    report["result_clock_scope"] = "complete relation copied before verification, file output and native destruction";
    save(output, report);
}

int main(int argc, char **argv) {
    if (argc != 5 && argc != 7) return 2;
    const std::string mode = argv[1], name = argv[2], output = argv[3];
    Json report = {{"schema", "semloom.imlane.varchar.cpu.v1"}, {"mode", mode}, {"case", name},
        {"declared_variant", argv[4]}, {"status", "failed"}, {"passed", false}, {"models", 0},
        {"GPU", false}, {"native_process_exit_verified", false}, {"performance_claim", false}};
    try {
        Dl_info location{};
        if (!dladdr(reinterpret_cast<void *>(&duckdb::ConverArrowAarrayToVector), &location))
            throw std::runtime_error("converter DSO symbol location unavailable");
        report["converter_resolved_DSO"] = location.dli_fname;
        report["library_source_id"] = duckdb::DuckDB::SourceID();
        if (mode == "converter" && argc == 5) {
            converter(name, report);
            report["status"] = "converter_returned";
            save(output, report);
        } else if (mode == "sql" && argc == 7 && (std::string(argv[5]) == "process" || std::string(argv[5]) == "thread")) {
            report["kind"] = argv[5];
            sql(name, argv[5], argv[6], output, report);
        } else throw std::invalid_argument("expected converter CASE OUTPUT VARIANT or sql CASE OUTPUT VARIANT KIND MODULE");
        return report["passed"].get<bool>() ? 0 : 1;
    } catch (IMLane::DBEnd::DBEndException &error) {
        report["error"] = error.Err();
    } catch (const std::exception &error) {
        report["error"] = error.what();
    } catch (...) {
        report["error"] = "nonstandard author exception";
    }
    try { save(output, report); }
    catch (const std::exception &error) { std::cerr << error.what() << '\n'; }
    return 1;
}
