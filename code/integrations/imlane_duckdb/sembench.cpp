// Prepared VARCHAR projection using the author's prediction operators and the
// separately qualified varchar-owned engineering DSO. No local model callback.
#include "duckdb.hpp"
#include "dbend/c/imlane_dbend.hpp"
#include "nlohmann/json.hpp"
#include "relation_handoff.hpp"

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <dlfcn.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <limits>
#include <limits.h>
#include <map>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

// Declaration only: including the converter implementation would defeat the
// check that this symbol actually comes from the separately qualified DSO.
struct ArrowArray;
struct ArrowSchema;
namespace duckdb {
void ConverArrowAarrayToVector(::ArrowArray *, ::ArrowSchema *, Vector &);
}

#ifndef SEMLOOM_EXPECTED_DUCKDB_DSO
#error "build sembench-driver with its verified DuckDB DSO identity"
#endif
#ifndef SEMLOOM_EXPECTED_DUCKDB_SHA256
#error "build sembench-driver with its verified DuckDB DSO digest"
#endif

namespace {
using Json = nlohmann::json;
constexpr const char *ALIAS = "imlane_prepared_text";

int64_t now_ns() {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value)) throw std::runtime_error("monotonic clock failed");
    return int64_t(value.tv_sec) * 1000000000 + value.tv_nsec;
}

Json library_identity() {
    Dl_info info{};
    char actual[PATH_MAX], expected[PATH_MAX];
    if (!dladdr(reinterpret_cast<void *>(&duckdb::ConverArrowAarrayToVector), &info)
            || !info.dli_fname || !realpath(info.dli_fname, actual)
            || !realpath(SEMLOOM_EXPECTED_DUCKDB_DSO, expected)
            || std::string(actual) != expected)
        throw std::runtime_error("VARCHAR converter did not resolve from the bound DuckDB DSO");
    return Json{{"variant", "varchar-owned-engineering"},
        {"author_commit", "072f8257db80569e42c9f5727d839587df529652"},
        {"converter_resolved_DSO", actual},
        {"expected_DSO_sha256", SEMLOOM_EXPECTED_DUCKDB_SHA256},
        {"library_version", duckdb::DuckDB::LibraryVersion()},
        {"library_source_id", duckdb::DuckDB::SourceID()}};
}

std::string literal(const std::string &text) {
    std::string out = "'";
    for (char value : text) out += value == '\'' ? "''" : std::string(1, value);
    return out + "'";
}

int64_t csv_maximum_record_bytes(const std::string &path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("cannot open prepared CSV for record sizing");
    int64_t longest = 0, length = 0;
    bool quoted = false;
    char value;
    // csv.writer doubles embedded quotes; only an unquoted LF ends a record.
    // Count raw bytes (including CRLF), without retaining records or fields.
    while (input.get(value)) {
        if (length == std::numeric_limits<int64_t>::max() - 1)
            throw std::runtime_error("prepared CSV record size cannot fit DuckDB BIGINT");
        ++length;
        if (length > longest) longest = length;
        if (value == '"') quoted = !quoted;
        if (value == '\n' && !quoted) length = 0;
    }
    if (input.bad()) throw std::runtime_error("cannot read prepared CSV for record sizing");
    return longest;
}

std::string prepared_source(const std::string &path, int64_t maximum_line_size) {
    return "read_csv(" + literal(path) +
        ", header=true, auto_detect=false, delim=',', columns={'call_id':'VARCHAR','messages_json':'VARCHAR'}, "
        "force_not_null=['call_id','messages_json'], maximum_line_size=" + std::to_string(maximum_line_size) + ")";
}

auto query(duckdb::Connection &connection, const std::string &sql) {
    auto result = connection.Query(sql);
    if (result->HasError()) result->ThrowError();
    return result;
}

void unexpected_local_call(duckdb::DataChunk &, duckdb::ExpressionState &, duckdb::Vector &) {
    throw std::runtime_error("native prediction unexpectedly used the local C++ callback");
}

void publish(const std::string &output, const Json &report) {
    const auto pending = output + ".pending-" + std::to_string(getpid());
    const int fd = open(pending.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0) throw std::runtime_error("cannot create fresh native relation file");
    try {
        const auto payload = report.dump(2) + "\n";
        size_t offset = 0;
        while (offset < payload.size()) {
            const auto written = write(fd, payload.data() + offset, payload.size() - offset);
            if (written < 0 && errno == EINTR) continue;
            if (written <= 0) throw std::runtime_error("cannot write native relation");
            offset += static_cast<size_t>(written);
        }
        if (fsync(fd)) throw std::runtime_error("cannot flush native relation");
    } catch (...) {
        close(fd);
        throw;
    }
    if (close(fd)) throw std::runtime_error("cannot close native relation file");
    if (link(pending.c_str(), output.c_str())) throw std::runtime_error("cannot publish fresh native relation");
    if (unlink(pending.c_str())) throw std::runtime_error("cannot remove native relation temporary link");
}

std::map<std::string, std::string> arguments(int argc, char **argv) {
    std::map<std::string, std::string> args;
    for (int i = 1; i < argc; i += 2) {
        const std::string key = argv[i];
        if (i + 1 == argc || (key != "--input" && key != "--module" && key != "--function"
                && key != "--kind" && key != "--output" && key != "--sink-release" && key != "--threads")
                || !args.emplace(key, argv[i + 1]).second)
            throw std::invalid_argument("invalid prepared prediction argument");
    }
    for (const auto *key : {"--input", "--module", "--function", "--kind", "--output", "--sink-release"})
        if (!args.count(key) || args.at(key).empty()) throw std::invalid_argument(std::string("missing ") + key);
    if (args.at("--kind") != "process" && args.at("--kind") != "thread")
        throw std::invalid_argument("kind must be process or thread");
    if (args.count("--threads")) {
        const auto &value = args.at("--threads");
        if (value.empty() || value.find_first_not_of("0123456789") != std::string::npos || std::stoull(value) == 0)
            throw std::invalid_argument("threads must be positive");
    }
    return args;
}

void execute(const std::map<std::string, std::string> &args, Json &report, bool &published) {
    report.update(library_identity());
    const auto kind = args.at("--kind") == "thread" ? duckdb::FunctionKind::THREAD_SCHEDULE_PREDICTION
                                                      : duckdb::FunctionKind::PROCESS_PREDICTION;
    IMLane::DBEnd::DBEndContext registration;
    const auto checked = [](const IMLane::DBEnd::ContextStatus &status) {
        if (status.kind != IMLane::DBEnd::StatusKind::OK) throw std::runtime_error(status.info);
    };
    checked(registration.Setup());
    checked(registration.RegisterFunction(args.at("--module"), args.at("--function"), ALIAS));
    duckdb::DuckDB database(nullptr);
    duckdb::Connection connection(database);
    if (args.count("--threads")) query(connection, "SET threads = " + args.at("--threads"));
    connection.CreateVectorizedFunction<duckdb::string_t, duckdb::string_t, duckdb::string_t>(
        ALIAS, unexpected_local_call, duckdb::LogicalType::INVALID, kind, DEFAULT_PREDICTION_BATCH_SIZE);
    report.update(Json{{"kind", args.at("--kind")}, {"duckdb_threads", database.NumberOfThreads()},
        {"registered_batch_size", DEFAULT_PREDICTION_BATCH_SIZE}, {"return_type", "VARCHAR"}});
    const auto record_bytes = csv_maximum_record_bytes(args.at("--input"));
    const auto maximum_line_size = record_bytes + 1;
    report["input_csv"] = Json{{"maximum_record_bytes", record_bytes}, {"maximum_line_size", maximum_line_size}};
    query(connection, "CREATE VIEW prepared AS SELECT * FROM " + prepared_source(args.at("--input"), maximum_line_size));
    auto counts = query(connection, "SELECT count(*), count(DISTINCT call_id) FROM prepared");
    const auto rows = counts->GetValue<int64_t>(0, 0);
    if (counts->GetValue<int64_t>(1, 0) != rows) throw std::runtime_error("native occurrences are not unique");
    report["source_rows"] = rows;
    const std::string sql = "SELECT call_id, imlane_prepared_text(call_id,messages_json) AS generated_text FROM prepared";
    report["sql"] = sql;
    report["explain"] = query(connection, "EXPLAIN " + sql)->ToString();
    const auto started = now_ns();
    auto result = query(connection, sql);
    if (result->ColumnCount() != 2 || result->RowCount() != rows)
        throw std::runtime_error("native VARCHAR projection changed occurrence cardinality");
    report["mapped_occurrences"] = Json::array();
    for (duckdb::idx_t row = 0; row < result->RowCount(); ++row) {
        const auto text = result->GetValue(1, row);
        if (text.IsNull()) throw std::runtime_error("native model returned NULL instead of its original text");
        report["mapped_occurrences"].push_back(Json{{"call_id", result->GetValue(0, row).ToString()},
            {"generated_text", text.ToString()}});
    }
    const auto ready = now_ns();
    semloom_imlane::RelationHandoff handoff(args.at("--sink-release"), ready);
    report.update(Json{{"status", "query_returned"}, {"full_projection_materialized", true},
        {"result_ready_monotonic_ns", ready}, {"result_clock", "CLOCK_MONOTONIC"},
        {"native_projection_seconds", (ready - started) / 1e9},
        {"sink_handoff", {{"protocol", semloom_imlane::RELATION_ACK_PROTOCOL}, {"release_path", args.at("--sink-release")}}}});
    publish(args.at("--output"), report);
    published = true;
    handoff.Wait(std::chrono::seconds(120));
}
} // namespace

int main(int argc, char **argv) {
    Json report = {{"schema", "semloom.imlane.prepared_text.v1"}, {"status", "failed"}};
    std::string output;
    bool published = false;
    try {
        if (argc == 3 && std::string(argv[1]) == "--probe-dso") {
            publish(argv[2], library_identity());
            return 0;
        }
        const auto args = arguments(argc, argv);
        output = args.at("--output");
        struct stat existing{};
        if (lstat(output.c_str(), &existing) == 0 || errno != ENOENT)
            throw std::runtime_error("native projection requires a fresh output path");
        execute(args, report, published);
        return 0;
    } catch (IMLane::DBEnd::DBEndException &error) {
        report["error"] = error.Err();
    } catch (const std::exception &error) {
        report["error"] = error.what();
    }
    std::cerr << report.dump() << '\n';
    if (!published && !output.empty()) {
        report["status"] = "failed";
        try { publish(output, report); }
        catch (const std::exception &error) { std::cerr << "failure publication: " << error.what() << '\n'; }
    }
    return 1;
}
