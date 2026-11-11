// One original SemBench MMQA Q3/Q6 Filter through the pinned author's DuckDB prediction operator.
// The coordinator, Python executor and model are owned by the outer run scope.
#include "duckdb.hpp"
#include "dbend/c/imlane_dbend.hpp"
#include "nlohmann/json.hpp"
#include "relation_handoff.hpp"
#include <memory>
#include <dlfcn.h>
#include <limits.h>
#include <cstdlib>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

#ifndef SEMLOOM_EXPECTED_DUCKDB_DSO
#error "build filter-driver with the original-author DSO binding"
#endif
#ifndef SEMLOOM_EXPECTED_DUCKDB_SHA256
#error "build filter-driver with the original-author DSO digest"
#endif

namespace {
using Json = nlohmann::json;
using Clock = std::chrono::steady_clock;
constexpr const char *alias = "imlane_mmqa_filter";
constexpr const char *author_commit = "072f8257db80569e42c9f5727d839587df529652";

Json library_identity() {
    Dl_info info{};
    char actual[PATH_MAX], expected[PATH_MAX];
    if (!dladdr(reinterpret_cast<void *>(&duckdb::DuckDB::LibraryVersion), &info)
            || !info.dli_fname || !realpath(info.dli_fname, actual)
            || !realpath(SEMLOOM_EXPECTED_DUCKDB_DSO, expected)
            || std::string(actual) != expected)
        throw std::runtime_error("Filter resolved a different DuckDB DSO");
    return Json{{"variant", "original-author"}, {"author_commit", author_commit},
        {"converter_resolved_DSO", actual},
        {"expected_DSO_sha256", SEMLOOM_EXPECTED_DUCKDB_SHA256}};
}

double seconds_since(Clock::time_point start) {
    return std::chrono::duration<double>(Clock::now() - start).count();
}

std::string literal(const std::string &text) {
    std::string result = "'";
    for (char character : text) {
        result += character == '\'' ? "''" : std::string(1, character);
    }
    return result + "'";
}

std::map<std::string, std::string> arguments(int argc, char **argv) {
    const std::string usage = "mmqa-filter --input CSV --module PY --function NAME "
                              "--kind process|thread --output JSON [--threads N] [--sink-release PATH]";
    std::map<std::string, std::string> result;
    for (int index = 1; index < argc; index += 2) {
        const std::string key = argv[index];
        if (index + 1 == argc || (key != "--input" && key != "--module" &&
            key != "--function" && key != "--kind" && key != "--output" && key != "--threads" && key != "--sink-release") ||
            !result.emplace(key, argv[index + 1]).second) {
            throw std::invalid_argument(usage);
        }
    }
    for (const char *name : {"--input", "--module", "--function", "--kind", "--output", "--sink-release"}) {
        if (!result.count(name) || result.at(name).empty()) throw std::invalid_argument(usage);
    }
    if (result.at("--kind") != "process" && result.at("--kind") != "thread") {
        throw std::invalid_argument("kind must be process or thread; SCHEDULE is not an implemented filter");
    }
    if (result.count("--threads")) {
        const auto &value = result.at("--threads");
        if (value.empty() || value.find_first_not_of("0123456789") != std::string::npos ||
            std::stoull(value) == 0) throw std::invalid_argument("threads must be a positive integer");
    }
    return result;
}

void check(const IMLane::DBEnd::ContextStatus &status) {
    if (status.kind != IMLane::DBEnd::StatusKind::OK) throw std::runtime_error(status.info);
}

duckdb::unique_ptr<duckdb::MaterializedQueryResult> query(duckdb::Connection &connection,
                                                        const std::string &sql) {
    auto result = connection.Query(sql);
    if (result->HasError()) result->ThrowError();
    return result;
}

void unexpected_local_call(duckdb::DataChunk &, duckdb::ExpressionState &, duckdb::Vector &) {
    throw std::runtime_error("native IMLane prediction unexpectedly used the local C++ callback");
}

void save(int descriptor, const Json &report) {
    const auto payload = report.dump(2) + "\n";
    size_t offset = 0;
    while (offset < payload.size()) {
        const auto count = ::write(descriptor, payload.data() + offset, payload.size() - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) throw std::runtime_error("cannot write native MMQA Filter result: " + std::string(std::strerror(errno)));
        offset += static_cast<size_t>(count);
    }
    if (::fsync(descriptor)) throw std::runtime_error("cannot flush native MMQA Filter result");
}

void publish(int descriptor, const std::string &pending, const std::string &output, const Json &report) {
    save(descriptor, report);
    if (::link(pending.c_str(), output.c_str())) throw std::runtime_error("cannot publish fresh native MMQA Filter result");
    if (::unlink(pending.c_str())) throw std::runtime_error("cannot remove published native MMQA Filter temporary link");
}

void execute(const std::map<std::string, std::string> &args, Json &report, int descriptor,
             const std::string &pending, bool &write_attempted) {
    const auto start = Clock::now();
    const bool threaded = args.at("--kind") == "thread";
    const auto kind = threaded ? duckdb::FunctionKind::THREAD_SCHEDULE_PREDICTION
                               : duckdb::FunctionKind::PROCESS_PREDICTION;
    report.update(library_identity());
    report.update(Json{{"author_commit", author_commit}, {"registered_alias", alias},
        {"registration_completed", false},
        {"module_function", args.at("--function")},
        {"kind", threaded ? "THREAD_SCHEDULE_PREDICTION" : "PROCESS_PREDICTION"},
        {"library_version", duckdb::DuckDB::LibraryVersion()},
        {"library_source_id", duckdb::DuckDB::SourceID()},
        {"standard_vector_size", duckdb::DuckDB::StandardVectorSize()},
        {"registered_batch_size", DEFAULT_PREDICTION_BATCH_SIZE},
        {"registered_batch_size_used_by_selected_filter", false},
        {"native_ai_query_attempts", 0}, {"native_ai_query_returns", 0}, {"process_exit_verified", false},
        {"input_columns", {"occurrence_id", "title", "text"}},
        {"arrow_columns", {"c0", "c1", "c2"}}, {"return_type", "TINYINT"}});

    // DuckDB's own prediction contexts also call Setup() with this fixed default.
    // The outer scope must lease and qualify this coordinator endpoint first.
    IMLane::DBEnd::DBEndContext registration;
    check(registration.Setup());
    check(registration.RegisterFunction(args.at("--module"), args.at("--function"), alias));
    report["registration_completed"] = true;
    report["registration_seconds"] = seconds_since(start);

    duckdb::DuckDB database(nullptr);
    duckdb::Connection connection(database);
    if (args.count("--threads")) query(connection, "SET threads = " + args.at("--threads"));
    report["duckdb_threads"] = database.NumberOfThreads();
    connection.CreateVectorizedFunction<int8_t, duckdb::string_t, duckdb::string_t,
        duckdb::string_t>(alias, unexpected_local_call,
        duckdb::LogicalType::INVALID, kind, DEFAULT_PREDICTION_BATCH_SIZE);

    // This is the complete gold-free source, not a Python-filtered eligible table.
    // force_not_null preserves CSV empty strings from the original source contract.
    const auto source = "read_csv(" + literal(args.at("--input")) +
        ", header=true, auto_detect=false, delim=',', columns={"
        "'occurrence_id':'VARCHAR','title':'VARCHAR','text':'VARCHAR','source_position':'BIGINT'}, "
        "force_not_null=['occurrence_id','title','text'])";
    query(connection, "CREATE VIEW mmqa_text AS SELECT * FROM " + source);
    const auto counts_start = Clock::now();
    auto counts = query(connection, "SELECT count(*), count(DISTINCT occurrence_id) FROM mmqa_text");
    const auto source_rows = counts->GetValue<int64_t>(0, 0);
    report["source_rows"] = source_rows;
    report["eligible_rows"] = source_rows;
    if (source_rows != 25 || counts->GetValue<int64_t>(1, 0) != source_rows) {
        throw std::runtime_error("native MMQA Filter requires exactly 25 unique original occurrences");
    }
    report["ordinary_count_seconds"] = seconds_since(counts_start);

    const std::string sql = "SELECT occurrence_id,title,source_position FROM mmqa_text "
        "WHERE imlane_mmqa_filter(occurrence_id,title,text)=1";
    report["sql"] = sql;
    const auto explain_start = Clock::now();
    auto explanation = query(connection, "EXPLAIN " + sql);
    report["explain"] = Json::array();
    for (duckdb::idx_t row = 0; row < explanation->RowCount(); ++row) {
        report["explain"].push_back(Json{
            {"key", explanation->GetValue(0, row).ToString()},
            {"plan", explanation->GetValue(1, row).ToString()}});
    }
    report["explain_seconds"] = seconds_since(explain_start);

    const auto query_start = Clock::now();
    report["native_ai_query_attempts"] = 1;
    auto result = query(connection, sql);
    report["native_ai_query_returns"] = 1;
    if (result->ColumnCount() != 3) throw std::runtime_error("MMQA Filter filter lost occurrence identity");
    report["native_filter_materialized_seconds"] = seconds_since(query_start);
    report["accepted_occurrences"] = Json::array();
    for (duckdb::idx_t row = 0; row < result->RowCount(); ++row) {
        report["accepted_occurrences"].push_back(Json{{"occurrence_id",result->GetValue(0,row).ToString()},
            {"title",result->GetValue(1,row).ToString()},{"source_position",result->GetValue<int64_t>(2,row)}});
    }
    report["positive_title_count"] = result->RowCount();
    report["full_filter_materialized_before_title_sink"] = true;
    report["native_query_seconds"] = seconds_since(query_start);
    report["driver_until_result_seconds"] = seconds_since(start);
    report["status"] = "query_returned";
    // Python's qualified Linux worker uses this same clock for the single
    // post-result cleanup allowance, including these native destructors.
    struct timespec ready;
    if (::clock_gettime(CLOCK_MONOTONIC, &ready) == 0) {
        report["result_ready_monotonic_ns"] = int64_t(ready.tv_sec) * 1000000000 + ready.tv_nsec;
    } else {
        report["result_ready_monotonic_ns"] = nullptr;
        report["result_clock_error_errno"] = errno;
    }
    report["result_clock"] = "CLOCK_MONOTONIC";
    std::unique_ptr<semloom_imlane::RelationHandoff> sink_handoff;
    if (args.count("--sink-release")) {
        sink_handoff = std::make_unique<semloom_imlane::RelationHandoff>(
            args.at("--sink-release"), report["result_ready_monotonic_ns"].get<int64_t>());
        report["sink_handoff"] = Json{{"protocol", semloom_imlane::RELATION_ACK_PROTOCOL},
            {"release_path", args.at("--sink-release")}};
    }
    // Publish the count before native destructors: a later crash/timeout must not
    // erase returned predictions. The supervisor verifies actual process exit.
    write_attempted = true;
    publish(descriptor, pending, args.at("--output"), report);
    if (sink_handoff) sink_handoff->Wait(std::chrono::seconds(120));
}
} // namespace

int main(int argc, char **argv) {
    Json report = {{"schema", "semloom.imlane-duckdb-mmqa-filter.v1"}, {"status", "failed"}};
    int descriptor = -1;
    bool write_attempted = false;
    std::string pending, output;
    try {
        if (argc == 3 && std::string(argv[1]) == "--probe-dso") {
            descriptor = ::open(argv[2], O_WRONLY | O_CREAT | O_EXCL, 0600);
            if (descriptor < 0) throw std::runtime_error("fresh DSO probe required");
            save(descriptor, library_identity());
            if (::close(descriptor)) throw std::runtime_error("cannot close DSO probe");
            return 0;
        }
        const auto args = arguments(argc, argv);
        output = args.at("--output");
        struct stat existing;
        if (::lstat(output.c_str(), &existing) == 0 || errno != ENOENT) {
            throw std::runtime_error("native MMQA Filter requires a fresh result path");
        }
        pending = output + ".pending-" + std::to_string(::getpid());
        descriptor = ::open(pending.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (descriptor < 0) throw std::runtime_error("cannot create fresh native MMQA Filter result file");
        execute(args, report, descriptor, pending, write_attempted);
        const int closed = ::close(descriptor);
        descriptor = -1;
        if (closed) throw std::runtime_error("cannot close native MMQA Filter result file");
        return 0;
    } catch (IMLane::DBEnd::DBEndException &error) {
        std::cerr << "native MMQA Filter author error: " << error.Err() << '\n';
        report["status"] = "failed";
        report["error"] = error.Err();
    } catch (const std::exception &error) {
        std::cerr << "native MMQA Filter failed: " << error.what() << '\n';
        report["status"] = "failed";
        report["error"] = error.what();
    } catch (...) {
        std::cerr << "native MMQA Filter failed with a non-standard author exception\n";
        report["status"] = "failed";
        report["error"] = "non-standard author exception; inspect coordinator/executor records";
    }
    if (descriptor >= 0) {
        if (!write_attempted) {
            try { publish(descriptor, pending, output, report); }
            catch (const std::exception &error) { std::cerr << "native MMQA Filter result write also failed: " << error.what() << '\n'; }
        }
        ::close(descriptor);
    }
    return 1;
}
