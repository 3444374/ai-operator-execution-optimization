#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <unordered_set>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "dbend/cpp/imlane_dbend.hpp"

using Clock = std::chrono::steady_clock;
using Table = std::shared_ptr<arrow::Table>;
uint64_t elapsed_ns(Clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
}
struct TaskMetrics {
    uint64_t author_execute_inclusive_ns = 0, output_copy_encode_ns = 0, output_copy_decode_ns = 0;
    size_t author_calls = 0, output_copies = 0, output_wire_limit = 0;
};
thread_local TaskMetrics* active_task_metrics = nullptr;
struct TaskMetricsScope {
    TaskMetrics* previous;
    explicit TaskMetricsScope(TaskMetrics& value): previous(active_task_metrics) { active_task_metrics = &value; }
    ~TaskMetricsScope() { active_task_metrics = previous; }
};
void validate_output(const Table& output) {
    if (!output || !output->ValidateFull().ok() || output->num_rows() != 1 || output->ColumnNames() !=
            std::vector<std::string>{"call_id", "row_id", "native_outcome"} ||
            !output->schema()->field(0)->type()->Equals(arrow::utf8()) ||
            !output->schema()->field(1)->type()->Equals(arrow::int64()) ||
            !output->schema()->field(2)->type()->Equals(arrow::utf8()))
        throw std::runtime_error("complete-call output shape differs");
}

// Copy the output before the author's ExecuteFunction frees Lane storage.
namespace IMLane::DBEnd {
template<> class TableConverter<arrow::Table, std::shared_ptr<arrow::Table>> {
public:
    std::shared_ptr<arrow::Table> ConvertToExchange(arrow::Table* table) {
        return std::shared_ptr<arrow::Table>(table, [](arrow::Table*) {});
    }
    void ConvertFromExchange(std::shared_ptr<arrow::Table> table,
                             std::shared_ptr<arrow::Table>& out) {
        auto start = Clock::now();
        validate_output(table);
        auto identity = std::static_pointer_cast<arrow::StringScalar>(
            table->column(0)->GetScalar(0).ValueOrDie());
        auto row = std::static_pointer_cast<arrow::Int64Scalar>(
            table->column(1)->GetScalar(0).ValueOrDie());
        auto result = std::static_pointer_cast<arrow::StringScalar>(
            table->column(2)->GetScalar(0).ValueOrDie());
        if (!identity->is_valid || identity->value->size() > 4096 || !row->is_valid || row->value < 0)
            throw std::runtime_error("complete-call output changed occurrence identity");
        if (!result->is_valid) throw std::runtime_error("complete-call output is NULL");
        if (!active_task_metrics) throw std::runtime_error("complete-call task metrics are unavailable");
        if (result->value->size() > static_cast<int64_t>(active_task_metrics->output_wire_limit))
            throw std::invalid_argument("complete-call output wire exceeds its finite allowance");
        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, table->schema()).ValueOrDie();
        if (!writer->WriteTable(*table).ok() || !writer->Close().ok())
            throw std::runtime_error("complete-call output copy failed");
        auto buffer = stream->Finish().ValueOrDie();
        active_task_metrics->output_copy_encode_ns += elapsed_ns(start);
        start = Clock::now();
        auto reader = arrow::ipc::RecordBatchStreamReader::Open(
            std::make_shared<arrow::io::BufferReader>(buffer)).ValueOrDie();
        out = reader->ToTable().ValueOrDie();
        active_task_metrics->output_copy_decode_ns += elapsed_ns(start);
        ++active_task_metrics->output_copies;
    }
};
}
namespace dbend = IMLane::DBEnd;

void check(const arrow::Status& value) {
    if (!value.ok()) throw std::runtime_error(value.ToString());
}
void check(const dbend::ContextStatus& value) {
    if (value.kind == dbend::StatusKind::ERROR) throw std::runtime_error(value.info);
}
size_t decoded_size(const std::string& value) {
    if (value.size() % 4 != 0) throw std::invalid_argument("complete-call base64 size is invalid");
    size_t size = value.size() / 4 * 3;
    if (!value.empty() && value.back() == '=') --size;
    if (value.size() > 1 && value[value.size() - 2] == '=') --size;
    return size;
}
struct BridgeLimits {
    size_t max_batch_rows = 128;
    size_t max_input_payload_bytes = 32 * 1024 * 1024;
    size_t max_input_wire_bytes = 48 * 1024 * 1024;
    size_t max_item_payload_bytes = 8 * 1024 * 1024;
    size_t max_pending_calls = 128;
    size_t max_batch_response_bytes = 32 * 1024 * 1024;

    nlohmann::json json() const {
        return {{"max_batch_rows", max_batch_rows}, {"max_input_payload_bytes", max_input_payload_bytes},
                {"max_input_wire_bytes", max_input_wire_bytes}, {"max_item_payload_bytes", max_item_payload_bytes},
                {"max_pending_calls", max_pending_calls}, {"max_batch_response_bytes", max_batch_response_bytes}};
    }
};
struct BatchMetrics {
    uint64_t input_read_parse_ns = 0, input_arrow_ns = 0, context_setup_ns = 0, function_register_ns = 0;
    uint64_t author_execute_inclusive_ns = 0, output_copy_encode_ns = 0, output_copy_decode_ns = 0;
    uint64_t output_wire_ns = 0, stdout_write_ns = 0, batch_wall_ns = 0;
    size_t context_creations = 0, registrations = 0, author_calls = 0, output_copies = 0;

    nlohmann::json timing() const {
        return {{"input_read_parse_ns", input_read_parse_ns}, {"input_arrow_ns", input_arrow_ns},
                {"context_setup_ns", context_setup_ns}, {"function_register_ns", function_register_ns},
                {"author_execute_inclusive_ns", author_execute_inclusive_ns},
                {"output_copy_encode_ns", output_copy_encode_ns}, {"output_copy_decode_ns", output_copy_decode_ns},
                {"output_wire_ns", output_wire_ns}, {"stdout_write_ns", stdout_write_ns}, {"batch_wall_ns", batch_wall_ns}};
    }
    nlohmann::json lifecycle() const {
        return {{"context_creations", context_creations}, {"registrations", registrations},
                {"author_calls", author_calls}, {"output_copies", output_copies}};
    }
    void collect(const TaskMetrics& task) {
        author_execute_inclusive_ns += task.author_execute_inclusive_ns;
        output_copy_encode_ns += task.output_copy_encode_ns;
        output_copy_decode_ns += task.output_copy_decode_ns;
        author_calls += task.author_calls; output_copies += task.output_copies;
    }
};
size_t bounded_argument(const char* text, size_t maximum) {
    std::string value(text); size_t used = 0;
    auto number = std::stoull(value, &used);
    if (value.empty() || value[0] == '-' || used != value.size() || number == 0 || number > maximum)
        throw std::invalid_argument("complete-call capacity argument is invalid");
    return static_cast<size_t>(number);
}
Table input(const std::string& wire, const nlohmann::json& value) {
    arrow::StringBuilder ids, calls; arrow::Int64Builder rows;
    check(ids.Append(value.at("call_id").get<std::string>()));
    check(rows.Append(value.at("row_id").get<int64_t>())); check(calls.Append(wire));
    std::shared_ptr<arrow::Array> a, b, c;
    check(ids.Finish(&a)); check(rows.Finish(&b)); check(calls.Finish(&c));
    return arrow::Table::Make(arrow::schema({arrow::field("call_id", arrow::utf8()),
        arrow::field("row_id", arrow::int64()), arrow::field("native_call", arrow::utf8())}), {a,b,c});
}
bool read_bounded_line(FILE* stream, std::string& line, size_t maximum) {
    line.clear();
    int next;
    // The main bridge thread exclusively owns this buffered FILE.
    while ((next = getc_unlocked(stream)) != EOF) {
        if (next == '\n') return true;
        if (line.size() == maximum)
            throw std::invalid_argument("complete-call input line exceeds its wire allowance");
        line.push_back(static_cast<char>(next));
    }
    if (std::ferror(stream)) throw std::runtime_error("complete-call input read failed");
    return !line.empty();
}
struct Call {
    Table input, output;
    std::string call_id;
    int64_t row_id;
    size_t response_max;
    TaskMetrics metrics;
};
struct Batch {
    std::vector<std::shared_ptr<Call>> calls;
    size_t input_bytes = 0, payload_bytes = 0;
    BatchMetrics metrics;
};
Batch read_batch(FILE* stream, const BridgeLimits& limits) {
    Batch batch; std::string line; std::unordered_set<std::string> identities;
    auto start = Clock::now();
    while (read_bounded_line(stream, line, batch.input_bytes < limits.max_input_wire_bytes
            ? limits.max_input_wire_bytes - batch.input_bytes - 1 : 0)) {
        batch.input_bytes += line.size() + 1;
        if (batch.calls.size() >= limits.max_batch_rows || batch.input_bytes > limits.max_input_wire_bytes)
            throw std::invalid_argument("complete-call total rows or stdin wire bytes exceed the selected capacity; author submission has not started");
        auto value = nlohmann::json::parse(line);
        auto bytes = decoded_size(value.at("payload").get<std::string>());
        batch.payload_bytes += bytes;
        if (bytes == 0 || bytes > limits.max_item_payload_bytes || batch.payload_bytes > limits.max_input_payload_bytes)
            throw std::invalid_argument("complete-call payload bytes exceed the selected capacity; author submission has not started");
        auto identity = value.at("call_id").get<std::string>();
        if (identity.empty() || identity.size() > 4096 || !identities.insert(identity).second)
            throw std::invalid_argument("complete-call occurrence identity is invalid; author submission has not started");
        auto response_max = value.at("max_response_bytes").get<size_t>();
        if (response_max == 0 || response_max > 8 * 1024 * 1024)
            throw std::invalid_argument("complete-call response capacity is invalid; author submission has not started");
        if (!value.at("row_id").is_number_integer())
            throw std::invalid_argument("complete-call row identity is invalid; author submission has not started");
        auto row_id = value.at("row_id").get<int64_t>();
        if (row_id < 0) throw std::invalid_argument("complete-call row identity is invalid; author submission has not started");
        auto arrow_start = Clock::now();
        auto table = input(line, value);
        batch.metrics.input_arrow_ns += elapsed_ns(arrow_start);
        auto call = std::make_shared<Call>(Call{table, nullptr, identity, row_id, response_max, {}});
        // Finite base64 response plus worst-case JSON escaping of identities/diagnostics.
        call->metrics.output_wire_limit = (response_max + 2) / 3 * 4 + 6 * (4096 + 8192) + 1024;
        batch.calls.push_back(std::move(call));
    }
    batch.metrics.input_read_parse_ns = elapsed_ns(start) - batch.metrics.input_arrow_ns;
    return batch;
}
using File = std::unique_ptr<FILE, int(*)(FILE*)>;
File batch_input(const std::string& batch_id, const std::string& path, const BridgeLimits& limits) {
    auto directory = "batch-" + batch_id;
    if (path != directory + "/input.jsonl")
        throw std::invalid_argument("complete-call session input path differs from its batch identity");
    auto dir = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    if (dir < 0) throw std::invalid_argument("complete-call session input directory is unavailable");
    auto fd = ::openat(dir, "input.jsonl", O_RDONLY | O_NOFOLLOW | O_NONBLOCK);
    ::close(dir);
    if (fd < 0) throw std::invalid_argument("complete-call session input file is unavailable");
    struct stat info;
    if (::fstat(fd, &info) || !S_ISREG(info.st_mode) || info.st_size < 0 ||
            static_cast<uint64_t>(info.st_size) > limits.max_input_wire_bytes) {
        ::close(fd);
        throw std::invalid_argument("complete-call session input file exceeds its finite allowance");
    }
    auto stream = ::fdopen(fd, "r");
    if (!stream) { ::close(fd); throw std::runtime_error("complete-call session input open failed"); }
    return File(stream, std::fclose);
}
void write_frame(const char* prefix, const std::string& wire, BatchMetrics* metrics = nullptr) {
    auto start = Clock::now();
    std::cout << prefix << wire << std::endl;
    if (!std::cout) throw std::runtime_error("complete-call stdout write failed");
    if (metrics) metrics->stdout_write_ns += elapsed_ns(start);
}
nlohmann::json execute_batch(const std::shared_ptr<dbend::DBEndContext>& context, Batch& batch,
        const BridgeLimits& limits, const std::string& batch_id, Clock::time_point start) {
    auto& calls = batch.calls;
    std::vector<std::unique_ptr<dbend::ExecAsyncFuncContext>> pending(calls.size());
    size_t finished = 0, submitted = 0, held = 0, pending_peak = 0, delivered_bytes = 0;
    auto supply = [&] {
        while (submitted < calls.size() && held < limits.max_pending_calls) {
            auto call = calls[submitted];
            pending[submitted] = context->ExecuteAsyncFuncPush([context, call] {
                TaskMetricsScope scope(call->metrics);
                auto begin = Clock::now();
                ++call->metrics.author_calls;
                check(context->ExecuteFunction("sema_complete_http", *call->input, call->output));
                call->metrics.author_execute_inclusive_ns = elapsed_ns(begin);
            });
            ++submitted; ++held; pending_peak = std::max(pending_peak, held);
        }
    };
    supply();
    while (finished != pending.size()) {
        for (size_t i = 0; i < pending.size(); ++i) {
            if (!pending[i] || !pending[i]->ExecuteAsyncFuncTryPull()) continue;
            pending[i]->task.get();
            batch.metrics.collect(calls[i]->metrics);
            auto wire_start = Clock::now();
            auto output = calls[i]->output;
            validate_output(output);
            auto identity = std::static_pointer_cast<arrow::StringScalar>(output->column(0)->GetScalar(0).ValueOrDie());
            auto row = std::static_pointer_cast<arrow::Int64Scalar>(output->column(1)->GetScalar(0).ValueOrDie());
            auto result = std::static_pointer_cast<arrow::StringScalar>(output->column(2)->GetScalar(0).ValueOrDie());
            if (!identity->is_valid || !row->is_valid || !result->is_valid ||
                    identity->ToString() != calls[i]->call_id || row->value != calls[i]->row_id)
                throw std::runtime_error("complete-call output changed occurrence identity");
            auto wire = result->ToString();
            auto outcome = nlohmann::json::parse(wire);
            if (outcome.at("call_id").get<std::string>() != calls[i]->call_id ||
                    outcome.at("row_id").get<int64_t>() != calls[i]->row_id)
                throw std::runtime_error("complete-call output wire changed occurrence identity");
            if (outcome.value("transport_error", std::string()).size() > 4096 ||
                    outcome.value("execution_error", std::string()).size() > 4096)
                throw std::invalid_argument("complete-call output diagnostics exceed their finite allowance");
            auto bytes = outcome.at("response").is_null()
                ? outcome.value("transport_error", std::string()).size() + outcome.value("execution_error", std::string()).size()
                : decoded_size(outcome.at("response").get<std::string>());
            delivered_bytes += bytes;
            if (bytes > calls[i]->response_max || delivered_bytes > limits.max_batch_response_bytes)
                throw std::invalid_argument("complete-call responses exceed the selected byte capacity");
            if (!batch_id.empty()) wire = nlohmann::json({{"batch_id", batch_id}, {"outcome", outcome}}).dump();
            batch.metrics.output_wire_ns += elapsed_ns(wire_start);
            write_frame("SEMA_IMLANE_RESULT ", wire, &batch.metrics);
            pending[i].reset(); calls[i].reset();
            ++finished; --held;
            supply();
        }
        if (finished != pending.size()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (held != 0 || submitted != finished || batch.metrics.output_copies != finished)
        throw std::runtime_error("complete-call local batch fence is incomplete");
    batch.metrics.batch_wall_ns = elapsed_ns(start);
    return {{"submitted", submitted}, {"delivered", finished}, {"pending_peak", pending_peak},
            {"input_payload_bytes", batch.payload_bytes}, {"stdin_wire_bytes", batch.input_bytes},
            {"delivered_response_bytes", delivered_bytes}, {"timing_ns", batch.metrics.timing()},
            {"lifecycle", batch.metrics.lifecycle()}};
}

int main(int argc, char** argv) {
    bool session = argc > 1 && std::string(argv[argc - 1]) == "--session";
    auto arguments = argc - (session ? 1 : 0);
    if (arguments != 4 && arguments != 10) {
        std::cerr << "usage: imlane_http PORT UDF_PATH NATIVE_TASK_THREADS [MAX_ROWS MAX_PAYLOAD MAX_WIRE MAX_ITEM MAX_PENDING MAX_OUTPUT] [--session]\n";
        return 2;
    }
    try {
        BridgeLimits limits;
        if (arguments == 10) {
            limits.max_batch_rows = bounded_argument(argv[4], 4096);
            limits.max_input_payload_bytes = bounded_argument(argv[5], 32 * 1024 * 1024);
            limits.max_input_wire_bytes = bounded_argument(argv[6], 48 * 1024 * 1024);
            limits.max_item_payload_bytes = bounded_argument(argv[7], 8 * 1024 * 1024);
            limits.max_pending_calls = bounded_argument(argv[8], 128);
            limits.max_batch_response_bytes = bounded_argument(argv[9], 32 * 1024 * 1024);
        }
        auto start = Clock::now();
        Batch batch;
        if (!session) batch = read_batch(stdin, limits);
        auto setup_start = Clock::now();
        auto context = std::make_shared<dbend::DBEndContext>();
        check(context->Setup("127.0.0.1", std::stoi(argv[1])));
        batch.metrics.context_setup_ns = elapsed_ns(setup_start);
        batch.metrics.context_creations = 1;
        auto register_start = Clock::now();
        check(context->RegisterFunction(argv[2], "complete_http", "sema_complete_http"));
        batch.metrics.function_register_ns = elapsed_ns(register_start);
        batch.metrics.registrations = 1;
        auto executors = context->GetSettings().n_executors;
        auto threads = dbend::TaskThreadPool::instance(executors).sys_cpu_core_nums;
        write_frame("SEMA_IMLANE_META ", nlohmann::json({{"native_executors", executors}, {"native_task_threads", threads}}).dump());
        write_frame("SEMA_IMLANE_LIMITS ", limits.json().dump());
        if (!session && batch.calls.empty() && std::stoi(argv[3]) == 0) return 0;
        if (threads != std::stoi(argv[3]))
            throw std::runtime_error("author task thread count differs from explicit configuration");
        if (!session) {
            write_frame("SEMA_IMLANE_STATS ", execute_batch(context, batch, limits, "", start).dump());
            return 0;
        }
        write_frame("SEMA_IMLANE_SESSION ", "{\"protocol\":1}");
        constexpr size_t MAX_SESSION_BATCHES = 4096;
        std::unordered_set<std::string> batches;
        std::string line;
        while (read_bounded_line(stdin, line, 1024)) {
            auto frame = nlohmann::json::parse(line);
            if (!frame.is_object()) throw std::invalid_argument("complete-call session frame is not an object");
            auto kind = frame.at("kind").get<std::string>();
            if (kind == "close" && frame.size() == 1) {
                write_frame("SEMA_IMLANE_SESSION_END ", "{\"protocol\":1}");
                return 0;
            }
            if (kind != "batch" || frame.size() != 3)
                throw std::invalid_argument("complete-call session frame has invalid fields");
            auto batch_id = frame.at("batch_id").get<std::string>();
            if (batch_id.size() != 32 || !std::all_of(batch_id.begin(), batch_id.end(), [](char value) {
                    return (value >= '0' && value <= '9') || (value >= 'a' && value <= 'f');
                }) || batches.size() == MAX_SESSION_BATCHES || !batches.insert(batch_id).second)
                throw std::invalid_argument("complete-call session batch identity is invalid or exhausted");
            start = Clock::now();
            auto file = batch_input(batch_id, frame.at("input_path").get<std::string>(), limits);
            auto next = read_batch(file.get(), limits);
            next.metrics.context_setup_ns = batch.metrics.context_setup_ns;
            next.metrics.function_register_ns = batch.metrics.function_register_ns;
            next.metrics.context_creations = batch.metrics.context_creations;
            next.metrics.registrations = batch.metrics.registrations;
            batch = std::move(next);
            auto stats = execute_batch(context, batch, limits, batch_id, start);
            stats["session_batches"] = batches.size();
            // Author futures/copies finished; RestoreLane was sent, with no author ACK.
            write_frame("SEMA_IMLANE_BATCH_END ", nlohmann::json({{"batch_id", batch_id}, {"stats", stats}}).dump());
            batch = Batch{};
        }
        throw std::runtime_error("complete-call session input ended without its close fence");
    } catch (const std::invalid_argument& error) {
        std::cout << "SEMA_IMLANE_ERROR " << nlohmann::json({{"error_type", "ValueError"}, {"message", error.what()}}).dump() << std::endl;
        std::_Exit(1);
    } catch (const std::exception& error) {
        std::cerr << "IMLane complete-call failure: " << error.what() << std::endl;
        // Do not wait for other author futures after the first local failure.
        // The owning caller records outstanding remote work as unknown.
        std::_Exit(1);
    }
}
