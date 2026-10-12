#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <nlohmann/json.hpp>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <unordered_set>
#include "dbend/cpp/imlane_dbend.hpp"

// Copy the output before the author's ExecuteFunction frees Lane storage.
namespace IMLane::DBEnd {
template<> class TableConverter<arrow::Table, std::shared_ptr<arrow::Table>> {
public:
    std::shared_ptr<arrow::Table> ConvertToExchange(arrow::Table* table) {
        return std::shared_ptr<arrow::Table>(table, [](arrow::Table*) {});
    }
    void ConvertFromExchange(std::shared_ptr<arrow::Table> table,
                             std::shared_ptr<arrow::Table>& out) {
        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, table->schema()).ValueOrDie();
        if (!writer->WriteTable(*table).ok() || !writer->Close().ok())
            throw std::runtime_error("complete-call output copy failed");
        auto reader = arrow::ipc::RecordBatchStreamReader::Open(
            std::make_shared<arrow::io::BufferReader>(stream->Finish().ValueOrDie())).ValueOrDie();
        out = reader->ToTable().ValueOrDie();
    }
};
}
namespace dbend = IMLane::DBEnd;
using Table = std::shared_ptr<arrow::Table>;

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

int main(int argc, char** argv) {
    if (argc != 4 && argc != 10) {
        std::cerr << "usage: imlane_http PORT UDF_PATH NATIVE_TASK_THREADS [MAX_ROWS MAX_PAYLOAD MAX_WIRE MAX_ITEM MAX_PENDING MAX_OUTPUT]\n";
        return 2;
    }
    try {
        BridgeLimits limits;
        if (argc == 10) {
            limits.max_batch_rows = bounded_argument(argv[4], 4096);
            limits.max_input_payload_bytes = bounded_argument(argv[5], 32 * 1024 * 1024);
            limits.max_input_wire_bytes = bounded_argument(argv[6], 48 * 1024 * 1024);
            limits.max_item_payload_bytes = bounded_argument(argv[7], 8 * 1024 * 1024);
            limits.max_pending_calls = bounded_argument(argv[8], 128);
            limits.max_batch_response_bytes = bounded_argument(argv[9], 32 * 1024 * 1024);
        }
        struct Call { Table input, output; size_t response_max; };
        std::vector<std::shared_ptr<Call>> calls; std::string line;
        std::unordered_set<std::string> identities;
        size_t input_bytes = 0, payload_bytes = 0;
        while (std::getline(std::cin, line)) {
            input_bytes += line.size() + 1;
            if (calls.size() >= limits.max_batch_rows || input_bytes > limits.max_input_wire_bytes)
                throw std::invalid_argument("complete-call total rows or stdin wire bytes exceed the selected capacity; author submission has not started");
            auto value = nlohmann::json::parse(line);
            auto bytes = decoded_size(value.at("payload").get<std::string>());
            payload_bytes += bytes;
            if (bytes == 0 || bytes > limits.max_item_payload_bytes || payload_bytes > limits.max_input_payload_bytes)
                throw std::invalid_argument("complete-call payload bytes exceed the selected capacity; author submission has not started");
            auto identity = value.at("call_id").get<std::string>();
            if (identity.empty() || identity.size() > 4096 || !identities.insert(identity).second)
                throw std::invalid_argument("complete-call occurrence identity is invalid; author submission has not started");
            auto response_max = value.at("max_response_bytes").get<size_t>();
            if (response_max == 0 || response_max > 8 * 1024 * 1024)
                throw std::invalid_argument("complete-call response capacity is invalid; author submission has not started");
            calls.push_back(std::make_shared<Call>(Call{input(line, value), nullptr, response_max}));
        }
        auto context = std::make_shared<dbend::DBEndContext>();
        check(context->Setup("127.0.0.1", std::stoi(argv[1])));
        check(context->RegisterFunction(argv[2], "complete_http", "sema_complete_http"));
        auto executors = context->GetSettings().n_executors;
        auto threads = dbend::TaskThreadPool::instance(executors).sys_cpu_core_nums;
        std::cout << "SEMA_IMLANE_META {\"native_executors\":"
                  << executors << ",\"native_task_threads\":" << threads << "}" << std::endl;
        std::cout << "SEMA_IMLANE_LIMITS " << limits.json().dump() << std::endl;
        if (calls.empty() && std::stoi(argv[3]) == 0) return 0;
        if (threads != std::stoi(argv[3]))
            throw std::runtime_error("author task thread count differs from explicit configuration");
        std::vector<std::unique_ptr<dbend::ExecAsyncFuncContext>> pending(calls.size());
        size_t finished = 0, submitted = 0, held = 0, pending_peak = 0, delivered_bytes = 0;
        auto supply = [&] {
            while (submitted < calls.size() && held < limits.max_pending_calls) {
                auto call = calls[submitted];
                pending[submitted] = context->ExecuteAsyncFuncPush([context, call] {
                    check(context->ExecuteFunction("sema_complete_http", *call->input, call->output));
                });
                ++submitted; ++held; pending_peak = std::max(pending_peak, held);
            }
        };
        supply();
        while (finished != pending.size()) {
            for (size_t i = 0; i < pending.size(); ++i) {
                if (!pending[i] || !pending[i]->ExecuteAsyncFuncTryPull()) continue;
                pending[i]->task.get();
                auto output = calls[i]->output;
                if (!output) throw std::runtime_error("complete-call output is absent");
                check(output->ValidateFull());
                if (output->num_rows() != 1 || output->ColumnNames() !=
                        std::vector<std::string>{"call_id", "row_id", "native_outcome"})
                    throw std::runtime_error("complete-call output shape differs");
                auto result = std::static_pointer_cast<arrow::StringScalar>(
                    output->column(2)->GetScalar(0).ValueOrDie());
                if (!result->is_valid) throw std::runtime_error("complete-call output is NULL");
                auto wire = result->ToString();
                auto outcome = nlohmann::json::parse(wire);
                auto bytes = outcome.at("response").is_null()
                    ? outcome.value("transport_error", std::string()).size() + outcome.value("execution_error", std::string()).size()
                    : decoded_size(outcome.at("response").get<std::string>());
                delivered_bytes += bytes;
                if (bytes > calls[i]->response_max || delivered_bytes > limits.max_batch_response_bytes)
                    throw std::invalid_argument("complete-call responses exceed the selected byte capacity");
                std::cout << "SEMA_IMLANE_RESULT " << wire << std::endl;
                pending[i].reset();
                calls[i].reset();
                ++finished; --held;
                supply();
            }
            if (finished != pending.size()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        std::cout << "SEMA_IMLANE_STATS " << nlohmann::json({{"submitted", submitted}, {"delivered", finished},
            {"pending_peak", pending_peak}, {"input_payload_bytes", payload_bytes},
            {"stdin_wire_bytes", input_bytes}, {"delivered_response_bytes", delivered_bytes}}).dump() << std::endl;
        return 0;
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
