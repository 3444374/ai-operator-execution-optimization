#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <nlohmann/json.hpp>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
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
Table input(const std::string& wire) {
    auto value = nlohmann::json::parse(wire);
    arrow::StringBuilder ids, calls; arrow::Int64Builder rows;
    check(ids.Append(value.at("call_id").get<std::string>()));
    check(rows.Append(value.at("row_id").get<int64_t>())); check(calls.Append(wire));
    std::shared_ptr<arrow::Array> a, b, c;
    check(ids.Finish(&a)); check(rows.Finish(&b)); check(calls.Finish(&c));
    return arrow::Table::Make(arrow::schema({arrow::field("call_id", arrow::utf8()),
        arrow::field("row_id", arrow::int64()), arrow::field("native_call", arrow::utf8())}), {a,b,c});
}

int main(int argc, char** argv) {
    if (argc != 4) { std::cerr << "usage: imlane_http PORT UDF_PATH NATIVE_TASK_THREADS\n"; return 2; }
    try {
        struct Call { Table input, output; };
        std::vector<std::shared_ptr<Call>> calls; std::string line;
        size_t input_bytes = 0;
        while (std::getline(std::cin, line)) {
            input_bytes += line.size();
            if (calls.size() >= 128 || input_bytes > 48 * 1024 * 1024)
                throw std::runtime_error("complete-call input exceeds snapshot allowance");
            calls.push_back(std::make_shared<Call>(Call{input(line), nullptr}));
        }
        auto context = std::make_shared<dbend::DBEndContext>();
        check(context->Setup("127.0.0.1", std::stoi(argv[1])));
        check(context->RegisterFunction(argv[2], "complete_http", "sema_complete_http"));
        auto executors = context->GetSettings().n_executors;
        auto threads = dbend::TaskThreadPool::instance(executors).sys_cpu_core_nums;
        std::cout << "SEMA_IMLANE_META {\"native_executors\":"
                  << executors << ",\"native_task_threads\":" << threads << "}" << std::endl;
        if (calls.empty() && std::stoi(argv[3]) == 0) return 0;
        if (threads != std::stoi(argv[3]))
            throw std::runtime_error("author task thread count differs from explicit configuration");
        std::vector<std::unique_ptr<dbend::ExecAsyncFuncContext>> pending;
        for (const auto& call : calls)
            pending.push_back(context->ExecuteAsyncFuncPush([context, call] {
                check(context->ExecuteFunction("sema_complete_http", *call->input, call->output));
            }));
        size_t finished = 0;
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
                std::cout << "SEMA_IMLANE_RESULT " << result->ToString() << std::endl;
                pending[i].reset();
                calls[i].reset();
                ++finished;
            }
            if (finished != pending.size()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "IMLane complete-call failure: " << error.what() << std::endl;
        // Do not wait for other author futures after the first local failure.
        // The owning caller records outstanding remote work as unknown.
        std::_Exit(1);
    }
}
