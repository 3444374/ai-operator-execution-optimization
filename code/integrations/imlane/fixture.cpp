#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <iostream>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include "dbend/cpp/imlane_dbend.hpp"

// The database adapter must own output buffers after the author frees Lane storage.
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
            throw std::runtime_error("fixture output ownership copy failed");
        auto buffer = stream->Finish().ValueOrDie();
        auto reader = arrow::ipc::RecordBatchStreamReader::Open(
            std::make_shared<arrow::io::BufferReader>(buffer)).ValueOrDie();
        out = reader->ToTable().ValueOrDie();
    }
};
}
namespace dbend = IMLane::DBEnd;
using Table = std::shared_ptr<arrow::Table>;

void check(const arrow::Status& status) {
    if (!status.ok()) throw std::runtime_error(status.ToString());
}
void check(const dbend::ContextStatus& status) {
    if (status.kind == dbend::StatusKind::ERROR) throw std::runtime_error(status.info);
}
Table input(int serial, bool empty = false) {
    arrow::Int64Builder ids, positions, values;
    arrow::StringBuilder text;
    if (!empty) {
        for (int i = 0; i < 5; ++i) {
            check(ids.Append(i < 2 ? 17 : i + 16));
            check(positions.Append(serial * 5 + i));
            if (i == 2) { check(values.AppendNull()); check(text.AppendNull()); }
            else {
                check(values.Append(serial * 10 + i - 1));
                const std::string label = i == 3 ? "多行\n文本" : (i == 4 ? std::string("a\0b", 3) : "repeated");
                check(text.Append(label));
            }
        }
    }
    std::shared_ptr<arrow::Array> a, b, c, d;
    check(ids.Finish(&a)); check(positions.Finish(&b));
    check(values.Finish(&c)); check(text.Finish(&d));
    return arrow::Table::Make(arrow::schema({arrow::field("id", arrow::int64()),
        arrow::field("position", arrow::int64()), arrow::field("value", arrow::int64()),
        arrow::field("text", arrow::utf8())}), {a,b,c,d});
}
std::set<int64_t> worker_pids;
void validate(const Table& in, const Table& out) {
    if (!out) throw std::runtime_error("fixture returned no table");
    check(out->ValidateFull());
    if (out->num_rows() != in->num_rows() || out->num_columns() != 6)
        throw std::runtime_error("fixture output shape differs");
    const std::vector<std::string> names = {"id","position","value","text","doubled","worker_pid"};
    if (out->ColumnNames() != names) throw std::runtime_error("fixture output schema differs");
    for (int c = 0; c < 4; ++c)
        if (!in->column(c)->Equals(out->column(c))) throw std::runtime_error("fixture input was changed");
    for (int64_t row = 0; row < in->num_rows(); ++row) {
        auto value = std::static_pointer_cast<arrow::Int64Scalar>(in->column(2)->GetScalar(row).ValueOrDie());
        auto doubled = std::static_pointer_cast<arrow::Int64Scalar>(out->column(4)->GetScalar(row).ValueOrDie());
        if (value->is_valid != doubled->is_valid || (value->is_valid && doubled->value != value->value * 2))
            throw std::runtime_error("fixture arithmetic or NULL handling differs");
        auto pid = std::static_pointer_cast<arrow::Int64Scalar>(out->column(5)->GetScalar(row).ValueOrDie());
        if (!pid->is_valid || pid->value <= 0) throw std::runtime_error("fixture executor process identity missing");
        worker_pids.insert(pid->value);
    }
}

int main(int argc, char** argv) {
    if (argc != 3) { std::cerr << "usage: imlane_fixture_cpp PORT UDF_PATH\n"; return 2; }
    try {
        dbend::DBEndContext context;
        check(context.Setup("127.0.0.1", std::stoi(argv[1])));
        check(context.RegisterFunction(argv[2], "transform", "fixture_sync"));
        check(context.RegisterFunction(argv[2], "async_transform", "fixture_async"));
        std::vector<std::pair<Table, Table>> retained;
        int sync_calls = 0, push_calls = 0, future_calls = 0;
        for (int i = 0; i < 12; ++i) {
            auto in = input(i); Table out;
            check(context.ExecuteFunction("fixture_sync", *in, out));
            validate(in, out); retained.emplace_back(in, out); ++sync_calls;
            for (const auto& pair : retained) validate(pair.first, pair.second);
        }
        std::cout << "{\"stage\":\"sync\",\"passed\":true,\"calls\":12,\"rows\":60}" << std::endl;
        for (int i = 0; i < 3; ++i) {
            std::array<Table, 2> inputs = {input(20 + i * 2), input(21 + i * 2)};
            std::array<Table, 2> outputs;
            auto a = context.ExecuteFunctionPush("fixture_async", *inputs[0], outputs[0]);
            auto b = context.ExecuteFunctionPush("fixture_async", *inputs[1], outputs[1]);
            check(a->push_status); check(b->push_status);
            auto until = std::chrono::steady_clock::now() + std::chrono::seconds(30);
            std::array<bool, 2> pulled = {false, false};
            while (!pulled[0] || !pulled[1]) {
                if (!pulled[0]) pulled[0] = a->ExecuteFuncTryPull();
                if (!pulled[1]) pulled[1] = b->ExecuteFuncTryPull();
                if (std::chrono::steady_clock::now() > until) throw std::runtime_error("async pull exceeded 30 seconds");
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            check(a->pull_status); check(b->pull_status);
            for (int j = 0; j < 2; ++j) { validate(inputs[j], outputs[j]); retained.emplace_back(inputs[j], outputs[j]); ++push_calls; }
        }
        std::cout << "{\"stage\":\"push_pull\",\"passed\":true,\"calls\":6,\"rows\":30}" << std::endl;
        std::array<Table, 2> inputs = {input(40), input(41)}, outputs;
        auto a = context.ExecuteAsyncFuncPush([&] { check(context.ExecuteFunction("fixture_sync", *inputs[0], outputs[0])); });
        auto b = context.ExecuteAsyncFuncPush([&] { check(context.ExecuteFunction("fixture_async", *inputs[1], outputs[1])); });
        a->ExecuteAsyncFuncPull(); b->ExecuteAsyncFuncPull();
        a->task.get(); b->task.get();
        for (int j = 0; j < 2; ++j) { validate(inputs[j], outputs[j]); ++future_calls; }
        std::cout << "{\"stage\":\"future\",\"passed\":true,\"calls\":2,\"rows\":10}" << std::endl;
        auto empty = input(50, true); Table empty_out;
        check(context.ExecuteFunction("fixture_sync", *empty, empty_out)); validate(empty, empty_out); ++sync_calls;
        for (const auto& pair : retained) validate(pair.first, pair.second);
        auto final_in = input(60); Table final_out;
        check(context.ExecuteFunction("fixture_sync", *final_in, final_out)); validate(final_in, final_out); ++sync_calls;
        std::cout << "{\"passed\":true,\"sync_calls\":" << sync_calls << ",\"push_pull_calls\":" << push_calls
            << ",\"future_calls\":" << future_calls << ",\"validated_rows\":105,\"duplicate_ids_preserved\":true"
            << ",\"nulls_preserved\":true,\"retained_outputs_preserved\":true,\"real_model_requests\":0,\"worker_pids\":[";
        bool first = true; for (auto pid : worker_pids) { if (!first) std::cout << ','; std::cout << pid; first = false; }
        std::cout << "]}" << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "fixture failed: " << error.what() << std::endl;
        return 1;
    }
}
