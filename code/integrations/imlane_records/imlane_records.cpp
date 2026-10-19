#include "record_dbend_adapter.hpp"
#include <iostream>
#include <cstdio>
#include <cstdlib>

using namespace imlane_records;
Table from_records(const Json& rows) {
    require(rows.is_array() && !rows.empty() && rows.size() <= 16, "records wire rows exceed capacity");
    arrow::StringBuilder calls, queries, stages, records;
    arrow::Int64Builder positions;
    for (const auto& row : rows) {
        require(row.is_object() && row.size() == 5, "records wire fields differ");
        require(row.at("row_id").is_number_integer(), "records wire row_id must be an integer");
        check(calls.Append(row.at("call_id").get<std::string>()));
        check(positions.Append(row.at("row_id").get<int64_t>()));
        check(queries.Append(row.at("query_id").get<std::string>()));
        check(stages.Append(row.at("stage_id").get<std::string>()));
        check(records.Append(row.at("record_json").get<std::string>()));
    }
    return arrow::Table::Make(arrow::schema({arrow::field("call_id", arrow::utf8()),
        arrow::field("row_id", arrow::int64()), arrow::field("query_id", arrow::utf8()),
        arrow::field("stage_id", arrow::utf8()), arrow::field("record_json", arrow::utf8())}),
        {take(calls.Finish()), take(positions.Finish()), take(queries.Finish()), take(stages.Finish()), take(records.Finish())});
}
Json to_records(const Table& table) {
    Json rows = Json::array();
    for (int64_t i = 0; i < table->num_rows(); ++i)
        rows.push_back({{"call_id", string_at(table, 0, i)}, {"row_id", row_at(table, i)},
            {"outcome_json", string_at(table, 2, i)}});
    return rows;
}
bool read_line(std::string& line) {
    line.clear();
    for (int c; (c = std::getchar()) != EOF;) {
        if (c == '\n') return true;
        if (line.size() >= 131072) throw std::invalid_argument("records command exceeds wire allowance");
        line.push_back(static_cast<char>(c));
    }
    require(line.empty(), "records command is truncated");
    return false;
}
void frame(Json value) {
    std::cout << "IMLANE_RECORDS " << value.dump() << std::endl;
    if (!std::cout) std::_Exit(74);
}
int main(int argc, char** argv) {
    try {
        require(argc == 4 || argc == 5, "usage: imlane_records PORT UDF_PATH FUNCTION [imlane.records.v2]");
        const bool terminals_v2 = argc == 5;
        require(!terminals_v2 || std::string(argv[4]) == "imlane.records.v2", "unknown records protocol");
        size_t used = 0;
        auto port = std::stoul(argv[1], &used);
        require(used == std::string(argv[1]).size() && port > 0 && port <= 65535, "invalid records port");
        auto context = std::make_shared<dbend::DBEndContext>();
        check(context->Setup("127.0.0.1", static_cast<int>(port)));
        check(context->RegisterFunction(argv[2], argv[3], "imlane_records"));
        Limits limits;
        if (terminals_v2) ++limits.max_query_rows; // One explicit worker-close record, outside the model budget.
        Session session(context, "imlane_records", limits, terminals_v2);
        Table retained;
        Json retained_rows;
        size_t retained_checks = 0;
        auto check_retained = [&] {
            if (!retained) return;
            try {
                require(to_records(retained) == retained_rows, "retained records output changed after Lane reuse");
                ++retained_checks;
            } catch (...) {
                session.mark_terminal();
                throw;
            }
        };
        Json startup = {{"event", "ready"}, {"protocol", terminals_v2 ? "imlane.records.v2" : "imlane.records.v1"},
            {"native_task_threads", context->GetSettings().n_executors}};
        if (!terminals_v2) startup["real_model_requests"] = 0;
        frame(startup);
        std::string line;
        while (read_line(line)) {
            try {
                auto command = Json::parse(line);
                const auto op = command.at("op").get<std::string>();
                Json reply = {{"event", op}, {"ok", true}};
                if (op == "begin") session.begin_query(command.at("query_id").get<std::string>());
                else if (op == "submit") session.submit(command.at("block_id").get<std::string>(), from_records(command.at("rows")));
                else if (op == "poll") reply["ready"] = session.ready_blocks();
                else if (op == "collect") {
                    auto table = session.collect(command.at("block_id").get<std::string>());
                    try {
                        check_retained();
                        retained = std::move(table);
                        retained_rows = to_records(retained);
                        reply["rows"] = retained_rows;
                    } catch (...) { session.mark_terminal(); throw; }
                }
                else if (op == "end") session.end_query();
                else if (op == "close") {
                    require(session.pending_count() == 0 && !session.poisoned(), "records close needs an empty healthy owner");
                    check_retained();
                    reply["retained_output_checks"] = retained_checks;
                    frame(reply);
                    return 0;
                } else throw std::invalid_argument("unknown records command");
                reply["pending_blocks"] = session.pending_count();
                reply["retained_output_checks"] = retained_checks;
                frame(reply);
            } catch (const std::exception& error) {
                frame({{"event", "error"}, {"ok", false}, {"error", error.what()}, {"owner_terminal", session.poisoned()}});
                if (session.poisoned()) std::_Exit(70);
            }
        }
        // EOF does not acknowledge unfinished work or release an external owner.
        if (session.pending_count()) std::_Exit(70);
        return 0;
    } catch (const std::exception& error) {
        frame({{"event", "error"}, {"ok", false}, {"error", error.what()}, {"owner_terminal", true}});
        std::_Exit(70);
    }
}
