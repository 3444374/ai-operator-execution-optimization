#include "record_dbend_adapter.hpp"
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <limits>

using namespace imlane_records;
Table from_records(const Json& rows, const Limits& limits) {
    require(rows.is_array() && !rows.empty() && rows.size() <= static_cast<size_t>(limits.max_block_rows),
        "records wire rows exceed declared capacity");
    // Check the exact non-null flat Arrow buffers before constructing builders.
    size_t bytes = 16;  // One final int32 offset in each of four utf8 arrays.
    auto account = [&](size_t count) {
        require(bytes <= limits.max_input_bytes && count <= limits.max_input_bytes - bytes,
            "records buffers exceed byte allowance before copy");
        bytes += count;
    };
    for (const auto& row : rows) {
        require(row.is_object() && row.size() == 5, "records wire fields differ");
        account(24);  // Four int32 offsets and one int64 row ID.
        for (const auto* name : {"call_id", "query_id", "stage_id", "record_json"}) {
            require(row.at(name).is_string(), "records wire text field must be a string");
            account(row.at(name).get_ref<const std::string&>().size());
        }
    }
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
bool read_line(std::string& line, size_t maximum) {
    line.clear();
    for (int c; (c = std::getchar()) != EOF;) {
        if (c == '\n') return true;
        if (line.size() >= maximum - 1) throw std::invalid_argument("records command exceeds wire allowance");
        line.push_back(static_cast<char>(c));
    }
    require(line.empty(), "records command is truncated");
    return false;
}
size_t response_limit = 0;
void frame(Json value) {
    const auto payload = std::string("IMLANE_RECORDS ") + value.dump() + "\n";
    require(!response_limit || payload.size() <= response_limit, "records response exceeds its declared pipe allowance");
    std::cout << payload << std::flush;
    if (!std::cout) std::_Exit(74);
}
int main(int argc, char** argv) {
    try {
        require(argc == 6 || argc == 7,
            "usage: imlane_records PORT UDF_PATH FUNCTION MAX_PENDING_BLOCKS LIMITS_JSON [imlane.records.v2]");
        const bool terminals_v2 = argc == 7;
        require(!terminals_v2 || std::string(argv[6]) == "imlane.records.v2", "unknown records protocol");
        Limits limits(pending_capacity(argv[4]));
        const auto declaration = Json::parse(argv[5]);
        require(declaration.is_object() && declaration.size() == 10, "declare all records transport limits");
        auto number = [&](const char* name) {
            const auto& value = declaration.at(name);
            require(value.is_number_unsigned() && value.get<uint64_t>() < (uint64_t{1} << 63) && value.get<uint64_t>() > 0,
                "records transport declarations must be positive int64 values");
            return value.get<size_t>();
        };
        require(number("max_pending_blocks") == limits.max_pending_blocks, "pending declaration differs from its CLI grant");
        limits.max_block_rows = number("max_block_rows");
        limits.max_query_rows = number("max_query_rows");
        limits.max_queries = number("max_queries");
        limits.max_input_bytes = number("max_input_bytes");
        limits.max_output_bytes = number("max_output_bytes");
        limits.max_command_bytes = number("max_command_bytes");
        limits.max_response_bytes = number("max_response_bytes");
        limits.owner_shm_bytes = number("owner_shm_bytes");
        limits.owner_rss_bytes = number("owner_rss_bytes");
        limits.validate();
        response_limit = limits.max_response_bytes;
        const auto ready_max = Json({{"event", "ready"},
            {"protocol", terminals_v2 ? "imlane.records.v2" : "imlane.records.v1"},
            {"max_pending_blocks", limits.max_pending_blocks}, {"transport_limits", declaration},
            {"native_task_threads", std::numeric_limits<int>::max()}, {"real_model_requests", 0}}).dump();
        require(std::string("IMLANE_RECORDS ").size() + ready_max.size() + 1 <= response_limit,
            "records ready receipt exceeds declared response bytes before author connection");
        size_t used = 0;
        auto port = std::stoul(argv[1], &used);
        require(used == std::string(argv[1]).size() && port > 0 && port <= 65535, "invalid records port");
        auto context = std::make_shared<dbend::DBEndContext>();
        check(context->Setup("127.0.0.1", static_cast<int>(port)));
        check(context->RegisterFunction(argv[2], argv[3], "imlane_records"));
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
            {"max_pending_blocks", limits.max_pending_blocks}, {"transport_limits", declaration},
            {"native_task_threads", context->GetSettings().n_executors}};
        if (!terminals_v2) startup["real_model_requests"] = 0;
        frame(startup);
        std::string line;
        while (read_line(line, limits.max_command_bytes)) {
            try {
                auto command = Json::parse(line);
                const auto op = command.at("op").get<std::string>();
                Json reply = {{"event", op}, {"ok", true}};
                if (op == "begin") session.begin_query(command.at("query_id").get<std::string>());
                else if (op == "submit") session.submit(command.at("block_id").get<std::string>(), from_records(command.at("rows"), limits));
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
