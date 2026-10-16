#include "record_dbend_adapter.hpp"
#include <iostream>

using namespace imlane_records;
Table input() {
    arrow::StringBuilder ids, queries, stages, records;
    arrow::Int64Builder rows;
    for (auto id : {"first", "second"}) {
        check(ids.Append(id)); check(rows.Append(7)); check(queries.Append("query"));
        check(stages.Append("filter")); check(records.Append("{\"text\":\"same\"}"));
    }
    return arrow::Table::Make(arrow::schema({arrow::field("call_id", arrow::utf8()),
        arrow::field("row_id", arrow::int64()), arrow::field("query_id", arrow::utf8()),
        arrow::field("stage_id", arrow::utf8()), arrow::field("record_json", arrow::utf8())}),
        {take(ids.Finish()), take(rows.Finish()), take(queries.Finish()), take(stages.Finish()), take(records.Finish())});
}
Json envelope(const Table& in, int64_t i) {
    return {{"query_id", "query"}, {"stage_id", "filter"}, {"status", "ok"},
        {"result", {{"_occurrence_id", string_at(in, 0, i)}, {"_position", 7}, {"text", "original"}}},
        {"error_type", nullptr}, {"error", nullptr}, {"model_entry_attempted", false},
        {"completion_known", true}, {"worker", {{"worker_pid", 1}}}};
}
Table output(const Table& in, const std::string& fault = "") {
    arrow::StringBuilder ids, results; arrow::Int64Builder rows;
    for (int64_t i = 0; i < (fault == "missing" ? 1 : 2); ++i) {
        auto pos = fault == "reversed" ? 1-i : i;
        check(ids.Append(string_at(in, 0, pos))); check(rows.Append(7));
        auto value = envelope(in, pos);
        if (fault == "error") value.update({{"status", "error"}, {"result", nullptr},
            {"error", "injected"}, {"error_type", "EchoFailure"}, {"completion_known", false}});
        if (fault == "mixed") value["error"] = "unexpected";
        if (fault == "query") value["query_id"] = "another-query";
        if (fault == "identity") value["result"]["_occurrence_id"] = "wrong";
        if (fault == "null") check(results.AppendNull());
        else check(results.Append(fault == "malformed" ? "[]" : value.dump()));
    }
    return arrow::Table::Make(arrow::schema({arrow::field("call_id", arrow::utf8()),
        arrow::field("row_id", arrow::int64()), arrow::field("outcome_json", arrow::utf8())}),
        {take(ids.Finish()), take(rows.Finish()), take(results.Finish())});
}
template<class F> void rejects(F&& f) {
    bool failed = false;
    try { f(); } catch (const std::exception&) { failed = true; }
    require(failed, "records negative check unexpectedly succeeded");
}
int main() {
    try {
        auto in = input();
        auto ids = validate_input(in, "query", Limits{});
        require(ids.size() == 2 && row_at(in, 0) == row_at(in, 1), "repeat source positions were lost");
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (const auto& field : in->schema()->fields()) fields.push_back(field->WithNullable(false));
        validate_input(arrow::Table::Make(arrow::schema(fields), in->columns()), "query", Limits{});
        rejects([&] { validate_input(in, "wrong-query", Limits{}); });
        auto small = Limits{}; small.max_input_bytes = 1;
        rejects([&] { validate_input(in, "query", small); });
        auto empty = in->Slice(0, 0);
        rejects([&] { validate_input(empty, "query", Limits{}); });
        IMLane::DBEnd::TableConverter<arrow::Table, ExchangeResult> converter;
        auto lane = output(in);
        auto result = std::make_shared<ExchangeResult>(); result->input = in; result->max_output_bytes = 1048576;
        converter.ConvertFromExchange(lane, *result);
        require(!result->conversion_error && bool(result->output), "records copy did not succeed");
        const auto before = string_at(result->output, 2, 0);
        auto raw = std::static_pointer_cast<arrow::StringArray>(lane->column(2)->chunk(0));
        std::fill(raw->value_data()->mutable_data(), raw->value_data()->mutable_data() + raw->value_data()->size(), 'x');
        require(string_at(result->output, 2, 0) == before, "records output still aliases Lane storage");
        std::promise<void> promise;
        Pending pending{result, std::make_unique<dbend::ExecAsyncFuncContext>(promise.get_future())};
        require(!ready(pending), "records pending was ready before task completion");
        rejects([&] { collect_ready(pending); });
        promise.set_value();
        auto kept = collect_ready(pending);
        require(kept->num_rows() == 2, "records collected table differs");
        rejects([&] { ready(pending); }); rejects([&] { collect_ready(pending); });
        for (const auto& fault : {"missing", "reversed", "mixed", "query", "identity", "null", "malformed", "cap", "error"}) {
            auto bad = std::make_shared<ExchangeResult>(); bad->input = in;
            bad->max_output_bytes = std::string(fault) == "cap" ? 1 : 1048576;
            converter.ConvertFromExchange(output(in, fault), *bad);
            require(bad->conversion_entered, "converter did not retain failure evidence");
            if (std::string(fault) != "error")
                require(bool(bad->conversion_error) && !bad->output, "invalid output escaped pre-copy validation");
            std::promise<void> done; auto future = done.get_future(); done.set_value();
            Pending rejected{bad, std::make_unique<dbend::ExecAsyncFuncContext>(std::move(future))};
            rejects([&] { collect_ready(rejected); });
            require(rejected.state == Pending::State::failed, "failed collect remained consumable");
            rejects([&] { collect_ready(rejected); });
        }
        std::promise<void> failed; auto future = failed.get_future();
        failed.set_exception(std::make_exception_ptr(std::runtime_error("author task failure")));
        Pending rejected{result, std::make_unique<dbend::ExecAsyncFuncContext>(std::move(future))};
        rejects([&] { collect_ready(rejected); }); rejects([&] { collect_ready(rejected); });
        std::cout << Json({{"passed", true}, {"negative_output_cases", 9}, {"future_exception_consumed_once", true},
            {"owned_output_survived_mutation", true}, {"real_model_requests", 0}}).dump() << std::endl;
        return 0;
    } catch (const std::exception& error) { std::cerr << error.what() << std::endl; return 1; }
}
