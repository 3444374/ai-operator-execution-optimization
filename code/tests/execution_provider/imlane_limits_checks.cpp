#include "record_limits.hpp"
#include <iostream>
#include <limits>
#include <string>

using namespace imlane_records;

Limits finite(size_t pending) {
    Limits value(pending);
    value.max_block_rows = 17; value.max_query_rows = 4097 + 2; value.max_queries = 65 + 2;
    value.max_input_bytes = 65536; value.max_output_bytes = 1048576;
    value.max_command_bytes = 131072; value.max_response_bytes = 8 * 1048576;
    value.owner_shm_bytes = lane_bytes; value.owner_rss_bytes = 8ULL * 1024 * 1024 * 1024;
    return value;
}
template<class F> void rejects(F&& f) {
    bool rejected = false;
    try { f(); } catch (const std::invalid_argument&) { rejected = true; }
    require(rejected, "invalid finite transport declaration was accepted");
}
int main() {
    try {
        for (size_t count : {size_t{1}, size_t{4}, size_t{129}}) {
            const auto parsed = pending_capacity(std::to_string(count));
            auto limits = finite(parsed);
            limits.validate();
            require(limits.max_pending_blocks == count, "declared pending capacity changed");
            require(limits.max_block_rows == 17 && limits.max_query_rows == 4099,
                "workload-derived row declarations changed");
        }
        const auto largest = std::numeric_limits<size_t>::max();
        require(pending_capacity(std::to_string(largest)) == largest, "size_t decimal parsing changed");
        rejects([&] { finite(largest).validate(); });  // Finite memory still bounds a parsed count.
        for (const auto& text : {"", "0", "-1", "+1", " 1", "1x", "1.0", "1e3"}) {
            bool rejected = false;
            try { pending_capacity(text); } catch (const std::invalid_argument&) { rejected = true; }
            require(rejected, "invalid pending declaration was accepted");
        }
        bool rejected = false;
        try { pending_capacity(std::to_string(std::numeric_limits<size_t>::max()) + "0"); }
        catch (const std::invalid_argument&) { rejected = true; }
        require(rejected, "pending integer overflow was accepted");
        rejected = false;
        try { Limits(0).validate(); } catch (const std::invalid_argument&) { rejected = true; }
        require(rejected, "zero pending capacity was accepted");
        auto lane = finite(1);
        lane.max_input_bytes = lane_bytes - 2 * arrow_ipc_overhead_bytes - lane.max_output_bytes;
        lane.validate();
        ++lane.max_input_bytes;
        rejects([&] { lane.validate(); });
        auto rss = finite(129);
        rss.owner_rss_bytes = (rss.max_pending_blocks + 1) * (rss.max_input_bytes + rss.max_output_bytes)
            + rss.max_command_bytes + rss.max_response_bytes;
        rss.validate();
        --rss.owner_rss_bytes;
        rejects([&] { rss.validate(); });
        auto shm = finite(1); shm.owner_shm_bytes = lane_bytes - 1;
        rejects([&] { shm.validate(); });
        QueryHistory queries(65 + 2);
        queries.insert("startup");
        for (size_t index = 0; index < 65; ++index) queries.insert("query-" + std::to_string(index));
        rejects([&] { queries.insert("query-0"); });
        queries.insert("close");
        require(queries.size() == 67, "resident control lifecycle count changed");
        rejects([&] { queries.insert("undeclared-extra-query"); });
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
