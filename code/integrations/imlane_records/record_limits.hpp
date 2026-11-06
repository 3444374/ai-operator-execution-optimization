#pragma once
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>

namespace imlane_records {
inline void require(bool value, const char* message) {
    if (!value) throw std::invalid_argument(message);
}

inline size_t pending_capacity(std::string_view text) {
    size_t value = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    require(parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size() && value > 0,
            "records pending capacity must be a positive integer");
    return value;
}

constexpr size_t lane_bytes = 128 * 1024 * 1024;
constexpr size_t arrow_ipc_overhead_bytes = 65536;

struct Limits {
    int64_t max_block_rows = 0;
    size_t max_pending_blocks, max_query_rows = 0, max_queries = 0;
    size_t max_input_bytes = 0, max_output_bytes = 0;
    size_t max_command_bytes = 0, max_response_bytes = 0;
    size_t owner_shm_bytes = 0, owner_rss_bytes = 0;

    explicit Limits(size_t pending) : max_pending_blocks(pending) {}

    void validate() const {
        require(max_block_rows > 0 && max_pending_blocks > 0 && max_query_rows > 0 && max_queries > 0 &&
            max_input_bytes > 0 && max_output_bytes > 0 && max_command_bytes > 0 && max_response_bytes > 0 &&
            owner_shm_bytes >= lane_bytes && owner_rss_bytes > 0,
            "records transport needs complete positive workload and byte declarations");
        require(max_input_bytes <= lane_bytes - 2 * arrow_ipc_overhead_bytes &&
            max_output_bytes <= lane_bytes - 2 * arrow_ipc_overhead_bytes - max_input_bytes,
            "records input/output declarations exceed the author's Lane");
        require(max_command_bytes <= owner_rss_bytes && max_response_bytes <= owner_rss_bytes - max_command_bytes,
            "records pipe declarations exceed owner RSS allowance");
        const size_t buffers = max_input_bytes + max_output_bytes;
        const size_t retained = (owner_rss_bytes - max_command_bytes - max_response_bytes) / buffers;
        require(retained > 0 && max_pending_blocks < retained,
            "records pending/frame declarations exceed owner RSS allowance");
    }
};

class QueryHistory {
    size_t limit_;
    std::unordered_set<std::string> seen_;
public:
    explicit QueryHistory(size_t limit) : limit_(limit) { require(limit > 0, "declare the owner query count"); }
    void insert(const std::string& query) {
        require(seen_.size() < limit_ && seen_.insert(query).second,
            "records query repeats or exceeds declared owner query count");
    }
    size_t size() const { return seen_.size(); }
};
}
