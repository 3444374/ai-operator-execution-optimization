#pragma once
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string_view>

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

struct Limits {
    int64_t max_block_rows = 16;
    size_t max_pending_blocks, max_query_rows = 4096, max_queries = 64;
    size_t max_input_bytes = 65536, max_output_bytes = 1048576;

    explicit Limits(size_t pending) : max_pending_blocks(pending) {}

    void validate() const {
        require(max_block_rows > 0 && max_block_rows <= 4096 && max_pending_blocks > 0 &&
            max_query_rows > 0 && max_query_rows <= 1048576 &&
            max_queries > 0 && max_queries <= 4096 && max_input_bytes > 0 &&
            max_input_bytes <= 16777216 && max_output_bytes > 0 && max_output_bytes <= 16777216,
            "records capacities must be positive and finite");
    }
};
}
