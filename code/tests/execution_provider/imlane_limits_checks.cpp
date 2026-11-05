#include "record_limits.hpp"
#include <iostream>
#include <limits>
#include <string>

using namespace imlane_records;

int main() {
    try {
        for (size_t count : {size_t{1}, size_t{4}, size_t{129}, std::numeric_limits<size_t>::max()}) {
            const auto parsed = pending_capacity(std::to_string(count));
            Limits limits(parsed);
            limits.validate();
            require(limits.max_pending_blocks == count, "declared pending capacity changed");
        }
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
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
