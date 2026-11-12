#pragma once

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <utility>

namespace semloom_imlane {
constexpr const char *RELATION_ACK_PROTOCOL = "semloom.imlane.relation_ack.v1";

// Construct before publishing the relation, then wait before its DuckDB result,
// connection or database leaves scope. The private caller owns this one path.
class RelationHandoff {
public:
    RelationHandoff(std::string path, int64_t native_ready_ns)
        : path_(std::move(path)), native_ready_ns_(native_ready_ns) {
        struct stat state{};
        if (lstat(path_.c_str(), &state) == 0 || errno != ENOENT)
            throw std::runtime_error("relation handoff requires a fresh ACK path");
    }

    void Wait(std::chrono::milliseconds timeout) const {
        const auto until = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < until) {
            struct stat state{};
            if (lstat(path_.c_str(), &state) == 0) {
                if (!S_ISREG(state.st_mode) || state.st_size > 256)
                    throw std::runtime_error("invalid relation handoff ACK file");
                std::ifstream stream(path_);
                std::string protocol, outcome, trailing;
                int64_t native_ready = 0, sink_ready = 0;
                if (!(stream >> protocol >> outcome >> native_ready >> sink_ready)
                        || stream >> trailing || protocol != RELATION_ACK_PROTOCOL
                        || native_ready != native_ready_ns_)
                    throw std::runtime_error("relation handoff ACK identity differs");
                if (outcome == "abort") throw std::runtime_error("relation sink aborted");
                if (outcome != "complete" || sink_ready < native_ready)
                    throw std::runtime_error("invalid relation sink completion ACK");
                return;
            }
            if (errno != ENOENT) throw std::runtime_error("cannot inspect relation handoff ACK");
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        throw std::runtime_error("relation sink ACK deadline exceeded");
    }

private:
    std::string path_;
    int64_t native_ready_ns_;
};
} // namespace semloom_imlane
