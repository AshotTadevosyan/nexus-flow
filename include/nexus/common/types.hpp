#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <chrono>
#include <system_error>

namespace nexus {

// Strong status codes for internal operations
enum class StatusCode : uint32_t {
    Success = 0,
    InvalidPacket = 1,
    CrcMismatch = 2,
    BufferFull = 3,
    ConnectionClosed = 4,
    Timeout = 5,
    NotSubscribed = 6,
    NotFound = 7,
    InternalError = 8,
    RateLimited = 9
};

inline std::string_view status_to_string(StatusCode code) {
    switch (code) {
        case StatusCode::Success: return "Success";
        case StatusCode::InvalidPacket: return "InvalidPacket";
        case StatusCode::CrcMismatch: return "CrcMismatch";
        case StatusCode::BufferFull: return "BufferFull";
        case StatusCode::ConnectionClosed: return "ConnectionClosed";
        case StatusCode::Timeout: return "Timeout";
        case StatusCode::NotSubscribed: return "NotSubscribed";
        case StatusCode::NotFound: return "NotFound";
        case StatusCode::InternalError: return "InternalError";
        case StatusCode::RateLimited: return "RateLimited";
        default: return "Unknown";
    }
}

// System time utilities
inline int64_t current_timestamp_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

inline int64_t current_timestamp_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

inline int64_t current_timestamp_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()).count();
}

} // namespace nexus
