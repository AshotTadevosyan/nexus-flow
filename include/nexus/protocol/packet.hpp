#pragma once

#include "nexus/common/buffer.hpp"
#include "nexus/common/crc32.hpp"
#include "nexus/common/types.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <optional>

namespace nexus {

constexpr uint32_t kMagicNumber = 0x4E455853; // "NEXS"
constexpr uint8_t  kProtocolVersion = 1;
constexpr size_t   kHeaderSize = 20;
constexpr size_t   kMaxPayloadSize = 10 * 1024 * 1024; // 10 MB max

enum class MessageType : uint8_t {
    Ping = 0x01,
    Pong = 0x02,
    Publish = 0x03,
    Subscribe = 0x04,
    Unsubscribe = 0x05,
    DispatchEvent = 0x06,
    QueryLatest = 0x07,
    QueryResponse = 0x08,
    MetricsReq = 0x09,
    MetricsResp = 0x0A,
    Ack = 0x0B,
    Error = 0xFF
};

inline std::string_view msg_type_to_string(MessageType type) {
    switch (type) {
        case MessageType::Ping: return "Ping";
        case MessageType::Pong: return "Pong";
        case MessageType::Publish: return "Publish";
        case MessageType::Subscribe: return "Subscribe";
        case MessageType::Unsubscribe: return "Unsubscribe";
        case MessageType::DispatchEvent: return "DispatchEvent";
        case MessageType::QueryLatest: return "QueryLatest";
        case MessageType::QueryResponse: return "QueryResponse";
        case MessageType::MetricsReq: return "MetricsReq";
        case MessageType::MetricsResp: return "MetricsResp";
        case MessageType::Ack: return "Ack";
        case MessageType::Error: return "Error";
        default: return "Unknown";
    }
}

struct PacketHeader {
    uint32_t magic{kMagicNumber};
    uint8_t  version{kProtocolVersion};
    MessageType type{MessageType::Ping};
    uint16_t flags{0};
    uint32_t seq_id{0};
    uint32_t payload_len{0};
    uint32_t payload_crc{0};

    void encode(ByteBuffer& buf) const;
    static std::optional<PacketHeader> decode(const ByteBuffer& buf, size_t offset = 0);
};

struct Packet {
    PacketHeader header;
    std::vector<uint8_t> payload;

    // Serialization helper
    void serialize(ByteBuffer& buf) const;
    
    // Wire verification
    bool is_crc_valid() const {
        if (payload.empty()) {
            return header.payload_crc == 0;
        }
        return Crc32::calculate(payload.data(), payload.size()) == header.payload_crc;
    }
};

// Domain structs for real-world telemetry & financial metrics
struct TelemetryPoint {
    std::string topic;
    std::string symbol;
    int64_t timestamp_ms{0};
    double value{0.0};
    std::string tags;

    void encode(ByteBuffer& buf) const;
    static std::optional<TelemetryPoint> decode(const uint8_t* data, size_t len);
};

struct SubscribeRequest {
    std::string topic_pattern;

    void encode(ByteBuffer& buf) const;
    static std::optional<SubscribeRequest> decode(const uint8_t* data, size_t len);
};

struct QueryRequest {
    std::string symbol;

    void encode(ByteBuffer& buf) const;
    static std::optional<QueryRequest> decode(const uint8_t* data, size_t len);
};

struct MetricsResponse {
    uint64_t uptime_sec{0};
    uint32_t active_connections{0};
    uint64_t total_packets_received{0};
    uint64_t total_packets_sent{0};
    uint64_t total_telemetry_published{0};
    uint64_t total_bytes_in{0};
    uint64_t total_bytes_out{0};
    uint32_t active_subscriptions{0};
    uint32_t worker_queue_depth{0};

    void encode(ByteBuffer& buf) const;
    static std::optional<MetricsResponse> decode(const uint8_t* data, size_t len);
};

struct ErrorResponse {
    uint32_t code{0};
    std::string message;

    void encode(ByteBuffer& buf) const;
    static std::optional<ErrorResponse> decode(const uint8_t* data, size_t len);
};

// Factory functions to construct outgoing packets easily
Packet make_ping_packet(uint32_t seq_id);
Packet make_pong_packet(uint32_t seq_id);
Packet make_ack_packet(uint32_t seq_id, std::string_view msg = "OK");
Packet make_error_packet(uint32_t seq_id, uint32_t error_code, std::string_view message);
Packet make_telemetry_packet(uint32_t seq_id, const TelemetryPoint& point);
Packet make_dispatch_packet(uint32_t seq_id, const TelemetryPoint& point);
Packet make_subscribe_packet(uint32_t seq_id, std::string_view topic);
Packet make_unsubscribe_packet(uint32_t seq_id, std::string_view topic);
Packet make_query_packet(uint32_t seq_id, std::string_view symbol);
Packet make_query_response_packet(uint32_t seq_id, bool found, const TelemetryPoint& point);
Packet make_metrics_req_packet(uint32_t seq_id);
Packet make_metrics_resp_packet(uint32_t seq_id, const MetricsResponse& metrics);

} // namespace nexus
