#include "nexus/protocol/packet.hpp"

namespace nexus {

void PacketHeader::encode(ByteBuffer& buf) const {
    buf.append_uint32(magic);
    buf.append_uint8(version);
    buf.append_uint8(static_cast<uint8_t>(type));
    buf.append_uint16(flags);
    buf.append_uint32(seq_id);
    buf.append_uint32(payload_len);
    buf.append_uint32(payload_crc);
}

std::optional<PacketHeader> PacketHeader::decode(const ByteBuffer& buf, size_t offset) {
    if (buf.readable_bytes() < offset + kHeaderSize) {
        return std::nullopt;
    }

    PacketHeader hdr;
    hdr.magic = buf.peek_uint32(offset);
    if (hdr.magic != kMagicNumber) {
        return std::nullopt;
    }

    hdr.version = buf.peek_uint8(offset + 4);
    hdr.type = static_cast<MessageType>(buf.peek_uint8(offset + 5));
    hdr.flags = buf.peek_uint16(offset + 6);
    hdr.seq_id = buf.peek_uint32(offset + 8);
    hdr.payload_len = buf.peek_uint32(offset + 12);
    hdr.payload_crc = buf.peek_uint32(offset + 16);

    return hdr;
}

void Packet::serialize(ByteBuffer& buf) const {
    header.encode(buf);
    if (!payload.empty()) {
        buf.append(payload.data(), payload.size());
    }
}

// Telemetry Point
void TelemetryPoint::encode(ByteBuffer& buf) const {
    buf.append_uint16(static_cast<uint16_t>(topic.size()));
    buf.append(topic);

    buf.append_uint16(static_cast<uint16_t>(symbol.size()));
    buf.append(symbol);

    buf.append_uint64(static_cast<uint64_t>(timestamp_ms));

    uint64_t val_bits = 0;
    std::memcpy(&val_bits, &value, sizeof(double));
    buf.append_uint64(val_bits);

    buf.append_uint16(static_cast<uint16_t>(tags.size()));
    buf.append(tags);
}

std::optional<TelemetryPoint> TelemetryPoint::decode(const uint8_t* data, size_t len) {
    if (!data || len < 2 + 2 + 8 + 8 + 2) {
        return std::nullopt;
    }

    ByteBuffer buf(len);
    buf.append(data, len);

    TelemetryPoint tp;
    uint16_t topic_len = buf.read_uint16();
    if (buf.readable_bytes() < topic_len) return std::nullopt;
    tp.topic.assign(reinterpret_cast<const char*>(buf.read_ptr()), topic_len);
    buf.consume(topic_len);

    if (buf.readable_bytes() < 2) return std::nullopt;
    uint16_t sym_len = buf.read_uint16();
    if (buf.readable_bytes() < sym_len) return std::nullopt;
    tp.symbol.assign(reinterpret_cast<const char*>(buf.read_ptr()), sym_len);
    buf.consume(sym_len);

    if (buf.readable_bytes() < 8 + 8 + 2) return std::nullopt;
    tp.timestamp_ms = static_cast<int64_t>(buf.read_uint64());

    uint64_t val_bits = buf.read_uint64();
    std::memcpy(&tp.value, &val_bits, sizeof(double));

    uint16_t tags_len = buf.read_uint16();
    if (buf.readable_bytes() < tags_len) return std::nullopt;
    tp.tags.assign(reinterpret_cast<const char*>(buf.read_ptr()), tags_len);
    buf.consume(tags_len);

    return tp;
}

// SubscribeRequest
void SubscribeRequest::encode(ByteBuffer& buf) const {
    buf.append_uint16(static_cast<uint16_t>(topic_pattern.size()));
    buf.append(topic_pattern);
}

std::optional<SubscribeRequest> SubscribeRequest::decode(const uint8_t* data, size_t len) {
    if (!data || len < 2) return std::nullopt;
    ByteBuffer buf(len);
    buf.append(data, len);
    uint16_t pat_len = buf.read_uint16();
    if (buf.readable_bytes() < pat_len) return std::nullopt;
    SubscribeRequest req;
    req.topic_pattern.assign(reinterpret_cast<const char*>(buf.read_ptr()), pat_len);
    return req;
}

// QueryRequest
void QueryRequest::encode(ByteBuffer& buf) const {
    buf.append_uint16(static_cast<uint16_t>(symbol.size()));
    buf.append(symbol);
}

std::optional<QueryRequest> QueryRequest::decode(const uint8_t* data, size_t len) {
    if (!data || len < 2) return std::nullopt;
    ByteBuffer buf(len);
    buf.append(data, len);
    uint16_t sym_len = buf.read_uint16();
    if (buf.readable_bytes() < sym_len) return std::nullopt;
    QueryRequest req;
    req.symbol.assign(reinterpret_cast<const char*>(buf.read_ptr()), sym_len);
    return req;
}

// MetricsResponse
void MetricsResponse::encode(ByteBuffer& buf) const {
    buf.append_uint64(uptime_sec);
    buf.append_uint32(active_connections);
    buf.append_uint64(total_packets_received);
    buf.append_uint64(total_packets_sent);
    buf.append_uint64(total_telemetry_published);
    buf.append_uint64(total_bytes_in);
    buf.append_uint64(total_bytes_out);
    buf.append_uint32(active_subscriptions);
    buf.append_uint32(worker_queue_depth);
}

std::optional<MetricsResponse> MetricsResponse::decode(const uint8_t* data, size_t len) {
    if (!data || len < 8 + 4 + 8 + 8 + 8 + 8 + 8 + 4 + 4) return std::nullopt;
    ByteBuffer buf(len);
    buf.append(data, len);
    MetricsResponse m;
    m.uptime_sec = buf.read_uint64();
    m.active_connections = buf.read_uint32();
    m.total_packets_received = buf.read_uint64();
    m.total_packets_sent = buf.read_uint64();
    m.total_telemetry_published = buf.read_uint64();
    m.total_bytes_in = buf.read_uint64();
    m.total_bytes_out = buf.read_uint64();
    m.active_subscriptions = buf.read_uint32();
    m.worker_queue_depth = buf.read_uint32();
    return m;
}

// ErrorResponse
void ErrorResponse::encode(ByteBuffer& buf) const {
    buf.append_uint32(code);
    buf.append_uint16(static_cast<uint16_t>(message.size()));
    buf.append(message);
}

std::optional<ErrorResponse> ErrorResponse::decode(const uint8_t* data, size_t len) {
    if (!data || len < 6) return std::nullopt;
    ByteBuffer buf(len);
    buf.append(data, len);
    ErrorResponse err;
    err.code = buf.read_uint32();
    uint16_t msg_len = buf.read_uint16();
    if (buf.readable_bytes() < msg_len) return std::nullopt;
    err.message.assign(reinterpret_cast<const char*>(buf.read_ptr()), msg_len);
    return err;
}

// Factory helpers
static Packet create_packet_with_payload(MessageType type, uint32_t seq_id, const ByteBuffer& payload_buf) {
    Packet pkt;
    pkt.header.magic = kMagicNumber;
    pkt.header.version = kProtocolVersion;
    pkt.header.type = type;
    pkt.header.flags = 0;
    pkt.header.seq_id = seq_id;
    pkt.header.payload_len = static_cast<uint32_t>(payload_buf.readable_bytes());

    if (pkt.header.payload_len > 0) {
        pkt.payload.resize(pkt.header.payload_len);
        std::memcpy(pkt.payload.data(), payload_buf.read_ptr(), pkt.header.payload_len);
        pkt.header.payload_crc = Crc32::calculate(pkt.payload.data(), pkt.payload.size());
    } else {
        pkt.header.payload_crc = 0;
    }
    return pkt;
}

Packet make_ping_packet(uint32_t seq_id) {
    ByteBuffer empty;
    return create_packet_with_payload(MessageType::Ping, seq_id, empty);
}

Packet make_pong_packet(uint32_t seq_id) {
    ByteBuffer empty;
    return create_packet_with_payload(MessageType::Pong, seq_id, empty);
}

Packet make_ack_packet(uint32_t seq_id, std::string_view msg) {
    ByteBuffer buf;
    buf.append_uint16(static_cast<uint16_t>(msg.size()));
    buf.append(msg);
    return create_packet_with_payload(MessageType::Ack, seq_id, buf);
}

Packet make_error_packet(uint32_t seq_id, uint32_t error_code, std::string_view message) {
    ErrorResponse resp{error_code, std::string(message)};
    ByteBuffer buf;
    resp.encode(buf);
    return create_packet_with_payload(MessageType::Error, seq_id, buf);
}

Packet make_telemetry_packet(uint32_t seq_id, const TelemetryPoint& point) {
    ByteBuffer buf;
    point.encode(buf);
    return create_packet_with_payload(MessageType::Publish, seq_id, buf);
}

Packet make_dispatch_packet(uint32_t seq_id, const TelemetryPoint& point) {
    ByteBuffer buf;
    point.encode(buf);
    return create_packet_with_payload(MessageType::DispatchEvent, seq_id, buf);
}

Packet make_subscribe_packet(uint32_t seq_id, std::string_view topic) {
    SubscribeRequest req{std::string(topic)};
    ByteBuffer buf;
    req.encode(buf);
    return create_packet_with_payload(MessageType::Subscribe, seq_id, buf);
}

Packet make_unsubscribe_packet(uint32_t seq_id, std::string_view topic) {
    SubscribeRequest req{std::string(topic)};
    ByteBuffer buf;
    req.encode(buf);
    return create_packet_with_payload(MessageType::Unsubscribe, seq_id, buf);
}

Packet make_query_packet(uint32_t seq_id, std::string_view symbol) {
    QueryRequest req{std::string(symbol)};
    ByteBuffer buf;
    req.encode(buf);
    return create_packet_with_payload(MessageType::QueryLatest, seq_id, buf);
}

Packet make_query_response_packet(uint32_t seq_id, bool found, const TelemetryPoint& point) {
    ByteBuffer buf;
    buf.append_uint8(found ? 1 : 0);
    if (found) {
        point.encode(buf);
    }
    return create_packet_with_payload(MessageType::QueryResponse, seq_id, buf);
}

Packet make_metrics_req_packet(uint32_t seq_id) {
    ByteBuffer empty;
    return create_packet_with_payload(MessageType::MetricsReq, seq_id, empty);
}

Packet make_metrics_resp_packet(uint32_t seq_id, const MetricsResponse& metrics) {
    ByteBuffer buf;
    metrics.encode(buf);
    return create_packet_with_payload(MessageType::MetricsResp, seq_id, buf);
}

} // namespace nexus
