#include "nexus/protocol/packet.hpp"
#include "nexus/common/crc32.hpp"
#include <cassert>
#include <iostream>

void test_packet_header_roundtrip() {
    nexus::PacketHeader hdr;
    hdr.magic = nexus::kMagicNumber;
    hdr.version = 1;
    hdr.type = nexus::MessageType::Publish;
    hdr.flags = 0x01;
    hdr.seq_id = 12345;
    hdr.payload_len = 100;
    hdr.payload_crc = 0xCAFEBABE;

    nexus::ByteBuffer buf;
    hdr.encode(buf);
    assert(buf.readable_bytes() == nexus::kHeaderSize);

    auto decoded = nexus::PacketHeader::decode(buf, 0);
    assert(decoded.has_value());
    assert(decoded->magic == hdr.magic);
    assert(decoded->version == hdr.version);
    assert(decoded->type == hdr.type);
    assert(decoded->flags == hdr.flags);
    assert(decoded->seq_id == hdr.seq_id);
    assert(decoded->payload_len == hdr.payload_len);
    assert(decoded->payload_crc == hdr.payload_crc);
    (void)decoded;

    std::cout << "[PASS] test_packet_header_roundtrip\n";
}

void test_telemetry_roundtrip() {
    nexus::TelemetryPoint pt;
    pt.topic = "industrial.sensors.vibration";
    pt.symbol = "TURBINE-07";
    pt.timestamp_ms = 1700000000123LL;
    pt.value = 42.857;
    pt.tags = "site=texas,hz=60";

    nexus::ByteBuffer buf;
    pt.encode(buf);

    auto decoded = nexus::TelemetryPoint::decode(buf.read_ptr(), buf.readable_bytes());
    assert(decoded.has_value());
    assert(decoded->topic == pt.topic);
    assert(decoded->symbol == pt.symbol);
    assert(decoded->timestamp_ms == pt.timestamp_ms);
    assert(std::abs(decoded->value - pt.value) < 1e-6);
    assert(decoded->tags == pt.tags);
    (void)decoded;

    std::cout << "[PASS] test_telemetry_roundtrip\n";
}

void test_crc32_verification() {
    std::string test_data = "Production socket programming in modern C++20 with kqueue";
    uint32_t crc1 = nexus::Crc32::calculate(test_data.data(), test_data.size());

    // Corrupt one byte
    std::string corrupted = test_data;
    corrupted[5] ^= 0xFF;
    uint32_t crc2 = nexus::Crc32::calculate(corrupted.data(), corrupted.size());

    assert(crc1 != crc2);
    (void)crc1;
    (void)crc2;

    // Factory packet check
    nexus::TelemetryPoint pt{"topic", "SYM", 100, 1.23, "tags"};
    nexus::Packet pkt = nexus::make_telemetry_packet(1, pt);
    assert(pkt.is_crc_valid());

    // Tamper with packet payload
    pkt.payload[0] ^= 0x01;
    assert(!pkt.is_crc_valid());

    std::cout << "[PASS] test_crc32_verification\n";
}

int main() {
    std::cout << "Running Protocol tests...\n";
    test_packet_header_roundtrip();
    test_telemetry_roundtrip();
    test_crc32_verification();
    std::cout << "All Protocol tests PASSED!\n";
    return 0;
}
