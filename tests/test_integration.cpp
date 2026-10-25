#include "nexus/net/tcp_server.hpp"
#include "nexus/net/socket.hpp"
#include "nexus/protocol/packet.hpp"
#include "nexus/common/logger.hpp"

#include <cassert>
#include <iostream>
#include <thread>
#include <chrono>
#include <poll.h>

namespace {

void send_packet_blocking(int fd, const nexus::Packet& pkt) {
    nexus::ByteBuffer buf;
    pkt.serialize(buf);
    size_t to_send = buf.readable_bytes();
    const uint8_t* p = buf.read_ptr();
    while (to_send > 0) {
        ssize_t n = ::send(fd, p, to_send, 0);
        if (n <= 0) {
            throw std::runtime_error("send_packet_blocking failed");
        }
        p += n;
        to_send -= static_cast<size_t>(n);
    }
}

nexus::Packet recv_packet_blocking(int fd, int timeout_ms = 5000) {
    nexus::ByteBuffer buf(4096);
    while (true) {
        if (buf.readable_bytes() >= nexus::kHeaderSize) {
            auto hdr = nexus::PacketHeader::decode(buf, 0);
            if (hdr && buf.readable_bytes() >= nexus::kHeaderSize + hdr->payload_len) {
                buf.consume(nexus::kHeaderSize);
                nexus::Packet pkt;
                pkt.header = *hdr;
                if (hdr->payload_len > 0) {
                    pkt.payload.resize(hdr->payload_len);
                    std::memcpy(pkt.payload.data(), buf.read_ptr(), hdr->payload_len);
                    buf.consume(hdr->payload_len);
                }
                return pkt;
            }
        }

        pollfd pfd{};
        pfd.fd = fd;
        pfd.events = POLLIN;
        int p_res = ::poll(&pfd, 1, timeout_ms);
        if (p_res <= 0) {
            throw std::runtime_error("recv_packet_blocking timed out");
        }

        buf.ensure_writable(2048);
        ssize_t n = ::recv(fd, buf.write_ptr(), buf.writable_bytes(), 0);
        if (n <= 0) {
            throw std::runtime_error("recv_packet_blocking connection closed");
        }
        buf.commit_write(static_cast<size_t>(n));
    }
}

} // namespace

int main() {
    nexus::Logger::instance().set_level(nexus::LogLevel::Warn);
    std::cout << "Starting end-to-end integration test...\n";

    nexus::ServerConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 9191;
    cfg.worker_threads = 2;
    cfg.idle_timeout_sec = 10;

    auto server = std::make_unique<nexus::TcpServer>(cfg);
    std::thread server_thread([&]() {
        server->start();
    });

    // Give server time to initialize
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    nexus::Endpoint server_ep;
    server_ep.ip = "127.0.0.1";
    server_ep.port = 9191;

    // 1. Connect Subscriber
    nexus::Socket sub_sock = nexus::Socket::create_tcp();
    if (!sub_sock.connect(server_ep, 2000)) {
        std::cerr << "Failed to connect sub_sock: " << std::strerror(errno) << "\n";
        return 1;
    }
    sub_sock.set_nonblocking(false);
    sub_sock.set_tcp_nodelay(true);
    int sub_fd = sub_sock.release();

    // Subscribe to "finance.crypto.*"
    send_packet_blocking(sub_fd, nexus::make_subscribe_packet(1, "finance.crypto.*"));
    nexus::Packet ack1 = recv_packet_blocking(sub_fd);
    assert(ack1.header.type == nexus::MessageType::Ack);
    std::cout << "[PASS] Subscriber successfully registered.\n";

    // 2. Connect Publisher
    nexus::Socket pub_sock = nexus::Socket::create_tcp();
    if (!pub_sock.connect(server_ep, 2000)) {
        std::cerr << "Failed to connect pub_sock\n";
        return 1;
    }
    pub_sock.set_nonblocking(false);
    pub_sock.set_tcp_nodelay(true);
    int pub_fd = pub_sock.release();

    nexus::TelemetryPoint eth_point;
    eth_point.topic = "finance.crypto.ETH";
    eth_point.symbol = "ETH";
    eth_point.value = 3450.75;
    eth_point.tags = "exchange=kraken";
    eth_point.timestamp_ms = 1700000000555LL;

    // Publish telemetry
    send_packet_blocking(pub_fd, nexus::make_telemetry_packet(2, eth_point));

    // Publisher should receive ACK
    nexus::Packet pub_ack = recv_packet_blocking(pub_fd);
    assert(pub_ack.header.type == nexus::MessageType::Ack);
    std::cout << "[PASS] Publisher received publish ACK.\n";

    // Subscriber should receive DispatchEvent
    nexus::Packet dispatch_event = recv_packet_blocking(sub_fd);
    assert(dispatch_event.header.type == nexus::MessageType::DispatchEvent);
    auto recv_point = nexus::TelemetryPoint::decode(dispatch_event.payload.data(), dispatch_event.payload.size());
    assert(recv_point.has_value());
    assert(recv_point->symbol == "ETH");
    assert(recv_point->topic == "finance.crypto.ETH");
    assert(std::abs(recv_point->value - 3450.75) < 1e-5);
    std::cout << "[PASS] Subscriber received routed event with exact payload.\n";

    // 3. Connect Query client to verify In-Memory State Cache
    nexus::Socket query_sock = nexus::Socket::create_tcp();
    if (!query_sock.connect(server_ep, 2000)) {
        std::cerr << "Failed to connect query_sock\n";
        return 1;
    }
    query_sock.set_nonblocking(false);
    query_sock.set_tcp_nodelay(true);
    int query_fd = query_sock.release();

    send_packet_blocking(query_fd, nexus::make_query_packet(3, "ETH"));
    nexus::Packet query_resp = recv_packet_blocking(query_fd);
    assert(query_resp.header.type == nexus::MessageType::QueryResponse);
    assert(!query_resp.payload.empty() && query_resp.payload[0] == 1);
    auto query_point = nexus::TelemetryPoint::decode(query_resp.payload.data() + 1, query_resp.payload.size() - 1);
    assert(query_point.has_value());
    assert(query_point->symbol == "ETH");
    assert(std::abs(query_point->value - 3450.75) < 1e-5);
    std::cout << "[PASS] Query request retrieved cached symbol state correctly.\n";

    // 4. Ping-Pong test
    send_packet_blocking(query_fd, nexus::make_ping_packet(4));
    nexus::Packet pong_resp = recv_packet_blocking(query_fd);
    assert(pong_resp.header.type == nexus::MessageType::Pong);
    assert(pong_resp.header.seq_id == 4);
    std::cout << "[PASS] Heartbeat Ping/Pong verified.\n";

    // 5. Cleanup
    ::close(sub_fd);
    ::close(pub_fd);
    ::close(query_fd);

    server->stop();
    if (server_thread.joinable()) {
        server_thread.join();
    }

    std::cout << "\n>>> ALL INTEGRATION TESTS PASSED SUCCESSFULLY! <<<\n";
    return 0;
}
