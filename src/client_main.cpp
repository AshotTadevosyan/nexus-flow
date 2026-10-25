#include "nexus/net/socket.hpp"
#include "nexus/protocol/packet.hpp"
#include "nexus/common/types.hpp"

#include <iostream>
#include <string>
#include <sstream>
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>
#include <iomanip>

namespace {

std::atomic<bool> g_running{true};
std::atomic<uint32_t> g_seq_id{1};

void receiver_loop(int fd) {
    nexus::ByteBuffer in_buf(16384);

    while (g_running.load()) {
        in_buf.ensure_writable(4096);
        ssize_t n = ::recv(fd, in_buf.write_ptr(), in_buf.writable_bytes(), 0);
        if (n <= 0) {
            if (g_running.load()) {
                std::cout << "\n\033[31m[NexusCLI] Server disconnected or connection lost.\033[0m\n> " << std::flush;
            }
            g_running.store(false);
            break;
        }
        in_buf.commit_write(static_cast<size_t>(n));

        while (in_buf.readable_bytes() >= nexus::kHeaderSize) {
            auto hdr_opt = nexus::PacketHeader::decode(in_buf, 0);
            if (!hdr_opt) {
                std::cerr << "\n[NexusCLI] Malformed header received.\n> " << std::flush;
                g_running.store(false);
                return;
            }

            const auto& hdr = *hdr_opt;
            if (in_buf.readable_bytes() < nexus::kHeaderSize + hdr.payload_len) {
                break; // Wait for full packet
            }

            in_buf.consume(nexus::kHeaderSize);
            std::vector<uint8_t> payload(hdr.payload_len);
            if (hdr.payload_len > 0) {
                std::memcpy(payload.data(), in_buf.read_ptr(), hdr.payload_len);
                in_buf.consume(hdr.payload_len);
            }

            // Handle packet by type
            switch (hdr.type) {
                case nexus::MessageType::Pong: {
                    std::cout << "\n\033[32m[PONG]\033[0m seq=" << hdr.seq_id << "\n> " << std::flush;
                    break;
                }
                case nexus::MessageType::Ack: {
                    std::string msg;
                    if (payload.size() >= 2) {
                        uint16_t msg_len = static_cast<uint16_t>((static_cast<uint16_t>(payload[0]) << 8) | static_cast<uint16_t>(payload[1]));
                        if (payload.size() >= 2 + msg_len) {
                            msg.assign(reinterpret_cast<const char*>(payload.data() + 2), msg_len);
                        }
                    }
                    std::cout << "\n\033[32m[ACK]\033[0m seq=" << hdr.seq_id << " status=" << msg << "\n> " << std::flush;
                    break;
                }
                case nexus::MessageType::DispatchEvent: {
                    auto pt = nexus::TelemetryPoint::decode(payload.data(), payload.size());
                    if (pt) {
                        std::cout << "\n\033[36m[EVENT]\033[0m Topic: \033[1m" << pt->topic << "\033[0m"
                                  << " | Symbol: " << pt->symbol
                                  << " | Value: \033[33m" << pt->value << "\033[0m"
                                  << " | Tags: " << pt->tags
                                  << " | Time: " << pt->timestamp_ms << "ms\n> " << std::flush;
                    }
                    break;
                }
                case nexus::MessageType::QueryResponse: {
                    if (!payload.empty() && payload[0] == 1) {
                        auto pt = nexus::TelemetryPoint::decode(payload.data() + 1, payload.size() - 1);
                        if (pt) {
                            std::cout << "\n\033[32m[QUERY RESULT]\033[0m Symbol: " << pt->symbol
                                      << " | Latest Value: \033[33m" << pt->value << "\033[0m"
                                      << " | Topic: " << pt->topic
                                      << " | Tags: " << pt->tags
                                      << " | Updated: " << pt->timestamp_ms << "ms\n> " << std::flush;
                        }
                    } else {
                        std::cout << "\n\033[33m[QUERY RESULT]\033[0m Symbol not found in active telemetry store.\n> " << std::flush;
                    }
                    break;
                }
                case nexus::MessageType::MetricsResp: {
                    auto m = nexus::MetricsResponse::decode(payload.data(), payload.size());
                    if (m) {
                        std::cout << "\n\033[34m--- NexusFlow Server Metrics ---\033[0m\n"
                                  << "  Uptime:                " << m->uptime_sec << " seconds\n"
                                  << "  Active Connections:    " << m->active_connections << "\n"
                                  << "  Active Subscriptions:  " << m->active_subscriptions << "\n"
                                  << "  Packets In / Out:      " << m->total_packets_received << " / " << m->total_packets_sent << "\n"
                                  << "  Telemetry Published:   " << m->total_telemetry_published << "\n"
                                  << "  Bytes In / Out:        " << (m->total_bytes_in / 1024) << " KB / " << (m->total_bytes_out / 1024) << " KB\n"
                                  << "\033[34m--------------------------------\033[0m\n> " << std::flush;
                    }
                    break;
                }
                case nexus::MessageType::Error: {
                    auto err = nexus::ErrorResponse::decode(payload.data(), payload.size());
                    if (err) {
                        std::cout << "\n\033[31m[ERROR]\033[0m Code: " << err->code
                                  << " Reason: " << err->message << "\n> " << std::flush;
                    }
                    break;
                }
                default:
                    std::cout << "\n[MSG] Type: " << static_cast<int>(hdr.type) << "\n> " << std::flush;
                    break;
            }
        }
    }
}

void send_packet(int fd, const nexus::Packet& pkt) {
    nexus::ByteBuffer buf;
    pkt.serialize(buf);
    size_t to_send = buf.readable_bytes();
    const uint8_t* p = buf.read_ptr();
    while (to_send > 0) {
        ssize_t n = ::send(fd, p, to_send, 0);
        if (n <= 0) break;
        p += n;
        to_send -= static_cast<size_t>(n);
    }
}

} // namespace

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    uint16_t port = 9090;

    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = static_cast<uint16_t>(std::atoi(argv[2]));

    nexus::Endpoint ep;
    ep.ip = host;
    ep.port = port;

    nexus::Socket client_sock = nexus::Socket::create_tcp();
    std::cout << "Connecting to NexusFlow Broker at " << ep.to_string() << "...\n";

    if (!client_sock.connect(ep, 5000)) {
        std::cerr << "\033[31mError: Unable to connect to " << ep.to_string() << "\033[0m\n";
        return 1;
    }

    client_sock.set_nonblocking(false);
    client_sock.set_tcp_nodelay(true);
    int fd = client_sock.release();

    std::cout << "\033[32mSuccessfully connected!\033[0m\n";
    std::cout << "Type 'help' for commands, 'exit' to quit.\n";

    std::thread receiver(receiver_loop, fd);

    std::string line;
    std::cout << "> " << std::flush;
    while (g_running.load() && std::getline(std::cin, line)) {
        if (line.empty()) {
            std::cout << "> " << std::flush;
            continue;
        }

        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;

        if (cmd == "exit" || cmd == "quit") {
            break;
        } else if (cmd == "help") {
            std::cout << "Available Commands:\n"
                      << "  pub <topic> <symbol> <val> [tags]   Publish telemetry data point\n"
                      << "  sub <topic_pattern>                 Subscribe to topic (e.g. market.*, sensors.temp)\n"
                      << "  unsub <topic_pattern>               Unsubscribe from topic\n"
                      << "  query <symbol>                      Query latest snapshot for symbol\n"
                      << "  stats                               Request broker runtime metrics\n"
                      << "  ping                                Send heartbeat ping\n"
                      << "  exit, quit                          Disconnect and exit\n";
        } else if (cmd == "ping") {
            nexus::Packet pkt = nexus::make_ping_packet(g_seq_id++);
            send_packet(fd, pkt);
        } else if (cmd == "stats") {
            nexus::Packet pkt = nexus::make_metrics_req_packet(g_seq_id++);
            send_packet(fd, pkt);
        } else if (cmd == "sub") {
            std::string topic;
            if (iss >> topic) {
                nexus::Packet pkt = nexus::make_subscribe_packet(g_seq_id++, topic);
                send_packet(fd, pkt);
            } else {
                std::cout << "Usage: sub <topic>\n";
            }
        } else if (cmd == "unsub") {
            std::string topic;
            if (iss >> topic) {
                nexus::Packet pkt = nexus::make_unsubscribe_packet(g_seq_id++, topic);
                send_packet(fd, pkt);
            } else {
                std::cout << "Usage: unsub <topic>\n";
            }
        } else if (cmd == "pub") {
            std::string topic, symbol;
            double value = 0.0;
            std::string tags;
            if (iss >> topic >> symbol >> value) {
                std::string rem;
                std::getline(iss, rem);
                if (!rem.empty() && rem.front() == ' ') rem.erase(0, 1);
                tags = rem;

                nexus::TelemetryPoint pt;
                pt.topic = topic;
                pt.symbol = symbol;
                pt.value = value;
                pt.tags = tags;
                pt.timestamp_ms = nexus::current_timestamp_ms();

                nexus::Packet pkt = nexus::make_telemetry_packet(g_seq_id++, pt);
                send_packet(fd, pkt);
            } else {
                std::cout << "Usage: pub <topic> <symbol> <val> [tags]\n";
            }
        } else if (cmd == "query") {
            std::string symbol;
            if (iss >> symbol) {
                nexus::Packet pkt = nexus::make_query_packet(g_seq_id++, symbol);
                send_packet(fd, pkt);
            } else {
                std::cout << "Usage: query <symbol>\n";
            }
        } else {
            std::cout << "Unknown command: '" << cmd << "'. Type 'help' for usage.\n";
        }

        std::cout << "> " << std::flush;
    }

    g_running.store(false);
    ::close(fd);
    if (receiver.joinable()) {
        receiver.join();
    }

    std::cout << "Disconnected.\n";
    return 0;
}
