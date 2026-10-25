#include "nexus/net/socket.hpp"
#include "nexus/protocol/packet.hpp"
#include "nexus/common/types.hpp"

#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <iomanip>

#include <poll.h>

struct BenchmarkConfig {
    std::string host{"127.0.0.1"};
    uint16_t port{9090};
    int num_clients{10};
    int msgs_per_client{5000};
    int num_subscribers{2};
};

struct ClientStats {
    std::vector<int64_t> latencies_us;
    uint64_t bytes_sent{0};
    uint64_t bytes_received{0};
    uint64_t errors{0};
};

void run_subscriber(int id, const BenchmarkConfig& config, std::atomic<bool>& stop_signal, std::atomic<uint64_t>& sub_received) {
    nexus::Endpoint ep;
    ep.ip = config.host;
    ep.port = config.port;

    nexus::Socket sock = nexus::Socket::create_tcp();
    if (!sock.connect(ep, 5000)) {
        return;
    }
    sock.set_nonblocking(false);
    sock.set_tcp_nodelay(true);
    int fd = sock.release();

    // Subscribe to all topics "*"
    nexus::Packet sub_pkt = nexus::make_subscribe_packet(static_cast<uint32_t>(id), "*");
    nexus::ByteBuffer buf;
    sub_pkt.serialize(buf);
    ::send(fd, buf.read_ptr(), buf.readable_bytes(), 0);

    nexus::ByteBuffer in_buf(65536);
    pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLIN;

    while (!stop_signal.load(std::memory_order_relaxed)) {
        int pr = ::poll(&pfd, 1, 100);
        if (pr <= 0) continue;

        in_buf.ensure_writable(16384);
        ssize_t n = ::recv(fd, in_buf.write_ptr(), in_buf.writable_bytes(), 0);
        if (n <= 0) break;
        in_buf.commit_write(static_cast<size_t>(n));

        while (in_buf.readable_bytes() >= nexus::kHeaderSize) {
            auto hdr = nexus::PacketHeader::decode(in_buf, 0);
            if (!hdr) break;
            if (in_buf.readable_bytes() < nexus::kHeaderSize + hdr->payload_len) break;
            in_buf.consume(nexus::kHeaderSize + hdr->payload_len);
            if (hdr->type == nexus::MessageType::DispatchEvent) {
                sub_received.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }
    ::close(fd);
}

void run_publisher(int id, const BenchmarkConfig& config, std::atomic<bool>& start_gate, ClientStats& stats) {
    nexus::Endpoint ep;
    ep.ip = config.host;
    ep.port = config.port;

    nexus::Socket sock = nexus::Socket::create_tcp();
    if (!sock.connect(ep, 5000)) {
        ++stats.errors;
        return;
    }
    sock.set_nonblocking(false);
    sock.set_tcp_nodelay(true);
    int fd = sock.release();

    stats.latencies_us.reserve(static_cast<size_t>(config.msgs_per_client));

    // Wait for synchronized start
    while (!start_gate.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    nexus::ByteBuffer send_buf(4096);
    nexus::ByteBuffer in_buf(4096);

    std::string symbol = "SYM_" + std::to_string(id);
    std::string topic = "market.tickers." + symbol;

    for (int i = 0; i < config.msgs_per_client; ++i) {
        nexus::TelemetryPoint pt;
        pt.topic = topic;
        pt.symbol = symbol;
        pt.value = 100.0 + (i % 50) + 0.25;
        pt.tags = "exchange=bench";
        pt.timestamp_ms = nexus::current_timestamp_ms();

        uint32_t seq = static_cast<uint32_t>((id << 16) | (i & 0xFFFF));
        nexus::Packet pkt = nexus::make_telemetry_packet(seq, pt);

        send_buf.clear();
        pkt.serialize(send_buf);

        auto t_start = std::chrono::high_resolution_clock::now();

        // Send packet
        ssize_t sent = ::send(fd, send_buf.read_ptr(), send_buf.readable_bytes(), 0);
        if (sent <= 0) {
            ++stats.errors;
            break;
        }
        stats.bytes_sent += static_cast<uint64_t>(sent);

        // Read ACK response
        bool acked = false;
        while (!acked) {
            in_buf.ensure_writable(2048);
            ssize_t n = ::recv(fd, in_buf.write_ptr(), in_buf.writable_bytes(), 0);
            if (n <= 0) {
                ++stats.errors;
                break;
            }
            stats.bytes_received += static_cast<uint64_t>(n);
            in_buf.commit_write(static_cast<size_t>(n));

            while (in_buf.readable_bytes() >= nexus::kHeaderSize) {
                auto hdr = nexus::PacketHeader::decode(in_buf, 0);
                if (!hdr) break;
                if (in_buf.readable_bytes() < nexus::kHeaderSize + hdr->payload_len) break;
                in_buf.consume(nexus::kHeaderSize + hdr->payload_len);
                if (hdr->type == nexus::MessageType::Ack && hdr->seq_id == seq) {
                    acked = true;
                    break;
                }
            }
        }

        auto t_end = std::chrono::high_resolution_clock::now();
        int64_t elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(t_end - t_start).count();
        stats.latencies_us.push_back(elapsed_us);
    }

    ::close(fd);
}

int main(int argc, char* argv[]) {
    BenchmarkConfig config;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--clients" && i + 1 < argc) config.num_clients = std::atoi(argv[++i]);
        else if (arg == "--messages" && i + 1 < argc) config.msgs_per_client = std::atoi(argv[++i]);
        else if (arg == "--subscribers" && i + 1 < argc) config.num_subscribers = std::atoi(argv[++i]);
        else if (arg == "--port" && i + 1 < argc) config.port = static_cast<uint16_t>(std::atoi(argv[++i]));
        else if (arg == "--host" && i + 1 < argc) config.host = argv[++i];
    }

    std::cout << "========================================================\n"
              << "          NexusFlow Load & Latency Benchmark            \n"
              << "========================================================\n"
              << "  Target:               " << config.host << ":" << config.port << "\n"
              << "  Publisher Clients:    " << config.num_clients << "\n"
              << "  Messages per Client:  " << config.msgs_per_client << "\n"
              << "  Total Messages:       " << (config.num_clients * config.msgs_per_client) << "\n"
              << "  Subscribers:          " << config.num_subscribers << "\n"
              << "========================================================\n"
              << "Connecting clients and preparing benchmark...\n";

    std::atomic<bool> start_gate{false};
    std::atomic<bool> stop_subscribers{false};
    std::atomic<uint64_t> sub_received{0};

    // Spawn subscriber threads
    std::vector<std::thread> sub_threads;
    for (int i = 0; i < config.num_subscribers; ++i) {
        sub_threads.emplace_back(run_subscriber, 1000 + i, config, std::ref(stop_subscribers), std::ref(sub_received));
    }

    // Give subscribers 100ms to register
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Spawn publisher threads
    std::vector<ClientStats> all_stats(static_cast<size_t>(config.num_clients));
    std::vector<std::thread> pub_threads;
    for (int i = 0; i < config.num_clients; ++i) {
        pub_threads.emplace_back(run_publisher, i + 1, config, std::ref(start_gate), std::ref(all_stats[static_cast<size_t>(i)]));
    }

    // Unleash all publishers simultaneously
    auto start_time = std::chrono::high_resolution_clock::now();
    start_gate.store(true, std::memory_order_release);

    for (auto& t : pub_threads) {
        if (t.joinable()) t.join();
    }
    auto end_time = std::chrono::high_resolution_clock::now();

    // Give a brief moment for pending broadcasts then stop subscribers
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    stop_subscribers.store(true);
    for (auto& t : sub_threads) {
        if (t.joinable()) t.join();
    }

    double total_sec = std::chrono::duration<double>(end_time - start_time).count();

    // Aggregate statistics
    std::vector<int64_t> all_latencies;
    uint64_t total_bytes_sent = 0;
    uint64_t total_errors = 0;
    for (const auto& s : all_stats) {
        all_latencies.insert(all_latencies.end(), s.latencies_us.begin(), s.latencies_us.end());
        total_bytes_sent += s.bytes_sent;
        total_errors += s.errors;
    }

    std::sort(all_latencies.begin(), all_latencies.end());

    size_t total_msgs = all_latencies.size();
    double throughput = total_sec > 0 ? (static_cast<double>(total_msgs) / total_sec) : 0.0;
    double mb_sec = total_sec > 0 ? ((static_cast<double>(total_bytes_sent) / (1024.0 * 1024.0)) / total_sec) : 0.0;

    auto percentile = [&](double p) -> int64_t {
        if (all_latencies.empty()) return 0;
        size_t idx = static_cast<size_t>(p * static_cast<double>(all_latencies.size()));
        if (idx >= all_latencies.size()) idx = all_latencies.size() - 1;
        return all_latencies[idx];
    };

    double avg_lat = all_latencies.empty() ? 0.0 :
        (std::accumulate(all_latencies.begin(), all_latencies.end(), 0.0) / static_cast<double>(all_latencies.size()));

    std::cout << "\n================ Benchmark Results ================\n"
              << std::fixed << std::setprecision(2)
              << "  Execution Time:       " << total_sec << " seconds\n"
              << "  Messages Completed:   " << total_msgs << " (Errors: " << total_errors << ")\n"
              << "  Throughput:           " << throughput << " msgs/sec\n"
              << "  Network Bandwidth:    " << mb_sec << " MB/sec\n"
              << "  Subscriber Received:  " << sub_received.load() << " broadcasts\n"
              << "---------------- Latency Profile ------------------\n"
              << "  Min Latency:          " << (all_latencies.empty() ? 0 : all_latencies.front()) << " us\n"
              << "  Average Latency:      " << avg_lat << " us\n"
              << "  P50  (Median):        " << percentile(0.50) << " us\n"
              << "  P90:                  " << percentile(0.90) << " us\n"
              << "  P95:                  " << percentile(0.95) << " us\n"
              << "  P99:                  " << percentile(0.99) << " us\n"
              << "  Max Latency:          " << (all_latencies.empty() ? 0 : all_latencies.back()) << " us\n"
              << "===================================================\n";

    return 0;
}
