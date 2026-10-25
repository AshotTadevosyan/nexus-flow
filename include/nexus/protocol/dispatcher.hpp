#pragma once

#include "nexus/net/connection.hpp"
#include "nexus/protocol/packet.hpp"

#include <unordered_map>
#include <unordered_set>
#include <shared_mutex>
#include <string>
#include <vector>
#include <atomic>
#include <chrono>

namespace nexus {

struct SymbolStats {
    TelemetryPoint latest_point;
    uint64_t update_count{0};
    double min_value{0.0};
    double max_value{0.0};
    double sum_values{0.0};

    void update(const TelemetryPoint& pt) {
        if (update_count == 0) {
            min_value = pt.value;
            max_value = pt.value;
        } else {
            if (pt.value < min_value) min_value = pt.value;
            if (pt.value > max_value) max_value = pt.value;
        }
        sum_values += pt.value;
        latest_point = pt;
        ++update_count;
    }
};

class Dispatcher {
public:
    Dispatcher();

    // Connection lifecycle
    void on_client_disconnected(int client_fd);

    // Packet processing
    void handle_packet(const ConnectionPtr& conn, const Packet& packet);

    // Business handlers
    void handle_publish(const ConnectionPtr& conn, const Packet& packet);
    void handle_subscribe(const ConnectionPtr& conn, const Packet& packet);
    void handle_unsubscribe(const ConnectionPtr& conn, const Packet& packet);
    void handle_query(const ConnectionPtr& conn, const Packet& packet);
    void handle_metrics(const ConnectionPtr& conn, const Packet& packet);
    void handle_ping(const ConnectionPtr& conn, const Packet& packet);

    // Registration for routing
    void register_connection(const ConnectionPtr& conn);
    void unregister_connection(int client_fd);

    // Metrics snapshot
    MetricsResponse get_metrics_snapshot() const;

    void increment_bytes_in(uint64_t bytes) { total_bytes_in_ += bytes; }
    void increment_bytes_out(uint64_t bytes) { total_bytes_out_ += bytes; }
    void increment_packets_in() { ++total_packets_received_; }
    void increment_packets_out() { ++total_packets_sent_; }

private:
    bool topic_matches(std::string_view pattern, std::string_view topic) const;

    // Concurrency protection for subscription routing
    mutable std::shared_mutex sub_mutex_;
    // client_fd -> ConnectionPtr
    std::unordered_map<int, ConnectionPtr> active_connections_;
    // topic_pattern -> set of client_fds
    std::unordered_map<std::string, std::unordered_set<int>> topic_subscribers_;

    // Concurrency protection for in-memory telemetry state
    mutable std::shared_mutex telemetry_mutex_;
    std::unordered_map<std::string, SymbolStats> telemetry_store_;

    // Performance & Observability counters
    std::chrono::steady_clock::time_point start_time_;
    std::atomic<uint64_t> total_packets_received_{0};
    std::atomic<uint64_t> total_packets_sent_{0};
    std::atomic<uint64_t> total_telemetry_published_{0};
    std::atomic<uint64_t> total_bytes_in_{0};
    std::atomic<uint64_t> total_bytes_out_{0};
};

} // namespace nexus
