#include "nexus/protocol/dispatcher.hpp"
#include "nexus/common/logger.hpp"

namespace nexus {

Dispatcher::Dispatcher() : start_time_(std::chrono::steady_clock::now()) {}

void Dispatcher::register_connection(const ConnectionPtr& conn) {
    std::unique_lock<std::shared_mutex> lock(sub_mutex_);
    active_connections_[conn->fd()] = conn;
}

void Dispatcher::unregister_connection(int client_fd) {
    on_client_disconnected(client_fd);
}

void Dispatcher::on_client_disconnected(int client_fd) {
    std::unique_lock<std::shared_mutex> lock(sub_mutex_);
    auto it = active_connections_.find(client_fd);
    if (it != active_connections_.end()) {
        const auto& subs = it->second->subscriptions();
        for (const auto& topic : subs) {
            auto sub_it = topic_subscribers_.find(topic);
            if (sub_it != topic_subscribers_.end()) {
                sub_it->second.erase(client_fd);
                if (sub_it->second.empty()) {
                    topic_subscribers_.erase(sub_it);
                }
            }
        }
        active_connections_.erase(it);
    }
}

bool Dispatcher::topic_matches(std::string_view pattern, std::string_view topic) const {
    if (pattern == "*" || pattern == topic) {
        return true;
    }
    if (!pattern.empty() && pattern.back() == '*') {
        std::string_view prefix = pattern.substr(0, pattern.size() - 1);
        if (topic.size() >= prefix.size() && topic.substr(0, prefix.size()) == prefix) {
            return true;
        }
    }
    return false;
}

void Dispatcher::handle_packet(const ConnectionPtr& conn, const Packet& packet) {
    increment_packets_in();
    increment_bytes_in(kHeaderSize + packet.payload.size());

    switch (packet.header.type) {
        case MessageType::Ping:
            handle_ping(conn, packet);
            break;
        case MessageType::Publish:
            handle_publish(conn, packet);
            break;
        case MessageType::Subscribe:
            handle_subscribe(conn, packet);
            break;
        case MessageType::Unsubscribe:
            handle_unsubscribe(conn, packet);
            break;
        case MessageType::QueryLatest:
            handle_query(conn, packet);
            break;
        case MessageType::MetricsReq:
            handle_metrics(conn, packet);
            break;
        default:
            LOG_WARN << "Unhandled packet type: " << static_cast<int>(packet.header.type)
                     << " from fd " << conn->fd();
            Packet err = make_error_packet(packet.header.seq_id, 400, "Unsupported message type");
            conn->send_packet(err);
            increment_packets_out();
            break;
    }
}

void Dispatcher::handle_ping(const ConnectionPtr& conn, const Packet& packet) {
    Packet pong = make_pong_packet(packet.header.seq_id);
    conn->send_packet(pong);
    increment_packets_out();
}

void Dispatcher::handle_publish(const ConnectionPtr& conn, const Packet& packet) {
    auto tp_opt = TelemetryPoint::decode(packet.payload.data(), packet.payload.size());
    if (!tp_opt) {
        LOG_WARN << "Failed to decode TelemetryPoint from fd " << conn->fd();
        Packet err = make_error_packet(packet.header.seq_id, 400, "Malformed telemetry payload");
        conn->send_packet(err);
        increment_packets_out();
        return;
    }

    const auto& pt = *tp_opt;
    ++total_telemetry_published_;

    // 1. Update In-Memory Telemetry State
    {
        std::unique_lock<std::shared_mutex> lock(telemetry_mutex_);
        telemetry_store_[pt.symbol].update(pt);
    }

    // 2. Route to matching subscribers
    std::vector<ConnectionPtr> target_conns;
    {
        std::shared_lock<std::shared_mutex> lock(sub_mutex_);
        for (const auto& [pattern, subscribers] : topic_subscribers_) {
            if (topic_matches(pattern, pt.topic)) {
                for (int fd : subscribers) {
                    auto it = active_connections_.find(fd);
                    if (it != active_connections_.end() && it->second->state() == ConnectionState::Connected) {
                        target_conns.push_back(it->second);
                    }
                }
            }
        }
    }

    // Broadcast dispatch packet to subscribers
    if (!target_conns.empty()) {
        Packet dispatch = make_dispatch_packet(packet.header.seq_id, pt);
        for (auto& target : target_conns) {
            target->send_packet(dispatch);
            increment_packets_out();
            increment_bytes_out(kHeaderSize + dispatch.payload.size());
        }
    }

    // Send ACK back to publisher
    Packet ack = make_ack_packet(packet.header.seq_id, "PUBLISHED");
    conn->send_packet(ack);
    increment_packets_out();
}

void Dispatcher::handle_subscribe(const ConnectionPtr& conn, const Packet& packet) {
    auto sub_opt = SubscribeRequest::decode(packet.payload.data(), packet.payload.size());
    if (!sub_opt) {
        Packet err = make_error_packet(packet.header.seq_id, 400, "Malformed subscribe payload");
        conn->send_packet(err);
        increment_packets_out();
        return;
    }

    const std::string& topic = sub_opt->topic_pattern;
    conn->add_subscription(topic);

    {
        std::unique_lock<std::shared_mutex> lock(sub_mutex_);
        topic_subscribers_[topic].insert(conn->fd());
    }

    LOG_INFO << "Client fd " << conn->fd() << " (" << conn->endpoint().to_string()
             << ") subscribed to: " << topic;

    Packet ack = make_ack_packet(packet.header.seq_id, "SUBSCRIBED");
    conn->send_packet(ack);
    increment_packets_out();
}

void Dispatcher::handle_unsubscribe(const ConnectionPtr& conn, const Packet& packet) {
    auto sub_opt = SubscribeRequest::decode(packet.payload.data(), packet.payload.size());
    if (!sub_opt) {
        Packet err = make_error_packet(packet.header.seq_id, 400, "Malformed unsubscribe payload");
        conn->send_packet(err);
        increment_packets_out();
        return;
    }

    const std::string& topic = sub_opt->topic_pattern;
    conn->remove_subscription(topic);

    {
        std::unique_lock<std::shared_mutex> lock(sub_mutex_);
        auto it = topic_subscribers_.find(topic);
        if (it != topic_subscribers_.end()) {
            it->second.erase(conn->fd());
            if (it->second.empty()) {
                topic_subscribers_.erase(it);
            }
        }
    }

    Packet ack = make_ack_packet(packet.header.seq_id, "UNSUBSCRIBED");
    conn->send_packet(ack);
    increment_packets_out();
}

void Dispatcher::handle_query(const ConnectionPtr& conn, const Packet& packet) {
    auto q_opt = QueryRequest::decode(packet.payload.data(), packet.payload.size());
    if (!q_opt) {
        Packet err = make_error_packet(packet.header.seq_id, 400, "Malformed query payload");
        conn->send_packet(err);
        increment_packets_out();
        return;
    }

    TelemetryPoint pt;
    bool found = false;

    {
        std::shared_lock<std::shared_mutex> lock(telemetry_mutex_);
        auto it = telemetry_store_.find(q_opt->symbol);
        if (it != telemetry_store_.end()) {
            found = true;
            pt = it->second.latest_point;
        }
    }

    Packet resp = make_query_response_packet(packet.header.seq_id, found, pt);
    conn->send_packet(resp);
    increment_packets_out();
}

void Dispatcher::handle_metrics(const ConnectionPtr& conn, const Packet& packet) {
    MetricsResponse metrics = get_metrics_snapshot();
    Packet resp = make_metrics_resp_packet(packet.header.seq_id, metrics);
    conn->send_packet(resp);
    increment_packets_out();
}

MetricsResponse Dispatcher::get_metrics_snapshot() const {
    MetricsResponse m;
    auto now = std::chrono::steady_clock::now();
    int64_t uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time_).count();
    m.uptime_sec = static_cast<uint64_t>(uptime > 0 ? uptime : 0);

    {
        std::shared_lock<std::shared_mutex> lock(sub_mutex_);
        m.active_connections = static_cast<uint32_t>(active_connections_.size());
        m.active_subscriptions = static_cast<uint32_t>(topic_subscribers_.size());
    }

    m.total_packets_received = total_packets_received_.load(std::memory_order_relaxed);
    m.total_packets_sent = total_packets_sent_.load(std::memory_order_relaxed);
    m.total_telemetry_published = total_telemetry_published_.load(std::memory_order_relaxed);
    m.total_bytes_in = total_bytes_in_.load(std::memory_order_relaxed);
    m.total_bytes_out = total_bytes_out_.load(std::memory_order_relaxed);
    m.worker_queue_depth = 0; // Populated by server if thread pool is used

    return m;
}

} // namespace nexus
