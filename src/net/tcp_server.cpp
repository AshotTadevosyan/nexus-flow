#include "nexus/net/tcp_server.hpp"
#include "nexus/common/logger.hpp"

#include <csignal>
#include <vector>

namespace nexus {

TcpServer::TcpServer(const ServerConfig& config)
    : config_(config),
      event_loop_(std::make_unique<EventLoop>()),
      thread_pool_(std::make_unique<ThreadPool>(config.worker_threads)),
      dispatcher_(std::make_unique<Dispatcher>()) {}

TcpServer::~TcpServer() {
    stop();
}

void TcpServer::start() {
    if (running_.exchange(true)) {
        LOG_WARN << "Server already running";
        return;
    }

    init_listener();

    LOG_INFO << "============================================================";
    LOG_INFO << "   NexusFlow Telemetry & Market Data Stream Broker v1.0    ";
    LOG_INFO << "   Listening on: " << config_.host << ":" << config_.port;
    LOG_INFO << "   Worker Pool: " << config_.worker_threads << " threads";
    LOG_INFO << "   Event Reactor: macOS BSD kqueue engine";
    LOG_INFO << "============================================================";

    // Register system signals for graceful shutdown
    event_loop_->add_signal(SIGINT);
    event_loop_->add_signal(SIGTERM);

    // Register 5-second periodic maintenance & reaping timer (timer ID 1)
    event_loop_->add_timer(1, 5000);

    run_loop();
}

void TcpServer::stop() {
    if (!running_.exchange(false)) {
        return;
    }

    LOG_INFO << "Initiating graceful shutdown sequence...";

    // 1. Close listening socket so no new connections arrive
    listen_socket_.close();

    // 2. Flush pending writes and close active client connections
    for (auto& [fd, conn] : connections_) {
        conn->flush_write_buffer();
        conn->mark_closed();
        event_loop_->remove_fd(fd);
    }
    connections_.clear();

    // 3. Stop event loop and thread pool
    if (event_loop_) {
        event_loop_->stop();
    }
    if (thread_pool_) {
        thread_pool_->shutdown();
    }

    LOG_INFO << "Server stopped successfully. All resources released.";
}

void TcpServer::init_listener() {
    listen_socket_ = Socket::create_tcp();
    listen_socket_.set_reuse_addr(true);
    if (config_.reuse_port) {
        listen_socket_.set_reuse_port(true);
    }
    listen_socket_.set_nosigpipe(true);
    listen_socket_.set_nonblocking(true);

    Endpoint ep;
    ep.ip = config_.host;
    ep.port = config_.port;

    listen_socket_.bind(ep);
    listen_socket_.listen(config_.backlog);

    // Register listen socket with kqueue for READ events
    event_loop_->add_fd(listen_socket_.fd(), EventType::Read, nullptr);
}

void TcpServer::run_loop() {
    std::vector<PolledEvent> events;
    events.reserve(256);

    while (running_.load(std::memory_order_relaxed)) {
        int n = event_loop_->poll(events, 100);
        if (n < 0) {
            break;
        }

        for (const auto& ev : events) {
            if (ev.events & EventType::Signal) {
                LOG_INFO << "Received termination signal (" << ev.ident << "). Shutting down...";
                stop();
                return;
            }

            if (ev.events & EventType::Timer) {
                handle_periodic_timer();
                continue;
            }

            // Check if event is on the listen socket
            if (ev.ident == listen_socket_.fd()) {
                if (ev.events & EventType::Read) {
                    handle_accept();
                }
                continue;
            }

            // Otherwise event is on an active client connection
            int client_fd = ev.ident;
            auto it = connections_.find(client_fd);
            if (it == connections_.end()) {
                continue;
            }

            if (ev.events & (EventType::Error | EventType::Closed)) {
                handle_close(client_fd, "Socket error or EOF detected");
                continue;
            }

            if (ev.events & EventType::Read) {
                handle_read(client_fd);
            }

            // Connection could have been closed during handle_read
            if (connections_.find(client_fd) != connections_.end()) {
                if (ev.events & EventType::Write) {
                    handle_write(client_fd);
                }
            }
        }
    }
}

void TcpServer::handle_accept() {
    while (true) {
        auto accept_res = listen_socket_.accept();
        if (!accept_res) {
            break; // No more incoming connections to accept
        }

        auto& [client_sock, remote_ep] = *accept_res;
        int client_fd = client_sock.release();

        Socket sock(client_fd);
        sock.set_nonblocking(true);
        sock.set_nosigpipe(true);
        if (config_.tcp_nodelay) {
            sock.set_tcp_nodelay(true);
        }
        sock.set_keepalive(true);
        sock.release(); // Keep fd open

        auto conn = std::make_shared<Connection>(client_fd, remote_ep);
        conn->set_write_state_callback([this](int fd, bool has_pending) {
            if (has_pending) {
                event_loop_->modify_fd(fd, EventType::Read | EventType::Write, nullptr);
            } else {
                event_loop_->modify_fd(fd, EventType::Read, nullptr);
            }
        });
        connections_[client_fd] = conn;

        // Register with Dispatcher and EventLoop
        dispatcher_->register_connection(conn);
        event_loop_->add_fd(client_fd, EventType::Read, nullptr);

        LOG_INFO << "New client connected: " << remote_ep.to_string()
                 << " (fd: " << client_fd << ", total: " << connections_.size() << ")";
    }
}

void TcpServer::handle_read(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) return;
    auto conn = it->second;

    ssize_t n = conn->read_from_socket();
    if (n < 0 || (n == 0 && conn->state() == ConnectionState::Closing)) {
        handle_close(fd, "Client disconnected (read EOF or error)");
        return;
    }

    // Process all complete packets in buffer
    ByteBuffer& in_buf = conn->in_buffer();
    while (in_buf.readable_bytes() >= kHeaderSize) {
        auto hdr_opt = PacketHeader::decode(in_buf, 0);
        if (!hdr_opt) {
            handle_close(fd, "Invalid packet header magic - protocol violation");
            return;
        }

        const auto& hdr = *hdr_opt;
        if (hdr.payload_len > kMaxPayloadSize) {
            handle_close(fd, "Payload size exceeds maximum allowed threshold");
            return;
        }

        // Check if full packet has arrived
        size_t total_packet_len = kHeaderSize + hdr.payload_len;
        if (in_buf.readable_bytes() < total_packet_len) {
            // Need more data from socket
            break;
        }

        // Consume header
        in_buf.consume(kHeaderSize);

        // Read payload
        std::vector<uint8_t> payload_bytes;
        if (hdr.payload_len > 0) {
            payload_bytes.resize(hdr.payload_len);
            std::memcpy(payload_bytes.data(), in_buf.read_ptr(), hdr.payload_len);
            in_buf.consume(hdr.payload_len);
        }

        // Validate CRC32
        if (hdr.payload_len > 0) {
            uint32_t computed_crc = Crc32::calculate(payload_bytes.data(), payload_bytes.size());
            if (computed_crc != hdr.payload_crc) {
                LOG_WARN << "CRC32 checksum mismatch from fd " << fd 
                         << " (expected: " << hdr.payload_crc << ", actual: " << computed_crc << ")";
                Packet err = make_error_packet(hdr.seq_id, 400, "CRC32 checksum mismatch");
                conn->send_packet(err);
                continue;
            }
        }

        Packet pkt;
        pkt.header = hdr;
        pkt.payload = std::move(payload_bytes);

        // Dispatch packet to business logic
        dispatcher_->handle_packet(conn, pkt);
    }

    // Try flushing any immediate responses
    if (conn->has_pending_writes()) {
        conn->flush_write_buffer();
        // If still has pending data, enable write notifications in kqueue
        if (conn->has_pending_writes()) {
            event_loop_->modify_fd(fd, EventType::Read | EventType::Write, nullptr);
        } else {
            event_loop_->modify_fd(fd, EventType::Read, nullptr);
        }
    }

    if (conn->state() == ConnectionState::Closing && !conn->has_pending_writes()) {
        handle_close(fd, "Orderly close requested");
    }
}

void TcpServer::handle_write(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) return;
    auto conn = it->second;

    conn->flush_write_buffer();

    if (!conn->has_pending_writes()) {
        // Disable write event to prevent busy loop
        event_loop_->modify_fd(fd, EventType::Read, nullptr);

        if (conn->state() == ConnectionState::Closing) {
            handle_close(fd, "All pending outgoing bytes flushed; closing");
        }
    }
}

void TcpServer::handle_close(int fd, std::string_view reason) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) return;

    auto conn = it->second;
    LOG_INFO << "Closing client " << conn->endpoint().to_string() 
             << " (fd: " << fd << "): " << reason;

    event_loop_->remove_fd(fd);
    dispatcher_->unregister_connection(fd);
    conn->mark_closed();
    connections_.erase(it);
}

void TcpServer::handle_periodic_timer() {
    reap_idle_connections();

    // Log diagnostic heartbeat
    MetricsResponse m = dispatcher_->get_metrics_snapshot();
    LOG_INFO << "[Heartbeat] Uptime: " << m.uptime_sec << "s"
             << " | Active Conns: " << m.active_connections
             << " | Active Subs: " << m.active_subscriptions
             << " | In: " << m.total_packets_received << " pkts (" << (m.total_bytes_in / 1024) << " KB)"
             << " | Out: " << m.total_packets_sent << " pkts (" << (m.total_bytes_out / 1024) << " KB)"
             << " | Published: " << m.total_telemetry_published;
}

void TcpServer::reap_idle_connections() {
    if (config_.idle_timeout_sec <= 0) return;

    auto now = std::chrono::steady_clock::now();
    std::vector<int> idle_fds;

    for (const auto& [fd, conn] : connections_) {
        auto idle_duration = std::chrono::duration_cast<std::chrono::seconds>(now - conn->last_active()).count();
        if (idle_duration >= config_.idle_timeout_sec) {
            idle_fds.push_back(fd);
        }
    }

    for (int fd : idle_fds) {
        handle_close(fd, "Idle connection timed out");
    }
}

} // namespace nexus
