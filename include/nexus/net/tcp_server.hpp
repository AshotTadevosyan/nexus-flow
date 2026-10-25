#pragma once

#include "nexus/net/socket.hpp"
#include "nexus/net/event_loop.hpp"
#include "nexus/net/connection.hpp"
#include "nexus/common/thread_pool.hpp"
#include "nexus/protocol/dispatcher.hpp"

#include <unordered_map>
#include <memory>
#include <atomic>
#include <chrono>

namespace nexus {

struct ServerConfig {
    std::string host{"0.0.0.0"};
    uint16_t port{8080};
    int backlog{SOMAXCONN};
    size_t worker_threads{4};
    int idle_timeout_sec{60};
    bool tcp_nodelay{true};
    bool reuse_port{true};
};

class TcpServer {
public:
    explicit TcpServer(const ServerConfig& config = {});
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void start();
    void stop();

    bool is_running() const { return running_.load(std::memory_order_relaxed); }
    const ServerConfig& config() const { return config_; }
    Dispatcher& dispatcher() { return *dispatcher_; }

private:
    void init_listener();
    void run_loop();
    void handle_accept();
    void handle_read(int fd);
    void handle_write(int fd);
    void handle_close(int fd, std::string_view reason);
    void handle_periodic_timer();
    void reap_idle_connections();

    ServerConfig config_;
    std::atomic<bool> running_{false};
    Socket listen_socket_;
    std::unique_ptr<EventLoop> event_loop_;
    std::unique_ptr<ThreadPool> thread_pool_;
    std::unique_ptr<Dispatcher> dispatcher_;

    std::unordered_map<int, ConnectionPtr> connections_;
};

} // namespace nexus
