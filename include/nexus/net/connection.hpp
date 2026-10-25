#pragma once

#include "nexus/net/socket.hpp"
#include "nexus/common/buffer.hpp"
#include "nexus/protocol/packet.hpp"

#include <memory>
#include <unordered_set>
#include <chrono>

namespace nexus {

enum class ConnectionState {
    Connected,
    Closing,
    Closed
};

class Connection {
public:
    static constexpr size_t kMaxOutgoingBufferSize = 8 * 1024 * 1024; // 8 MB backpressure limit

    Connection(int fd, const Endpoint& remote_ep);
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    int fd() const noexcept { return fd_; }
    const Endpoint& endpoint() const noexcept { return remote_endpoint_; }
    ConnectionState state() const noexcept { return state_; }

    void mark_closing() noexcept { state_ = ConnectionState::Closing; }
    void mark_closed() noexcept { state_ = ConnectionState::Closed; }
    bool is_closed() const noexcept { return state_ == ConnectionState::Closed; }

    // Activity tracking for idle reaping
    void touch() noexcept {
        last_active_ = std::chrono::steady_clock::now();
    }
    std::chrono::steady_clock::time_point last_active() const noexcept {
        return last_active_;
    }

    // Subscriptions
    void add_subscription(const std::string& topic);
    void remove_subscription(const std::string& topic);
    bool is_subscribed(const std::string& topic) const;
    const std::unordered_set<std::string>& subscriptions() const { return subscriptions_; }

    // Buffer access
    ByteBuffer& in_buffer() { return in_buffer_; }
    ByteBuffer& out_buffer() { return out_buffer_; }
    bool has_pending_writes() const { return out_buffer_.readable_bytes() > 0; }

    // I/O Operations (called by reactor)
    // Returns bytes read, 0 on EOF, or -1 on EAGAIN/error
    ssize_t read_from_socket();
    
    // Returns bytes written, or -1 on EAGAIN/error
    ssize_t flush_write_buffer();

    // Send packet: queues into out_buffer with backpressure check
    bool send_packet(const Packet& packet);
    bool send_raw(const void* data, size_t len);

    using WriteStateCallback = std::function<void(int fd, bool has_pending)>;
    void set_write_state_callback(WriteStateCallback cb) {
        write_state_cb_ = std::move(cb);
    }

private:
    int fd_;
    Endpoint remote_endpoint_;
    ConnectionState state_{ConnectionState::Connected};
    ByteBuffer in_buffer_{8192};
    ByteBuffer out_buffer_{8192};
    std::chrono::steady_clock::time_point last_active_;
    std::unordered_set<std::string> subscriptions_;
    WriteStateCallback write_state_cb_;
};

using ConnectionPtr = std::shared_ptr<Connection>;

} // namespace nexus
