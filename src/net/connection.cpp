#include "nexus/net/connection.hpp"
#include "nexus/common/logger.hpp"

#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>

namespace nexus {

Connection::Connection(int fd, const Endpoint& remote_ep)
    : fd_(fd),
      remote_endpoint_(remote_ep),
      last_active_(std::chrono::steady_clock::now()) {}

Connection::~Connection() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

void Connection::add_subscription(const std::string& topic) {
    subscriptions_.insert(topic);
}

void Connection::remove_subscription(const std::string& topic) {
    subscriptions_.erase(topic);
}

bool Connection::is_subscribed(const std::string& topic) const {
    // Exact match or prefix wildcard match (e.g. "sensor.*" matches "sensor.temp")
    if (subscriptions_.count(topic)) {
        return true;
    }
    for (const auto& sub : subscriptions_) {
        if (!sub.empty() && sub.back() == '*') {
            std::string_view prefix(sub.data(), sub.size() - 1);
            if (topic.rfind(prefix, 0) == 0) {
                return true;
            }
        }
    }
    return false;
}

ssize_t Connection::read_from_socket() {
    ssize_t total_read = 0;
    while (true) {
        in_buffer_.ensure_writable(8192);
        size_t writable = in_buffer_.writable_bytes();

        ssize_t n = ::recv(fd_, in_buffer_.write_ptr(), writable, 0);
        if (n > 0) {
            in_buffer_.commit_write(static_cast<size_t>(n));
            total_read += n;
            touch();
            // If we filled our buffer chunk, try reading more
            if (static_cast<size_t>(n) < writable) {
                break;
            }
        } else if (n == 0) {
            // Peer performed orderly shutdown
            mark_closing();
            return total_read > 0 ? total_read : 0;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Done reading all currently available bytes
                break;
            } else if (errno == EINTR) {
                continue;
            } else {
                LOG_WARN << "Socket recv error on " << remote_endpoint_.to_string() 
                         << " (fd " << fd_ << "): " << std::strerror(errno);
                mark_closing();
                return -1;
            }
        }
    }
    return total_read;
}

ssize_t Connection::flush_write_buffer() {
    ssize_t total_written = 0;
    while (out_buffer_.readable_bytes() > 0) {
        ssize_t n = ::send(fd_, out_buffer_.read_ptr(), out_buffer_.readable_bytes(), 0);
        if (n > 0) {
            out_buffer_.consume(static_cast<size_t>(n));
            total_written += n;
            touch();
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Socket buffer full; wait for reactor write event
                break;
            } else if (errno == EINTR) {
                continue;
            } else {
                LOG_WARN << "Socket send error on " << remote_endpoint_.to_string()
                         << " (fd " << fd_ << "): " << std::strerror(errno);
                mark_closing();
                return -1;
            }
        }
    }
    return total_written;
}

bool Connection::send_packet(const Packet& packet) {
    if (state_ != ConnectionState::Connected) {
        return false;
    }

    size_t packet_size = kHeaderSize + packet.payload.size();
    if (out_buffer_.readable_bytes() + packet_size > kMaxOutgoingBufferSize) {
        LOG_WARN << "Backpressure limit reached on fd " << fd_ << " (" << remote_endpoint_.to_string()
                 << "), buffer: " << out_buffer_.readable_bytes() << " bytes. Closing slow consumer.";
        mark_closing();
        return false;
    }

    packet.serialize(out_buffer_);
    touch();

    // Immediate zero-latency direct flush
    flush_write_buffer();

    if (write_state_cb_) {
        write_state_cb_(fd_, has_pending_writes());
    }

    return true;
}

bool Connection::send_raw(const void* data, size_t len) {
    if (state_ != ConnectionState::Connected || len == 0) {
        return false;
    }

    if (out_buffer_.readable_bytes() + len > kMaxOutgoingBufferSize) {
        LOG_WARN << "Backpressure limit reached on fd " << fd_ << " (" << remote_endpoint_.to_string()
                 << "), buffer: " << out_buffer_.readable_bytes() << " bytes. Closing slow consumer.";
        mark_closing();
        return false;
    }

    out_buffer_.append(data, len);
    touch();

    flush_write_buffer();

    if (write_state_cb_) {
        write_state_cb_(fd_, has_pending_writes());
    }

    return true;
}

} // namespace nexus
