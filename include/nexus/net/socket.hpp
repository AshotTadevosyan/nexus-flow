#pragma once

#include <string>
#include <string_view>
#include <system_error>
#include <optional>
#include <cstdint>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

namespace nexus {

struct Endpoint {
    std::string ip;
    uint16_t port{0};

    std::string to_string() const {
        return ip + ":" + std::to_string(port);
    }

    static Endpoint from_sockaddr(const sockaddr_in& addr);
    static std::optional<Endpoint> resolve(std::string_view host, uint16_t port);
};

class Socket {
public:
    static constexpr int kInvalidFd = -1;

    Socket();
    explicit Socket(int fd);
    ~Socket();

    // Move-only semantics (RAII)
    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    // Factory methods
    static Socket create_tcp();

    // Core operations
    void bind(const Endpoint& endpoint);
    void listen(int backlog = SOMAXCONN);
    std::optional<std::pair<Socket, Endpoint>> accept();
    bool connect(const Endpoint& endpoint, int timeout_ms = 3000);
    void close();

    // Socket tuning
    void set_nonblocking(bool enable = true);
    void set_reuse_addr(bool enable = true);
    void set_reuse_port(bool enable = true);
    void set_tcp_nodelay(bool enable = true);
    void set_nosigpipe(bool enable = true);
    void set_keepalive(bool enable = true);
    void set_send_buffer_size(int size_bytes);
    void set_recv_buffer_size(int size_bytes);

    // Queries
    int fd() const noexcept { return fd_; }
    bool is_valid() const noexcept { return fd_ != kInvalidFd; }
    Endpoint local_endpoint() const;
    Endpoint peer_endpoint() const;

    int release() noexcept {
        int temp = fd_;
        fd_ = kInvalidFd;
        return temp;
    }

private:
    int fd_{kInvalidFd};
};

} // namespace nexus
