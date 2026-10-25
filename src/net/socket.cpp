#include "nexus/net/socket.hpp"
#include "nexus/common/logger.hpp"

#include <fcntl.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <cstring>
#include <cerrno>

namespace nexus {

Endpoint Endpoint::from_sockaddr(const sockaddr_in& addr) {
    Endpoint ep;
    char ip_str[INET_ADDRSTRLEN];
    if (inet_ntop(AF_INET, &(addr.sin_addr), ip_str, INET_ADDRSTRLEN)) {
        ep.ip = ip_str;
    }
    ep.port = ntohs(addr.sin_port);
    return ep;
}

std::optional<Endpoint> Endpoint::resolve(std::string_view host, uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* res = nullptr;
    std::string host_str(host);
    int status = getaddrinfo(host_str.c_str(), nullptr, &hints, &res);
    if (status != 0 || !res) {
        return std::nullopt;
    }

    auto* ipv4 = reinterpret_cast<sockaddr_in*>(res->ai_addr);
    Endpoint ep = Endpoint::from_sockaddr(*ipv4);
    ep.port = port;
    freeaddrinfo(res);
    return ep;
}

Socket::Socket() : fd_(kInvalidFd) {}

Socket::Socket(int fd) : fd_(fd) {}

Socket::~Socket() {
    close();
}

Socket::Socket(Socket&& other) noexcept : fd_(other.fd_) {
    other.fd_ = kInvalidFd;
}

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = other.fd_;
        other.fd_ = kInvalidFd;
    }
    return *this;
}

Socket Socket::create_tcp() {
    int s = ::socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) {
        throw std::system_error(errno, std::generic_category(), "Failed to create socket");
    }
    return Socket(s);
}

void Socket::close() {
    if (fd_ != kInvalidFd) {
        ::close(fd_);
        fd_ = kInvalidFd;
    }
}

void Socket::bind(const Endpoint& endpoint) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(endpoint.port);
    if (endpoint.ip.empty() || endpoint.ip == "0.0.0.0") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (inet_pton(AF_INET, endpoint.ip.c_str(), &addr.sin_addr) <= 0) {
            throw std::system_error(errno, std::generic_category(), "Invalid IP address: " + endpoint.ip);
        }
    }

    if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        throw std::system_error(errno, std::generic_category(), "Failed to bind socket to " + endpoint.to_string());
    }
}

void Socket::listen(int backlog) {
    if (::listen(fd_, backlog) < 0) {
        throw std::system_error(errno, std::generic_category(), "Failed to listen on socket");
    }
}

std::optional<std::pair<Socket, Endpoint>> Socket::accept() {
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    
    int client_fd = ::accept(fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
    if (client_fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            return std::nullopt;
        }
        throw std::system_error(errno, std::generic_category(), "accept() error");
    }

    Endpoint ep = Endpoint::from_sockaddr(client_addr);
    return std::make_pair(Socket(client_fd), ep);
}

bool Socket::connect(const Endpoint& endpoint, int timeout_ms) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(endpoint.port);
    if (inet_pton(AF_INET, endpoint.ip.c_str(), &addr.sin_addr) <= 0) {
        return false;
    }

    set_nonblocking(true);
    set_nosigpipe(true);

    int res = ::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (res == 0) {
        return true;
    }

    if (errno != EINPROGRESS) {
        LOG_ERROR << "connect() immediately failed: " << std::strerror(errno);
        return false;
    }

    // Wait for connection to finish using poll
    pollfd pfd{};
    pfd.fd = fd_;
    pfd.events = POLLOUT;

    int poll_res = ::poll(&pfd, 1, timeout_ms);
    if (poll_res <= 0) {
        LOG_ERROR << "connect() poll timed out or error: " << std::strerror(errno);
        return false; // Timeout or error
    }

    int so_error = 0;
    socklen_t len = sizeof(so_error);
    if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &so_error, &len) < 0 || so_error != 0) {
        LOG_ERROR << "connect() so_error: " << std::strerror(so_error);
        return false;
    }

    return true;
}

void Socket::set_nonblocking(bool enable) {
    int flags = fcntl(fd_, F_GETFL, 0);
    if (flags < 0) return;
    if (enable) {
        flags |= O_NONBLOCK;
    } else {
        flags &= ~O_NONBLOCK;
    }
    fcntl(fd_, F_SETFL, flags);
}

void Socket::set_reuse_addr(bool enable) {
    int opt = enable ? 1 : 0;
    setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
}

void Socket::set_reuse_port(bool enable) {
#ifdef SO_REUSEPORT
    int opt = enable ? 1 : 0;
    setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif
}

void Socket::set_tcp_nodelay(bool enable) {
    int opt = enable ? 1 : 0;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
}

void Socket::set_nosigpipe(bool enable) {
#ifdef SO_NOSIGPIPE
    int opt = enable ? 1 : 0;
    setsockopt(fd_, SOL_SOCKET, SO_NOSIGPIPE, &opt, sizeof(opt));
#endif
}

void Socket::set_keepalive(bool enable) {
    int opt = enable ? 1 : 0;
    setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt));
}

void Socket::set_send_buffer_size(int size_bytes) {
    setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &size_bytes, sizeof(size_bytes));
}

void Socket::set_recv_buffer_size(int size_bytes) {
    setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &size_bytes, sizeof(size_bytes));
}

Endpoint Socket::local_endpoint() const {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    if (getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len) == 0) {
        return Endpoint::from_sockaddr(addr);
    }
    return {};
}

Endpoint Socket::peer_endpoint() const {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    if (getpeername(fd_, reinterpret_cast<sockaddr*>(&addr), &len) == 0) {
        return Endpoint::from_sockaddr(addr);
    }
    return {};
}

} // namespace nexus
