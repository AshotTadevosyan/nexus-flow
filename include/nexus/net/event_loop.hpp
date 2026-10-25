#pragma once

#include <vector>
#include <functional>
#include <chrono>
#include <cstdint>

namespace nexus {

enum class EventType : uint32_t {
    None   = 0x0,
    Read   = 0x1,
    Write  = 0x2,
    Error  = 0x4,
    Closed = 0x8,
    Signal = 0x10,
    Timer  = 0x20
};

inline EventType operator|(EventType a, EventType b) {
    return static_cast<EventType>(static_cast<uint32_t>(a) | static_cast<uint32_t>(b));
}

inline bool operator&(EventType a, EventType b) {
    return (static_cast<uint32_t>(a) & static_cast<uint32_t>(b)) != 0;
}

struct PolledEvent {
    int ident{0};
    EventType events{EventType::None};
    void* udata{nullptr};
};

class EventLoop {
public:
    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // FD Registration
    bool add_fd(int fd, EventType events, void* udata = nullptr);
    bool modify_fd(int fd, EventType events, void* udata = nullptr);
    bool remove_fd(int fd);

    // Signals & Timers
    bool add_signal(int signum);
    bool add_timer(int timer_id, int interval_ms);

    // Event Polling
    int poll(std::vector<PolledEvent>& active_events, int timeout_ms = 100);

    // Wakeup / Stop control
    void stop() { running_ = false; }
    bool is_running() const { return running_; }

private:
    int kq_fd_{-1};
    bool running_{true};
};

} // namespace nexus
