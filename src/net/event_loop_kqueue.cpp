#include "nexus/net/event_loop.hpp"
#include "nexus/common/logger.hpp"

#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#include <unistd.h>
#include <csignal>
#include <cerrno>
#include <cstring>

namespace nexus {

EventLoop::EventLoop() {
    kq_fd_ = ::kqueue();
    if (kq_fd_ < 0) {
        LOG_FATAL << "Failed to create kqueue: " << std::strerror(errno);
        throw std::system_error(errno, std::generic_category(), "kqueue() failed");
    }
}

EventLoop::~EventLoop() {
    if (kq_fd_ >= 0) {
        ::close(kq_fd_);
        kq_fd_ = -1;
    }
}

bool EventLoop::add_fd(int fd, EventType events, void* udata) {
    struct kevent evs[2];
    int n = 0;

    if (events & EventType::Read) {
        EV_SET(&evs[n++], fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, udata);
    }
    if (events & EventType::Write) {
        EV_SET(&evs[n++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, udata);
    }

    if (n > 0) {
        int res = ::kevent(kq_fd_, evs, n, nullptr, 0, nullptr);
        if (res < 0) {
            LOG_ERROR << "Failed to register kevent on fd " << fd << ": " << std::strerror(errno);
            return false;
        }
    }
    return true;
}

bool EventLoop::modify_fd(int fd, EventType events, void* udata) {
    struct kevent evs[2];
    int n = 0;

    if (events & EventType::Write) {
        EV_SET(&evs[n++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, udata);
    } else {
        EV_SET(&evs[n++], fd, EVFILT_WRITE, EV_DISABLE, 0, 0, udata);
    }

    if (events & EventType::Read) {
        EV_SET(&evs[n++], fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, udata);
    } else {
        EV_SET(&evs[n++], fd, EVFILT_READ, EV_DISABLE, 0, 0, udata);
    }

    int res = ::kevent(kq_fd_, evs, n, nullptr, 0, nullptr);
    return res >= 0;
}

bool EventLoop::remove_fd(int fd) {
    struct kevent evs[2];
    EV_SET(&evs[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    EV_SET(&evs[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    // Ignore error if filter wasn't registered
    ::kevent(kq_fd_, evs, 2, nullptr, 0, nullptr);
    return true;
}

bool EventLoop::add_signal(int signum) {
    // Ignore default signal behavior so kevent can capture it
    ::signal(signum, SIG_IGN);

    struct kevent ev;
    EV_SET(&ev, signum, EVFILT_SIGNAL, EV_ADD | EV_ENABLE, 0, 0, nullptr);
    int res = ::kevent(kq_fd_, &ev, 1, nullptr, 0, nullptr);
    if (res < 0) {
        LOG_ERROR << "Failed to register signal " << signum << " in kqueue: " << std::strerror(errno);
        return false;
    }
    return true;
}

bool EventLoop::add_timer(int timer_id, int interval_ms) {
    struct kevent ev;
    EV_SET(&ev, timer_id, EVFILT_TIMER, EV_ADD | EV_ENABLE, 0, interval_ms, nullptr);
    int res = ::kevent(kq_fd_, &ev, 1, nullptr, 0, nullptr);
    if (res < 0) {
        LOG_ERROR << "Failed to register timer in kqueue: " << std::strerror(errno);
        return false;
    }
    return true;
}

int EventLoop::poll(std::vector<PolledEvent>& active_events, int timeout_ms) {
    active_events.clear();

    struct timespec ts;
    struct timespec* pts = nullptr;
    if (timeout_ms >= 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000;
        pts = &ts;
    }

    constexpr int kMaxEvents = 256;
    struct kevent event_list[kMaxEvents];

    int num_events = ::kevent(kq_fd_, nullptr, 0, event_list, kMaxEvents, pts);
    if (num_events < 0) {
        if (errno == EINTR) {
            return 0;
        }
        LOG_ERROR << "kevent() poll error: " << std::strerror(errno);
        return -1;
    }

    for (int i = 0; i < num_events; ++i) {
        const auto& ev = event_list[i];
        PolledEvent pe;
        pe.ident = static_cast<int>(ev.ident);
        pe.udata = ev.udata;

        if (ev.flags & EV_ERROR) {
            pe.events = pe.events | EventType::Error;
        }
        if (ev.flags & EV_EOF) {
            pe.events = pe.events | EventType::Closed;
        }

        if (ev.filter == EVFILT_READ) {
            pe.events = pe.events | EventType::Read;
        } else if (ev.filter == EVFILT_WRITE) {
            pe.events = pe.events | EventType::Write;
        } else if (ev.filter == EVFILT_SIGNAL) {
            pe.events = pe.events | EventType::Signal;
        } else if (ev.filter == EVFILT_TIMER) {
            pe.events = pe.events | EventType::Timer;
        }

        active_events.push_back(pe);
    }

    return num_events;
}

} // namespace nexus
