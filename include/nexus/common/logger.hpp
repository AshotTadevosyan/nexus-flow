#pragma once

#include <string>
#include <sstream>
#include <iostream>
#include <mutex>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <thread>

namespace nexus {

enum class LogLevel : int {
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
    Fatal = 4
};

class Logger {
public:
    static Logger& instance();

    void set_level(LogLevel level) {
        current_level_.store(level, std::memory_order_relaxed);
    }

    LogLevel get_level() const {
        return current_level_.load(std::memory_order_relaxed);
    }

    bool is_enabled(LogLevel level) const {
        return static_cast<int>(level) >= static_cast<int>(current_level_.load(std::memory_order_relaxed));
    }

    void log(LogLevel level, const char* file, int line, const std::string& message);

private:
    Logger() = default;
    ~Logger() = default;
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    std::atomic<LogLevel> current_level_{LogLevel::Info};
    std::mutex log_mutex_;
};

class LogMessage {
public:
    LogMessage(LogLevel level, const char* file, int line)
        : level_(level), file_(file), line_(line) {}

    ~LogMessage() {
        Logger::instance().log(level_, file_, line_, stream_.str());
    }

    template <typename T>
    LogMessage& operator<<(const T& val) {
        stream_ << val;
        return *this;
    }

private:
    LogLevel level_;
    const char* file_;
    int line_;
    std::ostringstream stream_;
};

} // namespace nexus

#define NEXUS_LOG(lvl) \
    if (nexus::Logger::instance().is_enabled(lvl)) \
        nexus::LogMessage(lvl, __FILE__, __LINE__)

#define LOG_DEBUG NEXUS_LOG(nexus::LogLevel::Debug)
#define LOG_INFO  NEXUS_LOG(nexus::LogLevel::Info)
#define LOG_WARN  NEXUS_LOG(nexus::LogLevel::Warn)
#define LOG_ERROR NEXUS_LOG(nexus::LogLevel::Error)
#define LOG_FATAL NEXUS_LOG(nexus::LogLevel::Fatal)
