#include "nexus/common/logger.hpp"
#include <ctime>
#include <cstring>

namespace nexus {

Logger& Logger::instance() {
    static Logger inst;
    return inst;
}

static const char* level_to_string(LogLevel level) {
    switch (level) {
        case LogLevel::Debug: return "DEBUG";
        case LogLevel::Info:  return "INFO ";
        case LogLevel::Warn:  return "WARN ";
        case LogLevel::Error: return "ERROR";
        case LogLevel::Fatal: return "FATAL";
        default: return "UNKNOWN";
    }
}

static const char* level_to_color(LogLevel level) {
    switch (level) {
        case LogLevel::Debug: return "\033[36m"; // Cyan
        case LogLevel::Info:  return "\033[32m"; // Green
        case LogLevel::Warn:  return "\033[33m"; // Yellow
        case LogLevel::Error: return "\033[31m"; // Red
        case LogLevel::Fatal: return "\033[35m"; // Magenta
        default: return "\033[0m";
    }
}

void Logger::log(LogLevel level, const char* file, int line, const std::string& message) {
    // Strip directories from file name for cleaner logs
    const char* basename = file;
    const char* slash = std::strrchr(file, '/');
    if (slash) {
        basename = slash + 1;
    }

    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm_buf;
    localtime_r(&t, &tm_buf);

    std::lock_guard<std::mutex> lock(log_mutex_);
    
    // Format: [YYYY-MM-DD HH:MM:SS.mmm] [LEVEL] [ThreadID] [file:line] Message
    char time_str[64];
    std::strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &tm_buf);

    std::ostream& out = (level >= LogLevel::Error) ? std::cerr : std::cout;

    out << "[" << time_str << "." << std::setfill('0') << std::setw(3) << ms.count() << "] "
        << level_to_color(level) << "[" << level_to_string(level) << "]\033[0m "
        << "[T:" << std::this_thread::get_id() << "] "
        << "\033[90m[" << basename << ":" << line << "]\033[0m "
        << message << "\n";
    out.flush();
}

} // namespace nexus
