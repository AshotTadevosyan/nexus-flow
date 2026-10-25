#include "nexus/net/tcp_server.hpp"
#include "nexus/common/logger.hpp"

#include <iostream>
#include <cstdlib>
#include <string>

int main(int argc, char* argv[]) {
    nexus::ServerConfig config;
    config.host = "127.0.0.1";
    config.port = 9090;
    config.worker_threads = 4;
    config.idle_timeout_sec = 60;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            config.port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (arg == "--host" && i + 1 < argc) {
            config.host = argv[++i];
        } else if (arg == "--threads" && i + 1 < argc) {
            config.worker_threads = static_cast<size_t>(std::atoi(argv[++i]));
        } else if (arg == "--timeout" && i + 1 < argc) {
            config.idle_timeout_sec = std::atoi(argv[++i]);
        } else if (arg == "--debug") {
            nexus::Logger::instance().set_level(nexus::LogLevel::Debug);
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: nexus_server [options]\n"
                      << "  --host <ip>       Bind address (default: 127.0.0.1)\n"
                      << "  --port <port>     Bind port (default: 9090)\n"
                      << "  --threads <n>     Worker pool thread count (default: 4)\n"
                      << "  --timeout <sec>   Idle connection reaper timeout (default: 60s)\n"
                      << "  --debug           Enable debug log level\n"
                      << "  --help, -h        Show this help message\n";
            return 0;
        }
    }

    try {
        nexus::TcpServer server(config);
        server.start();
    } catch (const std::exception& ex) {
        LOG_FATAL << "Fatal server exception: " << ex.what();
        return 1;
    }

    return 0;
}
