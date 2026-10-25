cmake_minimum_required(VERSION 3.20)
project(NexusFlow VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Optimization and Warning flags
add_compile_options(
    -Wall
    -Wextra
    -Wpedantic
    -Wshadow
    -Wnon-virtual-dtor
    -Wold-style-cast
    -Wcast-align
    -Wunused
    -Woverloaded-virtual
    -Wconversion
    -Wsign-conversion
    -O3
)

include_directories(include)

# Core library
set(NEXUS_SOURCES
    src/common/logger.cpp
    src/common/crc32.cpp
    src/common/buffer.cpp
    src/net/socket.cpp
    src/net/connection.cpp
    src/net/event_loop_kqueue.cpp
    src/net/tcp_server.cpp
    src/protocol/packet.cpp
    src/protocol/dispatcher.cpp
)

add_library(nexus_core STATIC ${NEXUS_SOURCES})
target_include_directories(nexus_core PUBLIC include)

# Executables
add_executable(nexus_server src/server_main.cpp)
target_link_libraries(nexus_server PRIVATE nexus_core pthread)

add_executable(nexus_cli src/client_main.cpp)
target_link_libraries(nexus_cli PRIVATE nexus_core pthread)

add_executable(nexus_benchmark src/benchmark_main.cpp)
target_link_libraries(nexus_benchmark PRIVATE nexus_core pthread)

# Tests
enable_testing()

add_executable(test_buffer tests/test_buffer.cpp)
target_link_libraries(test_buffer PRIVATE nexus_core pthread)
add_test(NAME test_buffer COMMAND test_buffer)

add_executable(test_protocol tests/test_protocol.cpp)
target_link_libraries(test_protocol PRIVATE nexus_core pthread)
add_test(NAME test_protocol COMMAND test_protocol)

add_executable(test_integration tests/test_integration.cpp)
target_link_libraries(test_integration PRIVATE nexus_core pthread)
add_test(NAME test_integration COMMAND test_integration)
