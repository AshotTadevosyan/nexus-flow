# NexusFlow — Production-Grade Event-Driven Stream Broker in Modern C++20

**NexusFlow** is a high-throughput, sub-millisecond, event-driven socket server and real-time stream broker engineered in modern C++20. 

Designed for **real-life mission-critical domains**—such as high-frequency trading (HFT) ticker distribution, financial order routing, autonomous vehicle fleet telemetry, and industrial IoT monitoring—NexusFlow delivers deterministic latency, memory safety, and high-concurrency throughput.

---

## 🚀 Key Features

* **BSD `kqueue` Event Reactor**: Native OS kernel event multiplexing on macOS/BSD, delivering true non-blocking I/O without the C10K thread-per-connection bottleneck.
* **Deterministic Wire Protocol**: 20-byte binary header (`Magic`, `Version`, `MsgType`, `Flags`, `SeqId`, `PayloadLen`, `PayloadCRC32`) with hardware-accelerated IEEE 802.3 CRC32 verification to prevent frame corruption and bit rot.
* **Pub/Sub Engine with Wildcard Topic Routing**: Dynamic topic subscriptions with exact match and prefix wildcard support (e.g., `market.crypto.*`, `sensors.factory1.>`).
* **Real-Time In-Memory State Cache**: High-performance thread-safe state store (`std::shared_mutex`) retaining the latest value, moving statistics (min, max, average, count), and instant query snapshots.
* **Adaptive Backpressure & Slow-Consumer Defense**: High-watermark outgoing buffers with kernel socket drain detection. Slow consumers unable to keep up are dropped before memory exhaustion occurs.
* **Zero-Spurious Reactor Write Engine**: Dynamically arms `EVFILT_WRITE` only when socket output buffers are saturated, preventing CPU busy-spinning.
* **Boss-Worker Thread Pool**: Decouples network I/O from compute-intensive tasks, aggregations, and business logic.
* **Heartbeat & Zombie Reaper**: Background timer reaper detecting idle connections and releasing file descriptors.
* **Clean Signal Handling & Graceful Teardown**: Intercepts `SIGINT` / `SIGTERM` directly inside `kqueue` to drain active sockets, flush buffers, and cleanly stop threads.
* **Zero-Warning Strict Compilation**: Compiles under `-Wall -Wextra -Wpedantic -Wshadow -Wconversion -O3` with modern C++20 standard.

---

## 🏗️ Architecture

```
                      +-----------------------------------+
                      |   Client TCP Connections (N)      |
                      +-----------------+-----------------+
                                        | (Non-blocking TCP)
                                        v
                      +-----------------+-----------------+
                      |     Event Reactor (kqueue)        |
                      |  - EVFILT_READ / EVFILT_WRITE     |
                      |  - EVFILT_SIGNAL / EVFILT_TIMER   |
                      +-----------------+-----------------+
                                        |
                 +----------------------+----------------------+
                 |                                             |
                 v                                             v
      +---------------------+                       +---------------------+
      |   Connection Pool   |                       |    Dispatcher Core  |
      | - In/Out ByteBuffers|                       | - Topic Subscriptions
      | - State & Watermarks|                       | - Telemetry Cache   |
      +----------+----------+                       +----------+----------+
                 |                                             |
                 +----------------------+----------------------+
                                        |
                                        v
                      +-----------------+-----------------+
                      |     ThreadPool Workers (M)        |
                      | - Complex metric aggregations     |
                      | - Analytical alerts & persistence |
                      +-----------------------------------+
```

---

## 📦 Binary Wire Protocol Specification

Every packet transmitted across the wire begins with a strict 20-byte aligned binary header:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      Magic (0x4E455853 "NEXS")                |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|    Version    |   MsgType     |             Flags             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Sequence ID                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Payload Length                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      Payload CRC32 Checksum                   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Payload Data ...                       |
|                          (Variable)                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Supported Message Types (`MsgType`)

| Opcode | Name | Description |
|---|---|---|
| `0x01` | `PING` | Keepalive heartbeat ping |
| `0x02` | `PONG` | Heartbeat response |
| `0x03` | `PUBLISH` | Telemetry/market tick publication |
| `0x04` | `SUBSCRIBE` | Register subscription pattern |
| `0x05` | `UNSUBSCRIBE` | Remove subscription pattern |
| `0x06` | `DISPATCH_EVENT` | Broadcasted event delivered to matching subscribers |
| `0x07` | `QUERY_LATEST` | Synchronous state snapshot request for a symbol |
| `0x08` | `QUERY_RESPONSE` | State snapshot response with values and timestamps |
| `0x09` | `METRICS_REQ` | Broker diagnostic telemetry request |
| `0x0A` | `METRICS_RESP` | Broker runtime throughput & connection statistics |
| `0x0B` | `ACK` | Confirmation receipt |
| `0xFF` | `ERROR` | Malformed packet / internal error code |

---

## 🛠️ Building the Project

### Prerequisites
* Clang++ with C++20 support (Apple Clang 15+ or LLVM Clang 16+)
* CMake 3.20+ or Make

### Quick Compilation
```bash
# Build all targets (libnexus_core, server, client, benchmark, tests)
make
```

### Running Automated Test Suite
```bash
make test
```
*Output:*
```
Test project build
    Start 1: test_buffer
1/3 Test #1: test_buffer ......................   Passed    0.51 sec
    Start 2: test_protocol
2/3 Test #2: test_protocol ....................   Passed    0.49 sec
    Start 3: test_integration
3/3 Test #3: test_integration .................   Passed    0.12 sec

100% tests passed, 0 tests failed out of 3
```

---

## 💻 Usage

### 1. Starting the Broker Daemon
```bash
./build/nexus_server --port 9090 --threads 4 --timeout 60
```
Command options:
* `--host <ip>`: Bind address (default: `127.0.0.1`)
* `--port <port>`: Listening port (default: `9090`)
* `--threads <n>`: Worker pool thread count (default: `4`)
* `--timeout <sec>`: Idle connection reaper threshold (default: `60`)
* `--debug`: Enable verbose debug logging

### 2. Interactive CLI Client
Open a terminal and run:
```bash
./build/nexus_cli 127.0.0.1 9090
```

Available interactive commands:
```text
> help
Available Commands:
  pub <topic> <symbol> <val> [tags]   Publish telemetry data point
  sub <topic_pattern>                 Subscribe to topic (e.g. market.*, sensors.temp)
  unsub <topic_pattern>               Unsubscribe from topic
  query <symbol>                      Query latest snapshot for symbol
  stats                               Request broker runtime metrics
  ping                                Send heartbeat ping
  exit, quit                          Disconnect and exit
```

#### Example Session:
```text
> sub market.crypto.*
[ACK] seq=1 status=SUBSCRIBED

> pub market.crypto.BTC BTC 68500.50 exchange=coinbase
[ACK] seq=2 status=PUBLISHED
[EVENT] Topic: market.crypto.BTC | Symbol: BTC | Value: 68500.5 | Tags: exchange=coinbase | Time: 1729873000ms

> query BTC
[QUERY RESULT] Symbol: BTC | Latest Value: 68500.5 | Topic: market.crypto.BTC | Tags: exchange=coinbase

> stats
--- NexusFlow Server Metrics ---
  Uptime:                142 seconds
  Active Connections:    3
  Active Subscriptions:  1
  Packets In / Out:      1240 / 1239
  Telemetry Published:   620
  Bytes In / Out:        84 KB / 96 KB
--------------------------------
```

---

## ⚡ High-Concurrency Benchmark Suite

NexusFlow includes a multi-threaded load generator and microsecond latency profiler (`nexus_benchmark`).

```bash
# Benchmark 20 concurrent publishers, 50,000 total messages, with 4 background subscribers:
./build/nexus_benchmark --clients 20 --messages 2500 --subscribers 4 --port 9090
```

### Live Benchmark Results:
```text
========================================================
          NexusFlow Load & Latency Benchmark            
========================================================
  Target:               127.0.0.1:9090
  Publisher Clients:    20
  Messages per Client:  2500
  Total Messages:       50000
  Subscribers:          4
========================================================

================ Benchmark Results ================
  Execution Time:       0.74 seconds
  Messages Completed:   50000 (Errors: 0)
  Throughput:           67,819.07 msgs/sec
  Network Bandwidth:    5.31 MB/sec
  Subscriber Received:  200,000 broadcasts
---------------- Latency Profile ------------------
  Min Latency:          24 us
  Average Latency:      275.05 us
  P50  (Median):        242 us
  P90:                  457 us
  P95:                  498 us
  P99:                  707 us
  Max Latency:          1901 us (1.9 ms)
===================================================
```

---

## 📂 Project Organization

```
nexus_broker/
├── CMakeLists.txt                 # Modern CMake configuration with strict flags
├── Makefile                       # Top-level make interface
├── README.md                      # Complete system documentation
├── include/
│   └── nexus/
│       ├── common/
│       │   ├── types.hpp          # Status codes, time utilities
│       │   ├── logger.hpp         # Thread-safe structured colored logger
│       │   ├── crc32.hpp          # Fast IEEE 802.3 CRC32 checksum engine
│       │   ├── buffer.hpp         # Self-compacting dynamic contiguous byte buffer
│       │   └── thread_pool.hpp    # Exception-safe worker thread pool
│       ├── net/
│       │   ├── socket.hpp         # RAII UniqueFd, socket options & endpoint resolution
│       │   ├── event_loop.hpp     # Event reactor abstraction (kqueue)
│       │   ├── connection.hpp     # Per-client state, buffering & backpressure
│       │   └── tcp_server.hpp     # Core reactor TCP server daemon
│       └── protocol/
│           ├── packet.hpp         # Wire protocol framing & domain payloads
│           └── dispatcher.hpp     # Routing engine & in-memory state store
├── src/
│   ├── common/                    # Implementation of buffer, logger, crc32
│   ├── net/                       # Implementation of socket, connection, kqueue, server
│   ├── protocol/                  # Implementation of packet framing and dispatcher
│   ├── server_main.cpp            # Server daemon binary
│   ├── client_main.cpp            # Interactive CLI client
│   └── benchmark_main.cpp         # High-concurrency benchmark generator
└── tests/
    ├── test_buffer.cpp            # ByteBuffer unit tests (primitives, compaction)
    ├── test_protocol.cpp          # Wire framing, corruption rejection & CRC tests
    └── test_integration.cpp       # End-to-end integration test
```

---

## 🛡️ Production Engineering Highlights

1. **Memory Safety & Zero Leaks**: Strict RAII encapsulation across file descriptors, heap buffers, mutex guards, and threads.
2. **Buffer Compaction**: Dynamic shifting of unread bytes prevents unbounded allocation drift without frequent reallocations.
3. **Slow-Client Protection**: Connections exceeding the 8MB out-buffer watermark are disconnected gracefully, isolating other tenants from memory pressure.
4. **Signal Cleanliness**: Signals (`SIGINT`, `SIGTERM`) are trapped via kqueue without the classic async-signal-safety hazards of signal handlers.
5. **TCP Optimization**: Automatically configures `TCP_NODELAY`, `SO_REUSEADDR`, `SO_REUSEPORT`, `SO_KEEPALIVE`, and `SO_NOSIGPIPE`.
