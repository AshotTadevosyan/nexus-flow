#pragma once

#include <vector>
#include <cstdint>
#include <cstddef>
#include <string_view>
#include <cstring>
#include <algorithm>
#include <stdexcept>

namespace nexus {

class ByteBuffer {
public:
    explicit ByteBuffer(size_t initial_capacity = 4096);

    // Readable bytes count
    size_t readable_bytes() const noexcept {
        return write_pos_ - read_pos_;
    }

    // Writable space without reallocating
    size_t writable_bytes() const noexcept {
        return buffer_.size() - write_pos_;
    }

    // Direct pointer access
    const uint8_t* read_ptr() const noexcept {
        return buffer_.data() + read_pos_;
    }

    uint8_t* write_ptr() noexcept {
        return buffer_.data() + write_pos_;
    }

    // Appending data
    void append(const void* data, size_t len);
    void append(std::string_view sv);
    void append_uint8(uint8_t val);
    void append_uint16(uint16_t val);
    void append_uint32(uint32_t val);
    void append_uint64(uint64_t val);

    // Commit bytes written externally directly into write_ptr()
    void commit_write(size_t bytes_written);

    // Ensure writable space available
    void ensure_writable(size_t min_writable);

    // Consuming data
    void consume(size_t len);
    void clear() noexcept;

    // Peeking primitive values without consuming
    uint8_t peek_uint8(size_t offset = 0) const;
    uint16_t peek_uint16(size_t offset = 0) const;
    uint32_t peek_uint32(size_t offset = 0) const;
    uint64_t peek_uint64(size_t offset = 0) const;

    // Reading primitives and advancing read_pos_
    uint8_t read_uint8();
    uint16_t read_uint16();
    uint32_t read_uint32();
    uint64_t read_uint64();

    // Slices
    std::string_view string_view() const noexcept {
        return {reinterpret_cast<const char*>(read_ptr()), readable_bytes()};
    }

    // Compaction: shifts remaining readable bytes to index 0 if read_pos is large
    void maybe_compact();

    // Capacity & state inspection
    size_t capacity() const noexcept { return buffer_.size(); }
    size_t read_pos() const noexcept { return read_pos_; }
    size_t write_pos() const noexcept { return write_pos_; }

private:
    std::vector<uint8_t> buffer_;
    size_t read_pos_{0};
    size_t write_pos_{0};
};

} // namespace nexus
