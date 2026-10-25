#include "nexus/common/buffer.hpp"

namespace nexus {

ByteBuffer::ByteBuffer(size_t initial_capacity)
    : buffer_(std::max(size_t(64), initial_capacity)) {}

void ByteBuffer::append(const void* data, size_t len) {
    if (len == 0) return;
    ensure_writable(len);
    std::memcpy(buffer_.data() + write_pos_, data, len);
    write_pos_ += len;
}

void ByteBuffer::append(std::string_view sv) {
    append(sv.data(), sv.size());
}

void ByteBuffer::append_uint8(uint8_t val) {
    ensure_writable(1);
    buffer_[write_pos_++] = val;
}

void ByteBuffer::append_uint16(uint16_t val) {
    ensure_writable(2);
    buffer_[write_pos_++] = static_cast<uint8_t>((val >> 8) & 0xFF);
    buffer_[write_pos_++] = static_cast<uint8_t>(val & 0xFF);
}

void ByteBuffer::append_uint32(uint32_t val) {
    ensure_writable(4);
    buffer_[write_pos_++] = static_cast<uint8_t>((val >> 24) & 0xFF);
    buffer_[write_pos_++] = static_cast<uint8_t>((val >> 16) & 0xFF);
    buffer_[write_pos_++] = static_cast<uint8_t>((val >> 8) & 0xFF);
    buffer_[write_pos_++] = static_cast<uint8_t>(val & 0xFF);
}

void ByteBuffer::append_uint64(uint64_t val) {
    ensure_writable(8);
    for (int i = 7; i >= 0; --i) {
        buffer_[write_pos_++] = static_cast<uint8_t>((val >> (i * 8)) & 0xFF);
    }
}

void ByteBuffer::commit_write(size_t bytes_written) {
    if (write_pos_ + bytes_written > buffer_.size()) {
        throw std::out_of_range("commit_write exceeds allocated buffer capacity");
    }
    write_pos_ += bytes_written;
}

void ByteBuffer::ensure_writable(size_t min_writable) {
    if (writable_bytes() >= min_writable) {
        return;
    }

    // Check if compaction gives us enough room
    if (read_pos_ + writable_bytes() >= min_writable) {
        maybe_compact();
        if (writable_bytes() >= min_writable) {
            return;
        }
    }

    // Otherwise reallocate with growth factor 1.5x - 2x
    size_t required = write_pos_ + min_writable;
    size_t new_cap = std::max(buffer_.size() * 2, required);
    buffer_.resize(new_cap);
}

void ByteBuffer::consume(size_t len) {
    if (len > readable_bytes()) {
        throw std::out_of_range("Attempted to consume more bytes than available in buffer");
    }
    read_pos_ += len;
    if (read_pos_ == write_pos_) {
        read_pos_ = 0;
        write_pos_ = 0;
    } else {
        maybe_compact();
    }
}

void ByteBuffer::clear() noexcept {
    read_pos_ = 0;
    write_pos_ = 0;
}

void ByteBuffer::maybe_compact() {
    // Compact if read_pos_ has progressed past halfway or > 2KB
    if (read_pos_ > 0 && (read_pos_ > buffer_.size() / 2 || read_pos_ > 2048)) {
        size_t readable = readable_bytes();
        if (readable > 0) {
            std::memmove(buffer_.data(), buffer_.data() + read_pos_, readable);
        }
        read_pos_ = 0;
        write_pos_ = readable;
    }
}

uint8_t ByteBuffer::peek_uint8(size_t offset) const {
    if (readable_bytes() < offset + 1) {
        throw std::out_of_range("Not enough bytes to peek uint8");
    }
    return buffer_[read_pos_ + offset];
}

uint16_t ByteBuffer::peek_uint16(size_t offset) const {
    if (readable_bytes() < offset + 2) {
        throw std::out_of_range("Not enough bytes to peek uint16");
    }
    const uint8_t* p = buffer_.data() + read_pos_ + offset;
    return static_cast<uint16_t>((static_cast<uint16_t>(p[0]) << 8) | static_cast<uint16_t>(p[1]));
}

uint32_t ByteBuffer::peek_uint32(size_t offset) const {
    if (readable_bytes() < offset + 4) {
        throw std::out_of_range("Not enough bytes to peek uint32");
    }
    const uint8_t* p = buffer_.data() + read_pos_ + offset;
    return (static_cast<uint32_t>(p[0]) << 24) |
           (static_cast<uint32_t>(p[1]) << 16) |
           (static_cast<uint32_t>(p[2]) << 8)  |
           static_cast<uint32_t>(p[3]);
}

uint64_t ByteBuffer::peek_uint64(size_t offset) const {
    if (readable_bytes() < offset + 8) {
        throw std::out_of_range("Not enough bytes to peek uint64");
    }
    const uint8_t* p = buffer_.data() + read_pos_ + offset;
    uint64_t val = 0;
    for (int i = 0; i < 8; ++i) {
        val = (val << 8) | static_cast<uint64_t>(p[i]);
    }
    return val;
}

uint8_t ByteBuffer::read_uint8() {
    uint8_t val = peek_uint8(0);
    consume(1);
    return val;
}

uint16_t ByteBuffer::read_uint16() {
    uint16_t val = peek_uint16(0);
    consume(2);
    return val;
}

uint32_t ByteBuffer::read_uint32() {
    uint32_t val = peek_uint32(0);
    consume(4);
    return val;
}

uint64_t ByteBuffer::read_uint64() {
    uint64_t val = peek_uint64(0);
    consume(8);
    return val;
}

} // namespace nexus
