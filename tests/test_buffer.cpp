#include "nexus/common/buffer.hpp"
#include <cassert>
#include <iostream>
#include <string_view>

void test_basic_primitives() {
    nexus::ByteBuffer buf;
    buf.append_uint8(0x42);
    buf.append_uint16(0x1234);
    buf.append_uint32(0xDEADBEEF);
    buf.append_uint64(0x0102030405060708ULL);

    assert(buf.readable_bytes() == 1 + 2 + 4 + 8);

    assert(buf.peek_uint8(0) == 0x42);
    assert(buf.peek_uint16(1) == 0x1234);
    assert(buf.peek_uint32(3) == 0xDEADBEEF);
    assert(buf.peek_uint64(7) == 0x0102030405060708ULL);

    assert(buf.read_uint8() == 0x42);
    assert(buf.read_uint16() == 0x1234);
    assert(buf.read_uint32() == 0xDEADBEEF);
    assert(buf.read_uint64() == 0x0102030405060708ULL);

    assert(buf.readable_bytes() == 0);
    std::cout << "[PASS] test_basic_primitives\n";
}

void test_compaction() {
    nexus::ByteBuffer buf(128);
    // Write 2500 bytes to exceed compaction threshold
    std::vector<uint8_t> dummy(2500, 0xAB);
    buf.append(dummy.data(), dummy.size());

    // Consume 2200 bytes
    buf.consume(2200);
    assert(buf.readable_bytes() == 300);

    // Append another 1000 bytes; this should trigger compaction
    std::vector<uint8_t> dummy2(1000, 0xCD);
    buf.append(dummy2.data(), dummy2.size());

    assert(buf.readable_bytes() == 1300);

    // Verify initial 300 bytes are still intact
    for (size_t i = 0; i < 300; ++i) {
        assert(buf.read_uint8() == 0xAB);
    }
    // Verify following 1000 bytes
    for (size_t i = 0; i < 1000; ++i) {
        assert(buf.read_uint8() == 0xCD);
    }
    assert(buf.readable_bytes() == 0);
    std::cout << "[PASS] test_compaction\n";
}

int main() {
    std::cout << "Running ByteBuffer tests...\n";
    test_basic_primitives();
    test_compaction();
    std::cout << "All ByteBuffer tests PASSED!\n";
    return 0;
}
