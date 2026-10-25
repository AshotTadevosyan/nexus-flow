#include "nexus/common/crc32.hpp"

namespace nexus {

static constexpr uint32_t generate_crc32_table_entry(uint32_t i) {
    uint32_t crc = i;
    for (int j = 0; j < 8; ++j) {
        if (crc & 1) {
            crc = (crc >> 1) ^ 0xEDB88320u;
        } else {
            crc >>= 1;
        }
    }
    return crc;
}

// Compile-time generated table for maximum runtime efficiency
struct Crc32Table {
    uint32_t data[256];
    constexpr Crc32Table() : data{} {
        for (uint32_t i = 0; i < 256; ++i) {
            data[i] = generate_crc32_table_entry(i);
        }
    }
};

static constexpr Crc32Table kTable;

uint32_t Crc32::calculate(const void* data, size_t length) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < length; ++i) {
        uint8_t table_index = static_cast<uint8_t>((crc ^ bytes[i]) & 0xFF);
        crc = (crc >> 8) ^ kTable.data[table_index];
    }
    return crc ^ 0xFFFFFFFFu;
}

uint32_t Crc32::calculate(std::string_view sv) {
    return calculate(sv.data(), sv.size());
}

} // namespace nexus
