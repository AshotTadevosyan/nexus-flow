#pragma once

#include <cstdint>
#include <cstddef>
#include <string_view>

namespace nexus {

class Crc32 {
public:
    static uint32_t calculate(const void* data, size_t length);
    static uint32_t calculate(std::string_view sv);

private:
    static const uint32_t table[256];
};

} // namespace nexus
