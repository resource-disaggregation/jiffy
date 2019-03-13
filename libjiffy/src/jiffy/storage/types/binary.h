#ifndef JIFFY_BINARY_H
#define JIFFY_BINARY_H

#include <cstdint>
#include <string>
#include "jiffy/storage/block_memory_allocator.h"

namespace jiffy {
namespace storage {

typedef block_memory_allocator<uint8_t> binary_allocator;
typedef std::char_traits<uint8_t> binary_traits;
typedef std::basic_string<uint8_t, binary_traits, binary_allocator> binary;

std::string to_string(const binary &bin);

bool operator<(const binary& a, const std::string& b);

bool operator<=(const binary& a, const std::string& b);

bool operator>(const binary& a, const std::string& b);

bool operator>=(const binary& a, const std::string& b);

bool operator<(const std::string& a, const binary& b);

bool operator<=(const std::string& a, const binary& b);

bool operator>(const std::string& a, const binary& b);

bool operator>=(const std::string& a, const binary& b);

bool operator==(const binary& a, const std::string& b);

bool operator==(const std::string& a, const binary& b);

}
}

#endif //JIFFY_BINARY_H
