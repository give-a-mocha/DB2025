#pragma once
#include <string>

#ifndef HASH_H_
#define HASH_H_

namespace util {



using uint64 = unsigned long long;

inline uint64 getHashCode(std::string_view str, uint64 hash = 5381) {
    for (char c : str) {
        hash = ((hash << 5) + hash) + c;  // hash * 33 + c
    }
    return hash;
}


}

#endif