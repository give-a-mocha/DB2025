#pragma once
#include <string>

#ifndef HASH_H_
#define HASH_H_

using uint64 = unsigned long long;

constexpr uint64 getHashCode(const std::string &str, uint64 hash = 5381) {
    for (char c : str) {
        hash = ((hash << 5) + hash) + c;  // hash * 33 + c
    }
    return hash;
}

#endif