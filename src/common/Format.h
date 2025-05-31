#pragma once
#ifndef FORMAT_H_
#define FORMAT_H_

#include <algorithm>
#include <cassert>
#include <sstream>
#include <string>

namespace util {

template <typename... Args>
std::string format(std::string fmt, const Args &&...args);

namespace detail {
inline void format_helper(std::stringstream &ss, std::string &fmt) {
    ss << fmt;
    fmt.clear();
}

template <typename T, typename... Args>
void format_helper(std::stringstream &ss, std::string &fmt, const T &&val, const Args &&...args) {
    size_t start = fmt.find('{');
    size_t end = fmt.find('}', start);

    if (start == std::string::npos || end == std::string::npos) {
        ss << fmt;
        fmt.clear();
        return;
    }

    ss << fmt.substr(0, start);
    ss << val;
    fmt = fmt.substr(end + 1);
    if (sizeof...(args) != 0) format_helper(ss, fmt, std::forward<Args>(args)...);
}
}  // namespace detail

template <typename... Args>
std::string format(std::string fmt, const Args &&...args) {
    if (sizeof...(args) == 0) {
        return fmt;
    }

    std::stringstream ss;
    detail::format_helper(ss, fmt, std::forward<Args>(args)...);
    if (!fmt.empty()) {
        ss << fmt;
    }
    return ss.str();
}

}  // namespace util

#endif