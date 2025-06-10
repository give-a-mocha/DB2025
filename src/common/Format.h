#pragma once
#ifndef FORMAT_H_
#define FORMAT_H_

#ifdef protected
#undef protected
#define need_protected
#endif

#ifdef private
#undef private
#define need_private
#endif

#include <algorithm>
#include <cassert>
#include <sstream>
#include <string>
#include <utility>
#include <iostream>

namespace util {

template <typename T>
concept _can_print_ = requires(T &&a) {
    { std::cout << a };
};

template <_can_print_... Args>
std::string format(std::string fmt, Args &&...args);

namespace detail {
inline void format_helper(std::stringstream &ss, std::string &fmt) {
    ss << fmt;
    fmt.clear();
}

template <_can_print_ T, _can_print_... Args>
void format_helper(std::stringstream &ss, std::string &fmt, T &&val, Args &&...args) {
    size_t start = fmt.find('{');
    size_t end = fmt.find('}', start);

    if (start == std::string::npos || end == std::string::npos) {
        ss << fmt;
        fmt.clear();
        return;
    }

    ss << fmt.substr(0, start);
    ss << std::forward<T>(val);
    fmt = fmt.substr(end + 1);
    if (sizeof...(args) != 0) format_helper(ss, fmt, std::forward<Args>(args)...);
}
}  // namespace detail

template <_can_print_... Args>
std::string format(std::string fmt, Args &&...args) {
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

#ifdef need_protected
#define protected public
#undef need_protected
#endif

#ifdef need_private
#define private public
#undef need_private
#endif

#endif