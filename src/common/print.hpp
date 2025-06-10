#pragma once
#ifndef PRINT_HPP_
#define PRINT_HPP_

#include <string>
#include <string_view>

#include "common/Format.h"

template <typename T>
concept _can_print_ = requires(T &&a) {
    { std::cout << std::forward<T>(a) };
};

template <_can_print_... Args>
void print(std::string_view fmt_str, Args &&...args) {
    std::cout << util::format(std::string(fmt_str), std::forward<Args>(args)...);
}

template <_can_print_... Args>
void println(std::string_view fmt_str, Args &&...args) {
    std::cout << util::format(std::string(fmt_str), std::forward<Args>(args)...) << std::endl;
}

template <_can_print_... Args>
void INFO(std::string_view fmt_str, Args &&...args) {
    std::clog << "\033[0m\033[1;32m" << util::format(std::string(fmt_str), std::forward<Args>(args)...) << "\033[0m"
              << std::endl;
}

template <_can_print_... Args>
void WARN(std::string_view fmt_str, Args &&...args) {
    std::clog << "\033[0m\033[1;33m" << util::format(std::string(fmt_str), std::forward<Args>(args)...) << "\033[0m"
              << std::endl;
}

template <_can_print_... Args>
void ERROR(std::string_view fmt_str, Args &&...args) {
    std::clog << "\033[0m\033[1;31m" << util::format(std::string(fmt_str), std::forward<Args>(args)...) << "\033[0m"
              << std::endl;
}

#endif
