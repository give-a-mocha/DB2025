#pragma once
#ifndef PRINT_HPP_
#define PRINT_HPP_

#include <string>
#include <string_view>

#include "common/Format.h"

template <typename... Args>
void print(std::string_view fmt_str, const Args &&...args) {
    std::cout << util::format(std::string(fmt_str), std::forward<Args>(args)...);
}

template <typename... Args>
void println(std::string_view fmt_str, const Args &&...args) {
    std::cout << util::format(std::string(fmt_str), std::forward<Args>(args)...) << std::endl;
}

template <typename... Args>
void LOG(std::string_view fmt_str, const Args &&...args) {
    std::clog << "\033[0m\033[1;32m" << util::format(std::string(fmt_str), std::forward<Args>(args)...) << "\033[0m"
              << std::endl;
}

template <typename... Args>
void WARN(std::string_view fmt_str, const Args &&...args) {
    std::clog << "\033[0m\033[1;33m" << util::format(std::string(fmt_str), std::forward<Args>(args)...) << "\033[0m"
              << std::endl;
}

template <typename... Args>
void ERROR(std::string_view fmt_str, const Args &&...args) {
    std::clog << "\033[0m\033[1;31m" << util::format(std::string(fmt_str), std::forward<Args>(args)...) << "\033[0m"
              << std::endl;
}

#endif