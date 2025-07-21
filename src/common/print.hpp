#pragma once
#ifndef PRINT_HPP_
#define PRINT_HPP_

#include <string>
#include <string_view>
#include <iostream>

#include "common/Format.h"

// #define PrintEnable

#ifdef PrintEnable

template <typename... Args>
void print(std::string_view fmt_str, Args &&...args) {
    std::cout << util::format(std::string(fmt_str), std::forward<Args>(args)...);
}

template <typename... Args>
void println(std::string_view fmt_str, Args &&...args) {
    std::cout << util::format(std::string(fmt_str), std::forward<Args>(args)...) << std::endl;
}

template <typename... Args>
void INFO(std::string_view fmt_str, Args &&...args) {
    std::clog << "\033[0m\033[1;32m" << util::format(std::string(fmt_str), std::forward<Args>(args)...) << "\033[0m"
              << std::endl;
}

template <typename... Args>
void WARN(std::string_view fmt_str, Args &&...args) {
    std::clog << "\033[0m\033[1;33m" << util::format(std::string(fmt_str), std::forward<Args>(args)...) << "\033[0m"
              << std::endl;
}

template <typename... Args>
void ERROR(std::string_view fmt_str, Args &&...args) {
    std::clog << "\033[0m\033[1;31m" << util::format(std::string(fmt_str), std::forward<Args>(args)...) << "\033[0m"
              << std::endl;
}

#else

template <typename... Args>
void print(std::string_view fmt_str, Args &&...args) {}

template <typename... Args>
void println(std::string_view fmt_str, Args &&...args) {}

template <typename... Args>
void INFO(std::string_view fmt_str, Args &&...args) {}

template <typename... Args>
void WARN(std::string_view fmt_str, Args &&...args) {}

template <typename... Args>
void ERROR(std::string_view fmt_str, Args &&...args) {}

#endif

#endif
