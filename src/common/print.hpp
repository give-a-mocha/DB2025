#pragma once
#ifndef PRINT_HPP
#define PRINT_HPP

#include <string>

#include "common/Format.h"

template <typename... Args>
void print(const char *fmt_str, const Args &&...args) {
    std::cout << util::Format(fmt_str, std::forward<Args &&>(args));
}
template <>
void print(const char *fmt_str) {
    std::cout << fmt_str;
}

template <typename... Args>
void println(const char *fmt_str, Args &&...args) {
    std::cout << std::vformat(fmt_str, std::make_format_args(args...)) << std::endl;
}
template <>
void println(const char *fmt_str) {
    std::cout << fmt_str << std::endl;
}

template <typename... Args>
void LOG(const char *fmt_str, Args &&...args) {
    std::clog << std::vformat(fmt_str, std::make_format_args(args...)) << std::endl;
}
template <>
void LOG(const char *fmt_str) {
    std::clog << fmt_str << std::endl;
}

template <typename... Args>
void ERROR(const char *fmt_str, Args &&...args) {
    std::cerr << std::vformat(fmt_str, std::make_format_args(args...)) << std::endl;
}
template <>
void ERROR(const char *fmt_str) {
    std::cerr << fmt_str << std::endl;
}

#endif