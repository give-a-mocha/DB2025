#pragma once

#ifndef CONSTEXPR_STRING_H_
#define CONSTEXPR_STRING_H_

#include <cstddef>
#include <string_view>

template <std::size_t N>
class ConstexprString {
   private:
    char data_[N + 1] = {};

   public:
    // 默认构造函数
    constexpr ConstexprString() = default;

    // 从字符串字面量构造
    constexpr ConstexprString(const char (&str)[N + 1]) {
        for (std::size_t i = 0; i < N; ++i) {
            data_[i] = str[i];
        }
    }

    // 从字符数组构造
    // constexpr ConstexprString(const char* str) {
    //     for (std::size_t i = 0; i < N; ++i) {
    //         data_[i] = str[i];
    //     }
    // }

    constexpr ConstexprString(const char c) {
        for (std::size_t i = 0; i < N; ++i) {
            data_[i] = c;
        }
    }

    // 获取字符
    constexpr char& operator[](std::size_t index) { return data_[index]; }
    constexpr const char& operator[](std::size_t index) const { return data_[index]; }

    // 获取长度
    constexpr std::size_t size() const { return N; }
    constexpr std::size_t length() const { return N; }
    constexpr bool empty() const { return N == 0; }

    // 获取数据指针
    constexpr const char* data() const { return data_; }
    constexpr const char* c_str() const { return data_; }

    // 字符串连接
    template <std::size_t M>
    constexpr auto operator+(const ConstexprString<M>& other) const {
        ConstexprString<N + M> result;
        std::size_t i = 0;

        // 复制当前字符串
        for (std::size_t j = 0; j < N; ++j, ++i) {
            result.data_[i] = data_[j];
        }

        // 复制另一个字符串
        for (std::size_t j = 0; j < M; ++j, ++i) {
            result.data_[i] = other.data_[j];
        }

        return result;
    }

    // 比较操作
    template <std::size_t M>
    constexpr bool operator==(const ConstexprString<M>& other) const {
        if constexpr (N != M) return false;
        for (std::size_t i = 0; i < N; ++i) {
            if (data_[i] != other.data_[i]) return false;
        }
        return true;
    }

    template <std::size_t M>
    constexpr bool operator!=(const ConstexprString<M>& other) const {
        return !(*this == other);
    }

    // 子字符串
    template <std::size_t start, std::size_t len, std::size_t return_len = (start + len > N) ? N - start : len>
    constexpr ConstexprString<return_len> substr() const {
        ConstexprString<return_len> result;

        for (std::size_t i = 0; i < return_len; ++i) {
            result[i] = data_[start + i];
        }
        return result;
    }

    // 转换为 string_view
    constexpr std::string_view to_string_view() const { return std::string_view(data_, N); }
};

// 推导指引
template <std::size_t N>
ConstexprString(const char (&)[N]) -> ConstexprString<N - 1>;

// 辅助函数创建编译期字符串
template <std::size_t N>
constexpr auto make_constexpr_string(const char (&str)[N]) {
    return ConstexprString<N - 1>(str);
}

#endif