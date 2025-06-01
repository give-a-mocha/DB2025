#pragma once

#ifndef STACKSTRING_HPP
#define STACKSTRING_HPP
#include <cstddef>

template <size_t SIZE = 1024, bool external_space = false, bool is_throw = false>
class StackString {
    static_assert(SIZE > 0);

   private:
    using BufferType = std::conditional_t<external_space, char*, char[SIZE]>;
    BufferType buf_;
    // char buf_[SIZE];
    size_t size_;

   public:
    ~StackString() = default;
    StackString(const StackString&) = delete;
    StackString(StackString&&) = delete;
    StackString& operator=(const StackString&) = delete;
    StackString& operator=(StackString&&) = delete;

    constexpr StackString() : size_(0) {}
    constexpr StackString(char* buffer) : buf_(buffer), size_(0) { static_assert(external_space); }

    constexpr void operator+=(const char* str) {
        size_t len = strlen(str);
        if constexpr (is_throw) {
            if (len + size_ > SIZE - 1) {
                throw std::runtime_error("(StackString::operator+=) StackString overflow");
            }
        }
        strcpy(buf_ + size_, str);
        size_ += len;
    }

    constexpr void operator+=(const std::string& str) {
        if constexpr (is_throw) {
            if (str.size() + size_ > SIZE - 1) {
                throw std::runtime_error("(StackString::operator+=) StackString overflow");
            }
        }
        memcpy(buf_ + size_, str.c_str(), str.size());
        size_ += str.size();
    }

    constexpr void append(size_t size, char ch) {
        if constexpr (is_throw) {
            if (size + size_ > SIZE - 1) {
                throw std::runtime_error("(StackString::operator+=) StackString overflow");
            }
        }
        memset(buf_ + size_, ch, size);
        size_ += size;
    }

    constexpr const char* c_str() {
        buf_[size_] = '\0';
        return buf_;
    }

    constexpr size_t size() const { return size_; }

    constexpr void clear() { size_ = 0; }

    constexpr bool empty() const { return size_ == 0; }

    constexpr std::string toString() const { return std::string(buf_, size_); }
};

#endif