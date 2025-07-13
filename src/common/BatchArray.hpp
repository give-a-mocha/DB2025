#pragma once

#ifndef BATCH_ARRAY_HPP_
#define BATCH_ARRAY_HPP_

#include <cstddef>
#include <stdexcept>

template <typename T, size_t batch_size>
class BatchArray {
   private:
    T* data_;
    size_t size_ = 0;  // 初始化成员变量

   public:
    BatchArray() { data_ = new T[batch_size]; }
    ~BatchArray() { delete[] data_; }
    BatchArray(const BatchArray&) = delete;
    BatchArray& operator=(const BatchArray&) = delete;
    BatchArray(BatchArray&& other) noexcept : size_(other.size_) {
        data_ = other.data_;
        other.data_ = nullptr;
    }

    BatchArray& operator=(BatchArray&& other) noexcept {
        if (this != &other) {
            size_ = other.size_;
            data_ = other.data_;
            other.data_ = nullptr;
        }
        return *this;
    }

    void push_back(T&& ptr) {
        if (size_ == batch_size) throw std::runtime_error("BatchArray is full");
        data_[size_] = std::move(ptr);
        size_++;
    }

    T pop_back() {
        if (size_ == 0) throw std::runtime_error("BatchArray is empty");
        size_--;
        return std::move(data_[size_]);
    }

    bool full() const { return size_ == batch_size; }
    bool empty() const { return size_ == 0; }
    size_t size() const { return size_; }
    void clear() { size_ = 0; }

    T* begin() { return data_; }
    const T* begin() { return data_; }
    T* end() { return data_ + size_; }
    const T* end() { return data_ + size_; }
};

using BatchRecord = BatchArray<std::unique_ptr<RmRecord>, BATCHSIZE>;

#endif