#pragma once

#include <string>
#include <chrono>
#include <iostream>

class Timer {
   private:
    std::string msg_;
    std::chrono::high_resolution_clock::time_point start_;

   public:
    Timer(std::string_view msg) : msg_(msg), start_(std::chrono::high_resolution_clock::now()) {
        // std::cout << "[Timer] " << msg_ << " started" << std::endl;
    }

    ~Timer() {
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start_);
        std::cout << "[Timer] " << msg_ << " finished in " << duration.count() << " microseconds" << std::endl;
    }

    // 禁用拷贝构造和赋值
    Timer(const Timer&) = delete;
    Timer& operator=(const Timer&) = delete;

    // 支持移动语义
    Timer(Timer&& other) noexcept : msg_(std::move(other.msg_)), start_(other.start_) {}
    Timer& operator=(Timer&& other) noexcept {
        if (this != &other) {
            msg_ = std::move(other.msg_);
            start_ = other.start_;
        }
        return *this;
    }

    // 手动停止并获取耗时
    long long stop() {
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start_);
        std::cout << "[Timer] " << msg_ << " stopped manually in " << duration.count() << " microseconds" << std::endl;
        return duration.count();
    }

    // 重置计时器
    void reset() {
        start_ = std::chrono::high_resolution_clock::now();
        std::cout << "[Timer] " << msg_ << " reset" << std::endl;
    }

    // 获取当前已用时间（不停止计时器）
    long long elapsed() const {
        auto now = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(now - start_);
        return duration.count();
    }
};

// 便利宏定义
#define TIME_SCOPE(msg) Timer _timer(msg)
#define TIME_FUNCTION() Timer _timer(__FUNCTION__)