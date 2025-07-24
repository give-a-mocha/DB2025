#pragma once

#ifndef RERECORD_MANAGER_H
#define RECORD_MANAGER_H

#include <deque>
#include <mutex>

/**
 * @brief 线程安全内存池
 */
template <typename T>
class MemoryPool {
   private:
    std::mutex pool_mutex_;
    std::deque<void*> free_points_;
    constexpr static size_t INITIAL_POOL_SIZE = 100;
    MemoryPool() = default;

    void Init() {
        for(int i = 0; i < INITIAL_POOL_SIZE; i++) {
            free_points_.push_back(malloc(sizeof(T)));
        }
    }

    void Destroy() {
        for(auto& point : free_points_) {
            free(point);
        }
    }

   public:
    ~MemoryPool() {
        Destroy();
    }

    MemoryPool(const MemoryPool&) = delete;
    MemoryPool& operator=(const MemoryPool&) = delete;

    static MemoryPool& getInstance() {
        static MemoryPool instance;
        return instance;
    }

    T* Malloc() {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        if(free_points_.empty()) {
            Init();
        }
        T* ptr = static_cast<T*>(free_points_.back());
        free_points_.pop_back();
        return ptr;
    }

    void Free(void* ptr) {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        free_points_.push_back(ptr);
    }
};

#endif