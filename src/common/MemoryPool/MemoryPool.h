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
    constexpr static size_t INITIAL_POOL_SIZE = 128;
    constexpr static size_t MAX_POOL_SIZE = 1024;
    MemoryPool() = default;

    void Init() {
        for (int i = 0; i < INITIAL_POOL_SIZE; i++) {
            free_points_.push_back(malloc(sizeof(T)));
        }
    }

    void DestroyAll() {
        for (auto& point : free_points_) {
            free(point);
        }
    }

    void Destroy() {
        for (auto it = free_points_.begin() + INITIAL_POOL_SIZE; it != free_points_.end(); ++it) {
            free(*it);
        }
        free_points_.resize(INITIAL_POOL_SIZE);
    }

   public:
    ~MemoryPool() { DestroyAll(); }

    MemoryPool(const MemoryPool&) = delete;
    MemoryPool& operator=(const MemoryPool&) = delete;

    static MemoryPool& getInstance() {
        static thread_local MemoryPool instance;
        return instance;
    }

    T* Malloc() {
        if (free_points_.empty()) {
            Init();
        }
        T* ptr = static_cast<T*>(free_points_.back());
        free_points_.pop_back();
        return ptr;
    }

    void Free(void* ptr) { 
        free_points_.push_back(ptr); 
        if(free_points_.size() > MAX_POOL_SIZE) Destroy();
    }
};

#endif