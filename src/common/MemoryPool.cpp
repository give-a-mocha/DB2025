#pragma once

#ifndef MEMORY_POOL_H
#define MEMORY_POOL_H

template <typename T>
class MemoryPool {
   private:
    std::list<T*> buffer;
    constexpr int capacity_ = 16;

   public:
    MemoryPool() {
        for (int i = 0; i < capacity_; i++) {
            list.push_back(malloc(sizeof(T)));
        }
    }
    T* malloc() {
        
    }
};

#endif