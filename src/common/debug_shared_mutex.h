#pragma once

#include <shared_mutex>
#include <atomic>
#include <thread>
#include <sstream>
#include <iostream>
#include <iomanip>
#define DEBUG_LOCKS  // 启用调试模式

/**
 * 带调试功能的 shared_mutex 包装类
 * 提供锁状态输出和统计功能
 */
class DebugSharedMutex {
private:
    std::shared_mutex mutex_;
    
    // 统计信息
    std::atomic<int> shared_count_{0};           // 当前共享锁数量
    std::atomic<bool> exclusive_held_{false};    // 是否持有独占锁
    std::atomic<std::thread::id> exclusive_holder_{std::thread::id{}};  // 独占锁持有者
    
    // 调试标识
    std::string name_;
    
public:
    explicit DebugSharedMutex(const std::string& name = "unnamed") : name_(name) {}
    
    // 独占锁操作
    void lock() {
        std::thread::id current_thread = std::this_thread::get_id();
        
        #ifdef DEBUG_LOCKS
        std::cout << "[LOCK] Thread " << current_thread << " trying to acquire exclusive lock on " << name_ << std::endl;
        print_state();
        #endif
        
        mutex_.lock();
        exclusive_held_ = true;
        exclusive_holder_ = current_thread;
        
        #ifdef DEBUG_LOCKS
        std::cout << "[LOCK] Thread " << current_thread << " acquired exclusive lock on " << name_ << std::endl;
        #endif
    }
    
    void unlock() {
        std::thread::id current_thread = std::this_thread::get_id();
        
        #ifdef DEBUG_LOCKS
        std::cout << "[UNLOCK] Thread " << current_thread << " releasing exclusive lock on " << name_ << std::endl;
        #endif
        
        exclusive_held_ = false;
        exclusive_holder_ = std::thread::id{};
        mutex_.unlock();
        
        #ifdef DEBUG_LOCKS
        std::cout << "[UNLOCK] Thread " << current_thread << " released exclusive lock on " << name_ << std::endl;
        #endif
    }
    
    bool try_lock() {
        std::thread::id current_thread = std::this_thread::get_id();
        
        #ifdef DEBUG_LOCKS
        std::cout << "[TRY_LOCK] Thread " << current_thread << " trying to acquire exclusive lock on " << name_ << std::endl;
        #endif
        
        if (mutex_.try_lock()) {
            exclusive_held_ = true;
            exclusive_holder_ = current_thread;
            
            #ifdef DEBUG_LOCKS
            std::cout << "[TRY_LOCK] Thread " << current_thread << " acquired exclusive lock on " << name_ << std::endl;
            #endif
            return true;
        }
        
        #ifdef DEBUG_LOCKS
        std::cout << "[TRY_LOCK] Thread " << current_thread << " failed to acquire exclusive lock on " << name_ << std::endl;
        #endif
        return false;
    }
    
    // 共享锁操作
    void lock_shared() {
        std::thread::id current_thread = std::this_thread::get_id();
        
        #ifdef DEBUG_LOCKS
        std::cout << "[LOCK_SHARED] Thread " << current_thread << " trying to acquire shared lock on " << name_ << std::endl;
        print_state();
        #endif
        
        mutex_.lock_shared();
        shared_count_++;
        
        #ifdef DEBUG_LOCKS
        std::cout << "[LOCK_SHARED] Thread " << current_thread << " acquired shared lock on " << name_ 
                  << " (count: " << shared_count_.load() << ")" << std::endl;
        #endif
    }
    
    void unlock_shared() {
        std::thread::id current_thread = std::this_thread::get_id();
        
        #ifdef DEBUG_LOCKS
        std::cout << "[UNLOCK_SHARED] Thread " << current_thread << " releasing shared lock on " << name_ 
                  << " (count before: " << shared_count_.load() << ")" << std::endl;
        #endif
        
        shared_count_--;
        mutex_.unlock_shared();
        
        #ifdef DEBUG_LOCKS
        std::cout << "[UNLOCK_SHARED] Thread " << current_thread << " released shared lock on " << name_ 
                  << " (count: " << shared_count_.load() << ")" << std::endl;
        #endif
    }
    
    bool try_lock_shared() {
        std::thread::id current_thread = std::this_thread::get_id();
        
        #ifdef DEBUG_LOCKS
        std::cout << "[TRY_LOCK_SHARED] Thread " << current_thread << " trying to acquire shared lock on " << name_ << std::endl;
        #endif
        
        if (mutex_.try_lock_shared()) {
            shared_count_++;
            
            #ifdef DEBUG_LOCKS
            std::cout << "[TRY_LOCK_SHARED] Thread " << current_thread << " acquired shared lock on " << name_ 
                      << " (count: " << shared_count_.load() << ")" << std::endl;
            #endif
            return true;
        }
        
        #ifdef DEBUG_LOCKS
        std::cout << "[TRY_LOCK_SHARED] Thread " << current_thread << " failed to acquire shared lock on " << name_ << std::endl;
        #endif
        return false;
    }
    
    // 状态查询和输出
    void print_state() const {
        std::cout << "[STATE] Mutex " << name_ << ": ";
        
        if (exclusive_held_) {
            std::cout << "EXCLUSIVE (holder: " << exclusive_holder_.load() << ")";
        } else {
            int shared = shared_count_.load();
            if (shared > 0) {
                std::cout << "SHARED (count: " << shared << ")";
            } else {
                std::cout << "UNLOCKED";
            }
        }
        std::cout << std::endl;
    }
    
    std::string get_state_string() const {
        std::ostringstream oss;
        oss << "Mutex[" << name_ << "]: ";
        
        if (exclusive_held_) {
            oss << "EXCLUSIVE(holder:" << exclusive_holder_.load() << ")";
        } else {
            int shared = shared_count_.load();
            if (shared > 0) {
                oss << "SHARED(count:" << shared << ")";
            } else {
                oss << "UNLOCKED";
            }
        }
        return oss.str();
    }
    
    // 统计信息
    bool is_locked_exclusive() const { return exclusive_held_.load(); }
    int shared_lock_count() const { return shared_count_.load(); }
    bool is_locked() const { return exclusive_held_.load() || shared_count_.load() > 0; }
    std::thread::id get_exclusive_holder() const { return exclusive_holder_.load(); }
};

// 便利宏定义
#ifdef DEBUG_LOCKS
    #define DEBUG_MUTEX_STATE(mutex) do { (mutex).print_state(); } while(0)
    #define DEBUG_MUTEX_INFO(mutex, msg) do { \
        std::cout << "[DEBUG] " << msg << " - " << (mutex).get_state_string() << std::endl; \
    } while(0)
#else
    #define DEBUG_MUTEX_STATE(mutex) do {} while(0)
    #define DEBUG_MUTEX_INFO(mutex, msg) do {} while(0)
#endif
