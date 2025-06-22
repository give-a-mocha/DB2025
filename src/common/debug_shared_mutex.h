#pragma once

#include <shared_mutex>
#include <thread>
#include <atomic>
#include "common/print.hpp"
#include "common/Format.h"
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
        INFO("[LOCK] Thread {} trying to acquire exclusive lock on {}", current_thread, name_);
        print_state();
        #endif
        
        mutex_.lock();
        exclusive_held_ = true;
        exclusive_holder_ = current_thread;
        
        #ifdef DEBUG_LOCKS
        INFO("[LOCK] Thread {} acquired exclusive lock on {}", current_thread, name_);
        #endif
    }
    
    void unlock() {
        std::thread::id current_thread = std::this_thread::get_id();
        
        #ifdef DEBUG_LOCKS
        INFO("[UNLOCK] Thread {} releasing exclusive lock on {}", current_thread, name_);
        #endif
        
        exclusive_held_ = false;
        exclusive_holder_ = std::thread::id{};
        mutex_.unlock();
        
        #ifdef DEBUG_LOCKS
        INFO("[UNLOCK] Thread {} released exclusive lock on {}", current_thread, name_);
        #endif
    }
    
    bool try_lock() {
        std::thread::id current_thread = std::this_thread::get_id();
        
        #ifdef DEBUG_LOCKS
        INFO("[TRY_LOCK] Thread {} trying to acquire exclusive lock on {}", current_thread, name_);
        #endif
        
        if (mutex_.try_lock()) {
            exclusive_held_ = true;
            exclusive_holder_ = current_thread;
            
            #ifdef DEBUG_LOCKS
            INFO("[TRY_LOCK] Thread {} acquired exclusive lock on {}", current_thread, name_);
            #endif
            return true;
        }
        
        #ifdef DEBUG_LOCKS
        INFO("[TRY_LOCK] Thread {} failed to acquire exclusive lock on {}", current_thread, name_);
        #endif
        return false;
    }
    
    // 共享锁操作
    void lock_shared() {
        std::thread::id current_thread = std::this_thread::get_id();
        
        #ifdef DEBUG_LOCKS
        INFO("[LOCK_SHARED] Thread {} trying to acquire shared lock on {}", current_thread, name_);
        print_state();
        #endif
        
        mutex_.lock_shared();
        shared_count_++;
        
        #ifdef DEBUG_LOCKS
        INFO("[LOCK_SHARED] Thread {} acquired shared lock on {} (count: {})", 
             current_thread, name_, shared_count_.load());
        #endif
    }
    
    void unlock_shared() {
        std::thread::id current_thread = std::this_thread::get_id();
        
        #ifdef DEBUG_LOCKS
        INFO("[UNLOCK_SHARED] Thread {} releasing shared lock on {} (count before: {})", 
             current_thread, name_, shared_count_.load());
        #endif
        
        shared_count_--;
        mutex_.unlock_shared();
        
        #ifdef DEBUG_LOCKS
        INFO("[UNLOCK_SHARED] Thread {} released shared lock on {} (count: {})", 
             current_thread, name_, shared_count_.load());
        #endif
    }
    
    bool try_lock_shared() {
        std::thread::id current_thread = std::this_thread::get_id();
        
        #ifdef DEBUG_LOCKS
        INFO("[TRY_LOCK_SHARED] Thread {} trying to acquire shared lock on {}", current_thread, name_);
        #endif
        
        if (mutex_.try_lock_shared()) {
            shared_count_++;
            
            #ifdef DEBUG_LOCKS
            INFO("[TRY_LOCK_SHARED] Thread {} acquired shared lock on {} (count: {})", 
                 current_thread, name_, shared_count_.load());
            #endif
            return true;
        }
        
        #ifdef DEBUG_LOCKS
        INFO("[TRY_LOCK_SHARED] Thread {} failed to acquire shared lock on {}", current_thread, name_);
        #endif
        return false;
    }
    
    // 状态查询和输出
    void print_state() const {
        print("[STATE] Mutex {}: ", name_);
        
        if (exclusive_held_) {
            println("EXCLUSIVE (holder: {})", exclusive_holder_.load());
        } else {
            int shared = shared_count_.load();
            if (shared > 0) {
                println("SHARED (count: {})", shared);
            } else {
                println("UNLOCKED");
            }
        }
    }
    
    std::string get_state_string() const {
        if (exclusive_held_) {
            return util::format("Mutex[{}]: EXCLUSIVE(holder:{})", name_, exclusive_holder_.load());
        } else {
            int shared = shared_count_.load();
            if (shared > 0) {
                return util::format("Mutex[{}]: SHARED(count:{})", name_, shared);
            } else {
                return util::format("Mutex[{}]: UNLOCKED", name_);
            }
        }
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
        INFO("[DEBUG] {} - {}", msg, (mutex).get_state_string()); \
    } while(0)
#else
    #define DEBUG_MUTEX_STATE(mutex) do {} while(0)
    #define DEBUG_MUTEX_INFO(mutex, msg) do {} while(0)
#endif
