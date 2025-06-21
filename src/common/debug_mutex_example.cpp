#include "debug_shared_mutex.h"
#include <thread>
#include <vector>
#include <chrono>
#define DEBUG_LOCKS  // 启用调试模式

// 使用示例
void example_usage() {
    DebugSharedMutex debug_mutex("page_2_lock");
    
    std::cout << "=== 锁状态调试示例 ===" << std::endl;
    
    // 初始状态
    debug_mutex.print_state();
    
    // 获取共享锁
    std::cout << "\n--- 获取共享锁 ---" << std::endl;
    debug_mutex.lock_shared();
    debug_mutex.print_state();
    
    // 再获取一个共享锁
    std::cout << "\n--- 再获取一个共享锁 ---" << std::endl;
    debug_mutex.lock_shared();
    debug_mutex.print_state();
    
    // 释放一个共享锁
    std::cout << "\n--- 释放一个共享锁 ---" << std::endl;
    debug_mutex.unlock_shared();
    debug_mutex.print_state();
    
    // 释放最后一个共享锁
    std::cout << "\n--- 释放最后一个共享锁 ---" << std::endl;
    debug_mutex.unlock_shared();
    debug_mutex.print_state();
    
    // 获取独占锁
    std::cout << "\n--- 获取独占锁 ---" << std::endl;
    debug_mutex.lock();
    debug_mutex.print_state();
    
    // 释放独占锁
    std::cout << "\n--- 释放独占锁 ---" << std::endl;
    debug_mutex.unlock();
    debug_mutex.print_state();
}

// 并发测试示例
void concurrent_test() {
    DebugSharedMutex debug_mutex("concurrent_test");
    
    std::vector<std::thread> threads;
    
    // 启动几个读线程
    for (int i = 0; i < 3; ++i) {
        threads.emplace_back([&debug_mutex, i]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(100 * i));
            debug_mutex.lock_shared();
            std::cout << "Reader " << i << " working..." << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            debug_mutex.unlock_shared();
        });
    }
    
    // 启动一个写线程
    threads.emplace_back([&debug_mutex]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        std::cout << "Writer trying to acquire lock..." << std::endl;
        debug_mutex.print_state();
        debug_mutex.lock();
        std::cout << "Writer working..." << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        debug_mutex.unlock();
    });
    
    for (auto& t : threads) {
        t.join();
    }
    
    debug_mutex.print_state();
}

int main() {
    std::cout << "=== 基本功能测试 ===" << std::endl;
    example_usage();
    
    std::cout << "\n=== 并发测试 ===" << std::endl;
    concurrent_test();
    
    return 0;
}
