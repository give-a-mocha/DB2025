/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <condition_variable>
#include <mutex>
#include <unordered_map>

#include "transaction/transaction.h"

static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};

/**
 * @brief 锁管理器类，实现两阶段锁协议
 *
 * 该类负责管理数据库中的所有锁操作，包括：
 * 1. 记录级锁和表级锁的授予和释放
 * 2. 维护锁的相容性矩阵
 * 3. 处理死锁检测和预防
 * 4. 实现两阶段锁协议
 */
class LockManager {
    /**
     * @brief 锁的类型枚举
     *
     * 支持五种锁模式：
     * - SHARED：共享锁，允许并发读
     * - EXLUCSIVE：排他锁，禁止任何并发操作
     * - INTENTION_SHARED：意向共享锁，表示意图在更细粒度上加共享锁
     * - INTENTION_EXCLUSIVE：意向排他锁，表示意图在更细粒度上加排他锁
     * - S_IX：意向排他锁和共享锁的组合
     */
    enum class LockMode { SHARED, EXLUCSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, S_IX };

    /**
     * @brief 锁组的模式枚举
     *
     * 用于表示一个加锁队列中最高级别的锁类型：
     * - NON_LOCK：无锁状态
     * - IS：意向共享锁是最强的锁
     * - IX：意向排他锁是最强的锁
     * - S：共享锁是最强的锁
     * - X：排他锁是最强的锁
     * - SIX：同时具有S和IX特性的锁
     */
    enum class GroupLockMode { NON_LOCK, IS, IX, S, X, SIX };

    /**
     * @brief 锁请求类，表示事务的一次加锁申请
     */
    class LockRequest {
       public:
        LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

        txn_id_t txn_id_;     // 申请加锁的事务ID
        LockMode lock_mode_;  // 事务申请加锁的类型
        bool granted_;        // 该事务是否已经被赋予锁
    };

    /**
     * @brief 锁请求队列类，维护对同一数据项的所有锁请求
     *
     * 该类负责：
     * 1. 维护所有等待获取某个数据项锁的请求
     * 2. 管理锁请求的等待和唤醒
     * 3. 跟踪当前队列的整体锁模式
     */
    class LockRequestQueue {
       public:
        std::list<LockRequest> request_queue_;                     // 等待队列，包含已授予和等待中的锁请求
        std::condition_variable cv_;                               // 条件变量，用于实现锁等待
        GroupLockMode group_lock_mode_ = GroupLockMode::NON_LOCK;  // 当前队列的最高锁级别
    };

   public:
    LockManager() {}

    ~LockManager() {}

    /**
     * @brief 在记录上加共享锁
     * @param txn 请求加锁的事务
     * @param rid 记录ID
     * @param tab_fd 表文件描述符
     * @return 是否成功加锁
     */
    bool lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    /**
     * @brief 在记录上加排他锁
     * @param txn 请求加锁的事务
     * @param rid 记录ID
     * @param tab_fd 表文件描述符
     * @return 是否成功加锁
     */
    bool lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    /**
     * @brief 在表上加共享锁
     * @param txn 请求加锁的事务
     * @param tab_fd 表文件描述符
     * @return 是否成功加锁
     */
    bool lock_shared_on_table(Transaction* txn, int tab_fd);

    /**
     * @brief 在表上加排他锁
     * @param txn 请求加锁的事务
     * @param tab_fd 表文件描述符
     * @return 是否成功加锁
     */
    bool lock_exclusive_on_table(Transaction* txn, int tab_fd);

    /**
     * @brief 在表上加意向共享锁
     * @param txn 请求加锁的事务
     * @param tab_fd 表文件描述符
     * @return 是否成功加锁
     */
    bool lock_IS_on_table(Transaction* txn, int tab_fd);

    /**
     * @brief 在表上加意向排他锁
     * @param txn 请求加锁的事务
     * @param tab_fd 表文件描述符
     * @return 是否成功加锁
     */
    bool lock_IX_on_table(Transaction* txn, int tab_fd);

    /**
     * @brief 释放指定的锁
     * @param txn 持有锁的事务
     * @param lock_data_id 要释放的锁ID
     * @return 是否成功释放锁
     */
    bool unlock(Transaction* txn, LockDataId lock_data_id);

   private:
    std::mutex latch_;                                             // 保护锁表的互斥锁
    std::unordered_map<LockDataId, LockRequestQueue> lock_table_;  // 全局锁表，维护所有数据项的锁请求队列
};
