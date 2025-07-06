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
#include <list>
#include <mutex>
#include <unordered_map>

#include "common/common.h"
#include "transaction/transaction.h"
#include "transaction/txn_defs.h"

static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};

class LockManager {
    /* 加锁类型，包括共享锁、排他锁、意向共享锁、意向排他锁、SIX（意向排他锁+共享锁） */
    enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, S_IX };

    /* 用于标识加锁队列中排他性最强的锁类型，例如加锁队列中有SHARED和EXLUSIVE两个加锁操作，则该队列的锁模式为X */
    enum class GroupLockMode { NON_LOCK, IS, IX, S, X, SIX };

    /* 事务的加锁申请 */
    class LockRequest {
       public:
        txn_id_t txn_id_;     // 申请加锁的事务ID
        LockMode lock_mode_;  // 事务申请加锁的类型
        bool granted_;        // 该事务是否已经被赋予锁

        LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}
    };

    /* 数据项上的加锁队列 */
    class LockRequestQueue {
       public:
        // 加锁队列
        std::list<LockRequest> request_queue_;
        // 条件变量，用于唤醒正在等待加锁的申请，在no-wait策略下无需使用
        std::condition_variable cv_;
        // 加锁队列的锁模式
        GroupLockMode group_lock_mode_ = GroupLockMode::NON_LOCK;
        // 当前持有排他锁的事务ID，如果没有则为-1
        txn_id_t exclusive_holder_ = -1;
        // 当前持有排他锁的迭代器，如果没有则为end()
        std::list<LockRequest>::iterator exclusive_holder_it_;
        
        LockRequestQueue() {
            exclusive_holder_it_ = request_queue_.end();
        }
    };

    // GapLockRequest is now defined in "transaction/txn_defs.h"

    /* 数据表上的间隙锁队列 */
    class GapLockRequestQueue {
       public:
        // 间隙锁加锁队列
        std::list<GapLockRequest> request_queue_;
    };

   private:
    // 用于锁表的并发
    std::mutex latch_;
    // 全局锁表
    std::unordered_map<LockDataId, LockRequestQueue> lock_table_;
    // 全局间隙锁表, key 为 fd_
    std::unordered_map<int, GapLockRequestQueue> gap_lock_table_;

   public:
    LockManager() {}

    ~LockManager() {}

    bool lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    bool lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    bool lock_shared_on_table(Transaction* txn, int tab_fd);

    bool lock_exclusive_on_table(Transaction* txn, int tab_fd);

    bool is_lock_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    bool lock_IS_on_table(Transaction* txn, int tab_fd);

    bool lock_IX_on_table(Transaction* txn, int tab_fd);

    bool unlock(Transaction* txn, LockDataId lock_data_id);

    bool lock_gap(Transaction* txn, int tab_fd, std::vector<Condition> conds);

    std::vector<Condition> get_gap_condition(int tab_fd, Transaction* txn);

    bool unlock_gap(Transaction* txn, int tab_fd);
};
