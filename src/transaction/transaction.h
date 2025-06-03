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

#include <atomic>
#include <deque>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "common/common.h"
#include "record/rm_defs.h"
#include "transaction/txn_defs.h"

/**
 * @brief 表示元组前一个版本的链接，用于多版本并发控制 (MVCC)。
 */
struct UndoLink {
    /** @brief 创建前一个版本的事务ID。 */
    txn_id_t prev_txn_{INVALID_TXN_ID};
    /** @brief 前一个版本在对应事务撤销日志中的索引。 */
    int prev_log_idx_{0};

    /**
     * @brief 比较两个 UndoLink 对象是否相等。
     * @param a 第一个 UndoLink 对象。
     * @param b 第二个 UndoLink 对象。
     * @return 如果两个 UndoLink 相等则返回 true，否则返回 false。
     */
    friend auto operator==(const UndoLink &a, const UndoLink &b) {
        return a.prev_txn_ == b.prev_txn_ && a.prev_log_idx_ == b.prev_log_idx_;
    }

    /**
     * @brief 比较两个 UndoLink 对象是否不相等。
     * @param a 第一个 UndoLink 对象。
     * @param b 第二个 UndoLink 对象。
     * @return 如果两个 UndoLink 不相等则返回 true，否则返回 false。
     */
    friend auto operator!=(const UndoLink &a, const UndoLink &b) { return !(a == b); }

    /**
     * @brief 检查此撤销链接是否有效（即是否指向一个实际存在的前版本）。
     * @return 如果链接有效则返回 true，否则返回 false。
     */
    bool IsValid() { return prev_txn_ != INVALID_TXN_ID; }
};

/**
 * @brief 撤销日志结构，存储数据修改前的状态
 *
 * 主要功能：
 * 1. 支持事务回滚操作
 * 2. 提供多版本并发控制的版本链
 * 3. 记录修改的字段和对应的值
 *
 * 重要组成：
 * - 删除标记：标识是否为删除操作
 * - 修改标记：记录哪些字段被修改
 * - 数据内容：存储修改前的值
 * - 时间戳：用于并发控制
 * - 版本链：指向更早的版本
 */
struct UndoLog {
    /** @brief 标记此日志条目是否代表一个删除操作。 */
    bool is_deleted_;
    /** @brief 一个布尔向量，标记哪些字段被修改了 (true 表示对应索引的字段被修改)。 */
    std::vector<bool> modified_fields_;
    /** @brief 存储被修改字段在修改*前*的值。 */
    std::vector<Value> tuple_;
    /** @brief 指向实际元组记录的指针 (主要用于测试)。 */
    RmRecord *tuple_test_;
    /** @brief 此撤销日志条目的时间戳，用于可见性检查。 */
    timestamp_t ts_{INVALID_TS};
    /** @brief 指向该元组下一个更旧版本的链接，构成版本链。 */
    UndoLink prev_version_{};
};

/**
 * @brief 代表一个数据库事务。
 * @details 管理事务的状态、隔离级别、写集合、锁集合以及撤销日志等。
 */
class Transaction {
   public:
    /**
     * @brief 构造一个新的事务对象。
     * @param txn_id 此事务的唯一标识符。
     * @param isolation_level 此事务的隔离级别 (默认为 SERIALIZABLE)。
     */
    explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::SERIALIZABLE)
        : state_(TransactionState::DEFAULT), isolation_level_(isolation_level), txn_id_(txn_id) {
        /* 初始化事务的写集合（记录所有写操作） */
        write_set_ = std::make_shared<std::deque<WriteRecord *>>();
        /* 初始化事务持有的锁集合 */
        lock_set_ = std::make_shared<std::unordered_set<LockDataId>>();
        /* 初始化事务中使用的索引页面集合（加锁的索引页面） */
        index_latch_page_set_ = std::make_shared<std::deque<Page *>>();
        /* 初始化事务中删除的索引页面集合 */
        index_deleted_page_set_ = std::make_shared<std::deque<Page *>>();
        /* 初始化日志序列号为无效值 */
        prev_lsn_ = INVALID_LSN;
        /* 记录当前线程ID，用于标识哪个线程在执行此事务 */
        thread_id_ = std::this_thread::get_id();
    }

    /** @brief 默认析构函数。 */
    ~Transaction() = default;

    /**
     * @brief 获取事务ID。
     * @return 返回当前事务的唯一标识符。
     */
    inline txn_id_t get_transaction_id() { return txn_id_; }

    /**
     * @brief 获取执行此事务的线程ID。
     * @return 返回执行该事务的线程标识符。
     */
    inline std::thread::id get_thread_id() { return thread_id_; }

    /**
     * @brief 设置事务模式。
     * @param txn_mode true 表示显式事务，false 表示隐式事务。
     */
    inline void set_txn_mode(bool txn_mode) { txn_mode_ = txn_mode; }
    /**
     * @brief 获取事务模式。
     * @return 如果是显式事务返回 true，隐式事务返回 false。
     */
    inline bool get_txn_mode() { return txn_mode_; }

    /**
     * @brief 设置事务的开始时间戳。
     * @param start_ts 事务开始的时间戳值。
     */
    inline void set_start_ts(timestamp_t start_ts) { start_ts_ = start_ts; }
    /**
     * @brief 获取事务的开始时间戳。
     * @return 返回事务的开始时间戳。
     */
    inline timestamp_t get_start_ts() { return start_ts_; }

    /**
     * @brief 获取事务的隔离级别。
     * @return 返回事务的隔离级别。
     */
    inline IsolationLevel get_isolation_level() { return isolation_level_; }

    /**
     * @brief 获取事务的当前状态。
     * @return 返回事务的当前状态。
     */
    inline TransactionState get_state() { return state_; }
    /**
     * @brief 设置事务的状态 (例如：RUNNING, COMMITTED, ABORTED)。
     * @param state 新的事务状态。
     */
    inline void set_state(TransactionState state) { state_ = state; }

    /**
     * @brief 获取前一个日志序列号 (LSN)，用于故障恢复。
     * @return 返回前一个 LSN。
     */
    inline lsn_t get_prev_lsn() { return prev_lsn_; }
    /**
     * @brief 设置前一个日志序列号 (LSN)。
     * @param prev_lsn 前一个 LSN。
     */
    inline void set_prev_lsn(lsn_t prev_lsn) { prev_lsn_ = prev_lsn; }

    /**
     * @brief 获取写集合，包含事务中所有写操作的记录。
     * @return 返回指向写记录指针双端队列的共享指针。
     */
    inline std::shared_ptr<std::deque<WriteRecord *>> get_write_set() { return write_set_; }
    /**
     * @brief 向写集合追加一个写操作记录。
     * @param write_record 要追加的写记录。
     */
    inline void append_write_record(WriteRecord *write_record) { write_set_->push_back(write_record); }

    /**
     * @brief 获取被删除的索引页面集合。
     * @return 返回指向已删除页面指针双端队列的共享指针。
     */
    inline std::shared_ptr<std::deque<Page *>> get_index_deleted_page_set() { return index_deleted_page_set_; }
    /**
     * @brief 向集合中添加一个被删除的索引页面。
     * @param page 指向被删除索引页面的指针。
     */
    inline void append_index_deleted_page(Page *page) { index_deleted_page_set_->push_back(page); }

    /**
     * @brief 获取已加锁的索引页面集合。
     * @return 返回指向已加锁页面指针双端队列的共享指针。
     */
    inline std::shared_ptr<std::deque<Page *>> get_index_latch_page_set() { return index_latch_page_set_; }
    /**
     * @brief 向集合中添加一个已加锁的索引页面。
     * @param page 指向已加锁索引页面的指针。
     */
    inline void append_index_latch_page_set(Page *page) { index_latch_page_set_->push_back(page); }

    /**
     * @brief 获取事务持有的锁集合。
     * @return 返回指向 LockDataId 无序集合的共享指针。
     */
    inline std::shared_ptr<std::unordered_set<LockDataId>> get_lock_set() { return lock_set_; }

    /**
     * @brief 获取读时间戳，用于读操作的可见性检查。
     * @return 返回读时间戳。
     */
    inline timestamp_t get_read_ts() const { return read_ts_; }
    /**
     * @brief 获取提交时间戳，在事务提交时分配。
     * @return 返回提交时间戳。
     */
    inline timestamp_t get_commit_ts() const { return commit_ts_; }

    /**
     * @brief 修改一个已存在的撤销日志条目。线程安全。
     * @param log_idx 要修改的撤销日志条目的索引。
     * @param new_log 用于替换现有条目的新 UndoLog 数据。
     */
    inline auto ModifyUndoLog(int log_idx, UndoLog new_log) {
        std::scoped_lock<std::mutex> lck(latch_);
        undo_logs_[log_idx] = std::move(new_log);
    }

    /**
     * @brief 追加一个撤销日志条目并返回指向它的链接。线程安全。
     * @details 此链接可用于构建版本链。
     * @param log 要追加的 UndoLog 条目。
     * @return 返回指向新添加日志条目的 UndoLink。
     */
    inline auto AppendUndoLog(UndoLog log) -> UndoLink {
        std::scoped_lock<std::mutex> lck(latch_);
        undo_logs_.emplace_back(std::move(log));
        return {txn_id_, static_cast<int>(undo_logs_.size() - 1)};
    }

    /**
     * @brief 获取指定索引处的撤销日志条目。线程安全。
     * @param log_id 要获取的撤销日志条目的索引。
     * @return 返回 UndoLog 条目。
     * @throws std::out_of_range 如果 log_id 无效。
     */
    inline auto GetUndoLog(size_t log_id) -> UndoLog {
        std::scoped_lock<std::mutex> lck(latch_);
        // 注意：如果 log_id 无效，这里可能抛出 std::out_of_range 异常
        return undo_logs_[log_id];
    }

    /**
     * @brief 获取此事务的撤销日志总数。线程安全。
     * @details 用于调试和监控。
     * @return 返回撤销日志条目的数量。
     */
    inline auto GetUndoLogNum() -> size_t {
        std::scoped_lock<std::mutex> lck(latch_);
        return undo_logs_.size();
    }

   private:
    /** @brief 事务模式标志：true 表示显式事务，false 表示隐式事务（单条 SQL 语句）。 */
    bool txn_mode_;
    /** @brief 事务当前状态（例如：DEFAULT, RUNNING, COMMITTED, ABORTED）。 */
    TransactionState state_;
    /** @brief 事务的隔离级别（例如：READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE）。 */
    IsolationLevel isolation_level_;
    /** @brief 执行该事务的线程 ID。 */
    std::thread::id thread_id_;
    /** @brief 前一个日志序列号 (LSN)，用于 WAL（预写日志）和故障恢复。 */
    lsn_t prev_lsn_;
    /** @brief 事务的唯一标识符。 */
    txn_id_t txn_id_;
    /** @brief 事务的开始时间戳，用于并发控制。 */
    timestamp_t start_ts_;

    /** @brief 事务的写集合，记录所有写操作。 */
    std::shared_ptr<std::deque<WriteRecord *>> write_set_;
    /** @brief 事务持有的所有锁，用于并发控制。 */
    std::shared_ptr<std::unordered_set<LockDataId>> lock_set_;
    /** @brief 事务中已加锁的索引页面集合。 */
    std::shared_ptr<std::deque<Page *>> index_latch_page_set_;
    /** @brief 事务中已删除的索引页面集合。 */
    std::shared_ptr<std::deque<Page *>> index_deleted_page_set_;

    /** @brief 读时间戳，原子变量确保线程安全。 */
    std::atomic<timestamp_t> read_ts_{0};
    /** @brief 提交时间戳，事务提交时分配，用于确定对其他事务的可见性。原子变量确保线程安全。 */
    std::atomic<timestamp_t> commit_ts_{INVALID_TS};

    /**
     * @brief 存储事务的所有撤销日志。
     * @details 其他事务/表通过 (txn_id, index) 对引用这些日志，
     * 因此只能追加或修改，不能删除其中的元素。
     */
    std::vector<UndoLog> undo_logs_;

    /** @brief 保护撤销日志向量访问的互斥锁，确保多线程环境下的安全访问。 */
    std::mutex latch_;
};