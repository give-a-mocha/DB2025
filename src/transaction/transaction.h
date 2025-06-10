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

struct UndoLink {
    //创建前一个版本的事务ID。
    txn_id_t prev_txn_{INVALID_TXN_ID};
    // 前一个版本在对应事务撤销日志中的索引。
    int prev_log_idx_{0};

    friend auto operator==(const UndoLink &a, const UndoLink &b) {
        return a.prev_txn_ == b.prev_txn_ && a.prev_log_idx_ == b.prev_log_idx_;
    }

    friend auto operator!=(const UndoLink &a, const UndoLink &b) { return !(a == b); }

    bool IsValid() { return prev_txn_ != INVALID_TXN_ID; }
};


struct UndoLog {
    // 标记此日志条目是否代表一个删除操作。
    bool is_deleted_;
    // 一个布尔向量，标记哪些字段被修改了 (true 表示对应索引的字段被修改)
    std::vector<bool> modified_fields_;
    // 存储被修改字段在修改前的值
    std::vector<Value> tuple_;
    // 指向实际元组记录的指针
    RmRecord *tuple_test_;
    // 此撤销日志条目的时间戳，用于可见性检查
    timestamp_t ts_{INVALID_TS};
    // 指向该元组下一个更旧版本的链接，构成版本链。
    UndoLink prev_version_{};
};

/**
 * @brief 代表一个数据库事务。
 * @details 管理事务的状态、隔离级别、写集合、锁集合以及撤销日志等。
 */
class Transaction {
friend class TransactionManager; // 允许 TransactionManager 访问私有成员
private:
    // 事务模式标志：true 表示显式事务，false 表示隐式事务（单条 SQL 语句）
    bool txn_mode_;
    // 事务当前状态（例如：DEFAULT, RUNNING, COMMITTED, ABORTED）
    TransactionState state_;
    // 事务的隔离级别（例如：READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE）
    IsolationLevel isolation_level_;
    // 执行该事务的线程 ID
    std::thread::id thread_id_;
    // 前一个日志序列号 (LSN)，用于 WAL（预写日志）和故障恢复
    lsn_t prev_lsn_;
    // 事务的唯一标识符
    txn_id_t txn_id_;
    // 事务的开始时间戳，用于并发控制
    timestamp_t start_ts_;

    // 事务的写集合，记录所有写操作 (使用智能指针管理内存)
    std::shared_ptr<std::deque<std::unique_ptr<WriteRecord>>> write_set_;
    // 事务持有的所有锁，用于并发控制
    std::shared_ptr<std::unordered_set<LockDataId>> lock_set_;
    // 事务中已加锁的索引页面集合
    std::shared_ptr<std::deque<Page *>> index_latch_page_set_;
    // 事务中已删除的索引页面集合
    std::shared_ptr<std::deque<Page *>> index_deleted_page_set_;

    // 读时间戳，原子变量确保线程安全
    std::atomic<timestamp_t> read_ts_{0};
    // 提交时间戳，事务提交时分配，用于确定对其他事务的可见性。原子变量确保线程安全
    std::atomic<timestamp_t> commit_ts_{INVALID_TS};

    /**
     * @brief 存储事务的所有撤销日志。
     * @details 其他事务/表通过 (txn_id, index) 对引用这些日志，
     * 因此只能追加或修改，不能删除其中的元素。
     */
    std::vector<UndoLog> undo_logs_;

    /** @brief 保护撤销日志向量访问的互斥锁，确保多线程环境下的安全访问。 */
    std::mutex latch_;
public:

    explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::SERIALIZABLE)
        : state_(TransactionState::DEFAULT), isolation_level_(isolation_level), txn_id_(txn_id) {
        /* 初始化事务的写集合（记录所有写操作），使用智能指针 */
        write_set_ = std::make_shared<std::deque<std::unique_ptr<WriteRecord>>>();
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


    inline txn_id_t get_transaction_id() { return txn_id_; }

    inline std::thread::id get_thread_id() { return thread_id_; }

    inline void set_txn_mode(bool txn_mode) { txn_mode_ = txn_mode; }

    inline bool get_txn_mode() { return txn_mode_; }

    inline void set_start_ts(timestamp_t start_ts) { start_ts_ = start_ts; }

    inline timestamp_t get_start_ts() { return start_ts_; }

    inline IsolationLevel get_isolation_level() { return isolation_level_; }

    inline TransactionState get_state() { return state_; }

    inline void set_state(TransactionState state) { state_ = state; }

    inline lsn_t get_prev_lsn() { return prev_lsn_; }
 
    inline void set_prev_lsn(lsn_t prev_lsn) { prev_lsn_ = prev_lsn; }

    // 返回写集合的共享指针
    inline std::shared_ptr<std::deque<std::unique_ptr<WriteRecord>>> get_write_set() { return write_set_; }

    // 向写集合添加一个写记录 (接收 unique_ptr)
    inline void append_write_record(std::unique_ptr<WriteRecord> write_record) { write_set_->push_back(std::move(write_record)); }

    inline std::shared_ptr<std::deque<Page *>> get_index_deleted_page_set() { return index_deleted_page_set_; }

    inline void append_index_deleted_page(Page *page) { index_deleted_page_set_->push_back(page); }

    inline std::shared_ptr<std::deque<Page *>> get_index_latch_page_set() { return index_latch_page_set_; }

    inline void append_index_latch_page_set(Page *page) { index_latch_page_set_->push_back(page); }

    inline std::shared_ptr<std::unordered_set<LockDataId>> get_lock_set() { return lock_set_; }

    inline timestamp_t get_read_ts() const { return read_ts_; }

    inline timestamp_t get_commit_ts() const { return commit_ts_; }

    inline auto ModifyUndoLog(int log_idx, UndoLog new_log) {
        std::scoped_lock<std::mutex> lck(latch_);
        undo_logs_[log_idx] = std::move(new_log);
    }

    inline auto AppendUndoLog(UndoLog log) -> UndoLink {
        std::scoped_lock<std::mutex> lck(latch_);
        undo_logs_.emplace_back(std::move(log));
        return {txn_id_, static_cast<int>(undo_logs_.size() - 1)};
    }

    inline auto GetUndoLog(size_t log_id) -> UndoLog {
        std::scoped_lock<std::mutex> lck(latch_);
        // 注意：如果 log_id 无效，这里可能抛出 std::out_of_range 异常
        return undo_logs_[log_id];
    }

    inline auto GetUndoLogNum() -> size_t {
        std::scoped_lock<std::mutex> lck(latch_);
        return undo_logs_.size();
    }

    inline auto clear() -> void {
        write_set_->clear();               // 清空事务的写集合
        lock_set_->clear();                // 清空事务的锁集合
        index_latch_page_set_->clear();    // 清空索引加锁页面集合
        index_deleted_page_set_->clear();  // 清空索引删除页面集合
    }
};