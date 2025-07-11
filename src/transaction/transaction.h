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

/** 表示此tuple的前一个版本的链接 */
struct UndoLink {
    /* 之前的版本可以在其中的事务中找到 */
    txn_id_t prev_txn_{INVALID_TXN_ID};
    /* 在 `prev_txn_` 中前一个版本的日志索引 */
    int prev_log_idx_{0};

    friend auto operator==(const UndoLink &a, const UndoLink &b) {
        return a.prev_txn_ == b.prev_txn_ && a.prev_log_idx_ == b.prev_log_idx_;
    }

    friend auto operator!=(const UndoLink &a, const UndoLink &b) { return !(a == b); }

    bool IsValid() { return prev_txn_ != INVALID_TXN_ID; }
};

// 这里是创建跟当前操作相反的撤销日志
// 对于insert操作需要创建delete
struct UndoLog {
    /* 此日志是否为删除标记 */
    bool is_deleted_;
    /* 此撤销日志修改的字段 */
    RmRecord record_;
    /* 此撤销日志的时间戳 */
    timestamp_t ts_{INVALID_TS};
    /* 撤销日志的前一个版本 */
    UndoLink prev_version_{};

    // 构造函数
    UndoLog(bool is_deleted, const RmRecord &record, timestamp_t ts = INVALID_TS, UndoLink prev_version = {})
        : is_deleted_(is_deleted), record_(record), ts_(ts), prev_version_(prev_version) {}

    // 默认构造函数
    UndoLog() : is_deleted_(false) {}

    // 默认析构函数（编译器生成）
    ~UndoLog() = default;

    // 禁用拷贝构造和拷贝赋值（避免意外拷贝大对象）
    UndoLog(const UndoLog &) = delete;
    UndoLog &operator=(const UndoLog &) = delete;

    // 启用移动构造和移动赋值
    UndoLog(UndoLog &&other) noexcept
        : is_deleted_(other.is_deleted_),
          record_(std::move(other.record_)),
          ts_(other.ts_),
          prev_version_(other.prev_version_) {}

    UndoLog &operator=(UndoLog &&other) noexcept {
        if (this != &other) {
            is_deleted_ = other.is_deleted_;
            record_ = std::move(other.record_);
            ts_ = other.ts_;
            prev_version_ = other.prev_version_;
        }
        return *this;
    }
};

class Transaction {
   private:
    // 用于标识当前事务为显式事务还是单条SQL语句的隐式事务
    bool txn_mode_;
    // 事务状态
    TransactionState state_;
    // 事务的隔离级别，默认隔离级别为可串行化
    IsolationLevel isolation_level_;
    // 当前事务对应的线程id
    std::thread::id thread_id_;
    // 当前事务执行的最后一条操作对应的lsn，用于系统故障恢复
    lsn_t prev_lsn_;
    // 事务的ID，唯一标识符
    txn_id_t txn_id_;
    // 事务的开始时间戳
    timestamp_t start_ts_;

    // 事务包含的所有写操作
    std::shared_ptr<std::deque<std::unique_ptr<WriteRecord>>> write_set_;
    // 事务申请的所有锁
    std::shared_ptr<std::unordered_set<LockDataId>> lock_set_;

    std::shared_ptr<std::unordered_set<int>> lock_gap_set_;
    // 维护事务执行过程中加锁的索引页面
    std::shared_ptr<std::deque<Page *>> index_latch_page_set_;
    // 维护事务执行过程中删除的索引页面
    std::shared_ptr<std::deque<Page *>> index_deleted_page_set_;

    std::atomic<timestamp_t> read_ts_{0};
    /** 提交时间戳 */
    std::atomic<timestamp_t> commit_ts_{INVALID_TS};

    /**
     * @brief 存储撤销日志。
     * 其他撤销日志/表堆将存储 (txn_id, index) 对，因此只能向此vector中追加内容或就地更新内容，而不能删除任何内容。
     * 使用智能指针避免大对象的拷贝开销，提升性能。
     */
    std::vector<std::unique_ptr<UndoLog>> undo_logs_;

    /** 用于访问事务级撤销日志的锁。 */
    std::mutex latch_;

   public:
    explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::SERIALIZABLE)
        : state_(TransactionState::DEFAULT), isolation_level_(isolation_level), txn_id_(txn_id) {
        /* 初始化事务的写集合（记录所有写操作），使用智能指针 */
        write_set_ = std::make_shared<std::deque<std::unique_ptr<WriteRecord>>>();
        /* 初始化事务持有的锁集合 */
        lock_set_ = std::make_shared<std::unordered_set<LockDataId>>();

        lock_gap_set_ = std::make_shared<std::unordered_set<int>>();
        /* 初始化事务中使用的索引页面集合（加锁的索引页面） */
        index_latch_page_set_ = std::make_shared<std::deque<Page *>>();
        /* 初始化事务中删除的索引页面集合 */
        index_deleted_page_set_ = std::make_shared<std::deque<Page *>>();
        /* 初始化日志序列号为无效值 */
        prev_lsn_ = INVALID_LSN;
        /* 记录当前线程ID，用于标识哪个线程在执行此事务 */
        thread_id_ = std::this_thread::get_id();
    }

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
    inline void append_write_record(std::unique_ptr<WriteRecord> write_record) {
        write_set_->push_back(std::move(write_record));
    }

    inline std::shared_ptr<std::deque<Page *>> get_index_deleted_page_set() { return index_deleted_page_set_; }

    inline void append_index_deleted_page(Page *page) { index_deleted_page_set_->push_back(page); }

    inline std::shared_ptr<std::deque<Page *>> get_index_latch_page_set() { return index_latch_page_set_; }

    inline void append_index_latch_page_set(Page *page) { index_latch_page_set_->push_back(page); }

    inline std::shared_ptr<std::unordered_set<LockDataId>> get_lock_set() { return lock_set_; }

    inline std::shared_ptr<std::unordered_set<int>> get_lock_gap_set() { return lock_gap_set_; }

    inline timestamp_t get_read_ts() const { return read_ts_; }

    inline void set_read_ts(timestamp_t read_ts) { read_ts_.store(read_ts); }

    inline timestamp_t get_commit_ts() const { return commit_ts_; }

    inline void set_commit_ts(timestamp_t commit_ts) { commit_ts_.store(commit_ts); }
    /** 修改现有的撤销日志 */
    inline auto ModifyUndoLog(int log_idx, std::unique_ptr<UndoLog> new_log) {
        std::scoped_lock<std::mutex> lck(latch_);
        undo_logs_[log_idx] = std::move(new_log);
    }

    /** @return 此事务中撤销日志的索引 */
    inline auto AppendUndoLog(std::unique_ptr<UndoLog> log) -> UndoLink {
        std::scoped_lock<std::mutex> lck(latch_);
        undo_logs_.emplace_back(std::move(log));
        return {txn_id_, static_cast<int>(undo_logs_.size() - 1)};
    }

    inline auto GetUndoLog(size_t log_id) -> const UndoLog * {
        std::scoped_lock<std::mutex> lck(latch_);
        // 注意：如果 log_id 无效，这里可能抛出 std::out_of_range 异常
        return undo_logs_[log_id].get();
    }

    inline auto CommitUndoLogs() -> void {
        std::scoped_lock<std::mutex> lck(latch_);
        // 提交事务的撤销日志
        for (auto &log : undo_logs_) {
            log->ts_ = commit_ts_.load();  // 设置撤销日志的时间戳为提交时间戳
        }
    }

    inline auto ClearUndoLogs() -> void {
        std::scoped_lock<std::mutex> lck(latch_);
        undo_logs_.clear();  // 清空事务的撤销日志
    }

    /** @return 撤销日志的数量 */
    inline auto GetUndoLogNum() -> size_t {
        std::scoped_lock<std::mutex> lck(latch_);
        return undo_logs_.size();
    }

    inline auto clear_lock_set() -> void {
        lock_set_->clear();                // 清空事务的锁集合
        lock_gap_set_->clear();            // 清空事务的间隙锁集合
        index_latch_page_set_->clear();    // 清空索引加锁页面集合
        index_deleted_page_set_->clear();  // 清空索引删除页面集合
    }
};