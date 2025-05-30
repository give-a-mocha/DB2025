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
#include "transaction/txn_defs.h"
#include "record/rm_defs.h"

/** 表示此tuple的前一个版本的链接，用于多版本并发控制 */
struct UndoLink {
  /* 指向前一个版本所在的事务ID */
  txn_id_t prev_txn_{INVALID_TXN_ID};
  /* 前一个版本在对应事务的撤销日志中的索引位置 */
  int prev_log_idx_{0};

  /* 比较两个UndoLink是否相等（用于测试和调试） */
  friend auto operator==(const UndoLink &a, const UndoLink &b) {
    return a.prev_txn_ == b.prev_txn_ && a.prev_log_idx_ == b.prev_log_idx_;
  }

  /* 比较两个UndoLink是否不相等 */
  friend auto operator!=(const UndoLink &a, const UndoLink &b) { return !(a == b); }

  /* 检查当前的撤销链接是否有效（指向一个实际存在的版本） */
  bool IsValid() { return prev_txn_ != INVALID_TXN_ID; }
};

/** 撤销日志结构，存储数据修改前的状态以支持事务回滚和多版本并发控制 */
struct UndoLog {
  /* 标记此日志是否记录了删除操作 */
  bool is_deleted_;
  /* 用布尔数组标记哪些字段被修改了（true表示对应索引的字段被修改） */
  std::vector<bool> modified_fields_;
  /* 存储修改后的字段值数组 */
  std::vector<Value> tuple_;
  /* 指向实际元组记录的指针（用于测试） */
  RmRecord* tuple_test_;
  /* 此撤销日志的时间戳，用于确定可见性 */
  timestamp_t ts_{INVALID_TS};
  /* 指向该元组更早版本的链接，形成版本链 */
  UndoLink prev_version_{};
};


/** 
 * 事务类，负责管理数据库事务的执行
 * 包含事务的状态、隔离级别、写集合、锁集合以及撤销日志等
 */
class Transaction {
   public:
    /* 构造函数，创建一个新事务 */
    explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::SERIALIZABLE)
        : state_(TransactionState::DEFAULT), isolation_level_(isolation_level), txn_id_(txn_id) {
        /* 初始化事务的写集合（记录所有写操作） */
        write_set_ = std::make_shared<std::deque<WriteRecord *>>();
        /* 初始化事务持有的锁集合 */
        lock_set_ = std::make_shared<std::unordered_set<LockDataId>>();
        /* 初始化事务中使用的索引页面集合（加锁的索引页面） */
        index_latch_page_set_ = std::make_shared<std::deque<Page *>>();
        /* 初始化事务中删除的索引页面集合 */
        index_deleted_page_set_ = std::make_shared<std::deque<Page*>>();
        /* 初始化日志序列号为无效值 */
        prev_lsn_ = INVALID_LSN;
        /* 记录当前线程ID，用于标识哪个线程在执行此事务 */
        thread_id_ = std::this_thread::get_id();
    }

    /* 默认析构函数 */
    ~Transaction() = default;

    /* 获取事务ID */
    inline txn_id_t get_transaction_id() { return txn_id_; }

    /* 获取执行事务的线程ID */
    inline std::thread::id get_thread_id() { return thread_id_; }

    /* 设置事务模式（显式事务还是隐式事务） */
    inline void set_txn_mode(bool txn_mode) { txn_mode_ = txn_mode; }
    /* 获取事务模式 */
    inline bool get_txn_mode() { return txn_mode_; }

    /* 设置事务的开始时间戳 */
    inline void set_start_ts(timestamp_t start_ts) { start_ts_ = start_ts; }
    /* 获取事务的开始时间戳 */
    inline timestamp_t get_start_ts() { return start_ts_; }

    /* 获取事务的隔离级别 */
    inline IsolationLevel get_isolation_level() { return isolation_level_; }

    /* 获取事务的当前状态 */
    inline TransactionState get_state() { return state_; }
    /* 设置事务的状态（如进行中、已提交、已中止等） */
    inline void set_state(TransactionState state) { state_ = state; }

    /* 获取前一个日志序列号（用于故障恢复） */
    inline lsn_t get_prev_lsn() { return prev_lsn_; }
    /* 设置前一个日志序列号 */
    inline void set_prev_lsn(lsn_t prev_lsn) { prev_lsn_ = prev_lsn; }

    /* 获取写集合，包含事务中所有的写操作记录 */
    inline std::shared_ptr<std::deque<WriteRecord *>> get_write_set() { return write_set_; }  
    /* 添加一个写记录到写集合 */
    inline void append_write_record(WriteRecord* write_record) { write_set_->push_back(write_record); }

    /* 获取被删除的索引页面集合 */
    inline std::shared_ptr<std::deque<Page*>> get_index_deleted_page_set() { return index_deleted_page_set_; }
    /* 添加一个被删除的索引页面 */
    inline void append_index_deleted_page(Page* page) { index_deleted_page_set_->push_back(page); }

    /* 获取索引加锁页面集合 */
    inline std::shared_ptr<std::deque<Page*>> get_index_latch_page_set() { return index_latch_page_set_; }
    /* 添加一个索引加锁页面 */
    inline void append_index_latch_page_set(Page* page) { index_latch_page_set_->push_back(page); }

    /* 获取事务持有的锁集合 */
    inline std::shared_ptr<std::unordered_set<LockDataId>> get_lock_set() { return lock_set_; }

    /* 获取读时间戳，用于确定读操作的可见性 */
    inline timestamp_t get_read_ts() const { return read_ts_; }
    /* 获取提交时间戳，用于确定事务提交的时刻 */
    inline timestamp_t get_commit_ts() const { return commit_ts_; }

    /** 
     * 修改已存在的撤销日志
     * 需要加锁以确保线程安全
     */
    inline auto ModifyUndoLog(int log_idx, UndoLog new_log) {
        std::scoped_lock<std::mutex> lck(latch_);
        undo_logs_[log_idx] = std::move(new_log);
      }

    /** 
     * 添加一个撤销日志并返回指向它的链接
     * 该链接可用于构建版本链
     */
    inline auto AppendUndoLog(UndoLog log) -> UndoLink {
        std::scoped_lock<std::mutex> lck(latch_);
        undo_logs_.emplace_back(std::move(log));
        return {txn_id_, static_cast<int>(undo_logs_.size() - 1)};
      }

    /**
     * 获取指定位置的撤销日志
     * 线程安全的操作
     */
    inline auto GetUndoLog(size_t log_id) -> UndoLog {
        std::scoped_lock<std::mutex> lck(latch_);
        return undo_logs_[log_id];
      }

    /** 
     * 获取撤销日志的总数量
     * 用于调试和监控
     */
    inline auto GetUndoLogNum() -> size_t {
        std::scoped_lock<std::mutex> lck(latch_);
        return undo_logs_.size();
      }

   private:
    bool txn_mode_;                   // 事务模式标志：true表示显式事务，false表示隐式事务（单条SQL语句）
    TransactionState state_;          // 事务当前状态（默认、进行中、已提交、已中止等）
    IsolationLevel isolation_level_;  // 事务的隔离级别（读未提交、读已提交、可重复读、可串行化）
    std::thread::id thread_id_;       // 执行该事务的线程ID，用于并发控制
    lsn_t prev_lsn_;                  // 前一个日志序列号，用于WAL（预写日志）和故障恢复
    txn_id_t txn_id_;                 // 事务的唯一标识符
    timestamp_t start_ts_;            // 事务的开始时间戳，用于并发控制

    std::shared_ptr<std::deque<WriteRecord *>> write_set_;  // 事务的写集合，记录所有写操作
    std::shared_ptr<std::unordered_set<LockDataId>> lock_set_;  // 事务持有的所有锁，用于并发控制
    std::shared_ptr<std::deque<Page*>> index_latch_page_set_;  // 事务中已加锁的索引页面集合
    std::shared_ptr<std::deque<Page*>> index_deleted_page_set_;  // 事务中已删除的索引页面集合

    std::atomic<timestamp_t> read_ts_{0};  // 读时间戳，原子变量确保线程安全
    /** 提交时间戳，事务提交时分配，用于确定对其他事务的可见性 */
    std::atomic<timestamp_t> commit_ts_{INVALID_TS}; 
  
    /**
    * 存储事务的所有撤销日志
    * 其他事务/表通过(txn_id, index)对引用这些日志
    * 因此只能追加或修改，不能删除其中的元素
    */
    std::vector<UndoLog> undo_logs_;
  
    /** 保护撤销日志访问的互斥锁，确保多线程环境下的安全访问 */
    std::mutex latch_;
};