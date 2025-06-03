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
#include <functional>
#include <optional>
#include <shared_mutex>
#include <unordered_map>

#include "common/exception.h"
#include "concurrency/lock_manager.h"
#include "recovery/log_manager.h"
#include "system/sm_manager.h"
#include "transaction.h"
#include "watermark.h"

/**
 * @brief 并发控制算法的枚举类型
 *
 * 系统支持以下并发控制算法：
 * TWO_PHASE_LOCKING: 两阶段封锁协议
 * - 增长阶段：事务只能获取锁，不能释放锁
 * - 缩减阶段：事务只能释放锁，不能获取锁
 * - 保证可串行化
 *
 * BASIC_TO: 基本时间戳排序
 * - 使用时间戳确定事务执行顺序
 * - 较早的时间戳优先执行
 * - 检测读写冲突
 *
 * MVCC: 多版本并发控制
 * - 为数据维护多个版本
 * - 读操作不阻塞写操作
 * - 支持快照隔离
 */
enum class ConcurrencyMode {
    TWO_PHASE_LOCKING = 0,  // 两阶段封锁协议
    BASIC_TO,               // 基本时间戳排序
    MVCC                    // 多版本并发控制
};

/**
 * @brief 版本链接结构，用于MVCC实现
 *
 * 该结构维护表记录的版本信息，将表记录与其历史版本（撤销日志）关联起来。
 * 主要用于：
 * - 维护记录的多个历史版本
 * - 支持事务的回滚操作
 * - 实现快照隔离
 */
struct VersionUndoLink {
    UndoLink prev_;  // 指向记录前一个版本的撤销日志链接
                     // 用于构建版本链，支持MVCC和事务回滚

    bool in_progress_{false};  // 版本状态标记
                               // true: 该版本正在被某个事务修改
                               // false: 该版本是稳定的

    friend auto operator==(const VersionUndoLink &a, const VersionUndoLink &b) {
        return a.prev_ == b.prev_ && a.in_progress_ == b.in_progress_;
    }

    friend auto operator!=(const VersionUndoLink &a, const VersionUndoLink &b) { return !(a == b); }

    /**
     * @brief Creates a VersionUndoLink from an optional UndoLink.
     * @param undo_link The optional UndoLink.
     * @return An optional VersionUndoLink, or std::nullopt if undo_link is nullopt.
     */
    inline static std::optional<VersionUndoLink> FromOptionalUndoLink(std::optional<UndoLink> undo_link) {
        if (undo_link.has_value()) {
            return VersionUndoLink{*undo_link};
        }
        return std::nullopt;
    }
};

/**
 * @brief 事务管理器类
 *
 * 负责管理数据库中的所有事务，主要功能包括：
 * 1. 事务的创建、提交和回滚
 * 2. 并发控制
 * 3. 版本管理（MVCC模式）
 * 4. 事务恢复
 */
class TransactionManager {
   public:
    /**
     * @brief 构造函数
     * @param lock_manager 锁管理器指针
     * @param sm_manager 系统管理器指针
     * @param concurrency_mode 并发控制模式，默认为2PL
     */
    explicit TransactionManager(LockManager *lock_manager, SmManager *sm_manager,
                                ConcurrencyMode concurrency_mode = ConcurrencyMode::TWO_PHASE_LOCKING);

    ~TransactionManager() = default;

    /**
     * @brief 开始一个新事务
     * @param txn 事务对象指针
     * @param log_manager 日志管理器指针
     * @return 初始化后的事务对象指针
     */
    Transaction *begin(Transaction *txn, LogManager *log_manager);

    /**
     * @brief 提交事务
     * @param txn 要提交的事务
     * @param log_manager 日志管理器指针
     */
    void commit(Transaction *txn, LogManager *log_manager);

    /**
     * @brief 回滚事务
     * @param txn 要回滚的事务
     * @param log_manager 日志管理器指针
     */
    void abort(Transaction *txn, LogManager *log_manager);

    /**
     * @brief 获取当前的并发控制模式
     */
    ConcurrencyMode get_concurrency_mode() { return concurrency_mode_; }

    /**
     * @brief 设置并发控制模式
     */
    void set_concurrency_mode(ConcurrencyMode concurrency_mode) { concurrency_mode_ = concurrency_mode; }

    /**
     * @brief 获取锁管理器指针
     */
    LockManager *get_lock_manager() { return lock_manager_; }

    /**
     * @brief Retrieves the Transaction object associated with the given transaction ID.
     * @param txn_id The ID of the transaction to retrieve.
     * @return A pointer to the Transaction object, or nullptr if txn_id is INVALID_TXN_ID.
     * @note Asserts that the transaction exists and belongs to the current thread.
     */
    Transaction *get_transaction(txn_id_t txn_id) {
        if (txn_id == INVALID_TXN_ID) return nullptr;

        std::unique_lock<std::mutex> lock(latch_);
        assert(TransactionManager::txn_map.find(txn_id) != TransactionManager::txn_map.end());
        auto *res = TransactionManager::txn_map[txn_id];
        lock.unlock();
        assert(res != nullptr);
        assert(res->get_thread_id() == std::this_thread::get_id());

        return res;
    }

    /** @brief 全局事务表，维护所有活跃事务 */
    static std::unordered_map<txn_id_t, Transaction *> txn_map;
    /** @brief 保护事务表的读写锁 */
    std::shared_mutex txn_map_mutex_;

    /** ------------------------以下为MVCC相关接口------------------------------------------*/

    /**
     * @brief Updates the undo link for a tuple, connecting it to its first undo log.
     * Optionally performs a check before updating.
     * @param rid The record ID of the tuple.
     * @param prev_link The previous undo link to set.
     * @param check An optional function to validate the current link before updating.
     * @return True if the update was successful, false otherwise.
     */
    bool UpdateUndoLink(Rid rid, std::optional<UndoLink> prev_link,
                        std::function<bool(std::optional<UndoLink>)> &&check = nullptr);

    /**
     * @brief Updates the version link for a tuple, used in MVCC.
     * Optionally performs a check before updating.
     * @param rid The record ID of the tuple.
     * @param prev_version The previous version link to set.
     * @param check An optional function to validate the current version link before updating.
     * @return True if the update was successful, false otherwise.
     */
    bool UpdateVersionLink(Rid rid, std::optional<VersionUndoLink> prev_version,
                           std::function<bool(std::optional<VersionUndoLink>)> &&check = nullptr);

    /**
     * @brief Gets the first undo link associated with a tuple.
     * @param rid The record ID of the tuple.
     * @return An optional UndoLink.
     */
    std::optional<UndoLink> GetUndoLink(Rid rid);

    /**
     * @brief Gets the first version link associated with a tuple (for MVCC).
     * @param rid The record ID of the tuple.
     * @return An optional VersionUndoLink.
     */
    std::optional<VersionUndoLink> GetVersionLink(Rid rid);

    /**
     * @brief Retrieves an undo log from the transaction's undo buffer.
     * @param link The UndoLink pointing to the desired log.
     * @return An optional UndoLog. Returns nullopt if the transaction doesn't exist.
     * @throws std::out_of_range if the link's index is out of bounds.
     */
    std::optional<UndoLog> GetUndoLogOptional(UndoLink link);

    /**
     * @brief Retrieves an undo log from the transaction's undo buffer.
     * This is the preferred way to access undo logs unless accessing the current transaction's buffer.
     * @param link The UndoLink pointing to the desired log.
     * @return The UndoLog.
     * @throws TransactionAbortException if the transaction doesn't exist or other errors occur.
     */
    UndoLog GetUndoLog(UndoLink link);

    /**
     * @brief Gets the lowest read timestamp (watermark) among all running transactions.
     * @return The current watermark timestamp.
     */
    timestamp_t GetWatermark();

    /** @brief Performs garbage collection for old versions in MVCC. Should only be called when no transactions are active. */
    void GarbageCollection();

    /**
     * @brief Stores version information for all slots within a single page for MVCC.
     */
    struct PageVersionInfo {
        /** @brief Mutex protecting access to this page's version information. */
        std::shared_mutex mutex_;
        /**
         * @brief Maps slot offsets to their corresponding previous version link.
         * @note Use `find()` instead of `[]` for access to avoid creating default elements.
         */
        std::unordered_map<slot_offset_t, VersionUndoLink> prev_version_;
    };

    /** 保护版本信息 */
    std::shared_mutex version_info_mutex_;
    /** 存储表堆中每个元组的先前版本。 */
    std::unordered_map<page_id_t, std::shared_ptr<PageVersionInfo>> version_info_;

   private:
    /** @brief 当前使用的并发控制模式，决定使用哪种并发控制策略 */
    ConcurrencyMode concurrency_mode_;

    /** @brief 事务ID生成器，保证每个事务有唯一的ID
     *  使用atomic保证多线程下的原子递增 */
    std::atomic<txn_id_t> next_txn_id_{0};

    /** @brief 事务时间戳生成器，用于MVCC和时间戳排序
     *  使用atomic保证多线程下的原子递增 */
    std::atomic<timestamp_t> next_timestamp_{0};

    /** @brief 保护事务相关数据结构的互斥锁
     *  用于保护事务表等共享数据结构的并发访问 */
    std::mutex latch_;

    /** @brief 系统管理器指针，用于访问表、索引等系统资源 */
    SmManager *sm_manager_;

    /** @brief 锁管理器指针，用于实现2PL等基于锁的并发控制 */
    LockManager *lock_manager_;

    /** @brief MVCC相关成员 */
    /** @brief 最近提交事务的时间戳，用于实现快照隔离 */
    std::atomic<timestamp_t> last_commit_ts_{0};

    /** @brief 活跃事务的时间水位线
     *  用于垃圾回收：低于水位线的版本可以安全删除
     *  因为它们不会再被任何活跃事务访问 */
    Watermark running_txns_{0};
};