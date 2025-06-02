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
 * 系统支持多种并发控制算法：
 * - 两阶段封锁(2PL)
 * - 基本时间戳排序(BASIC_TO)
 * - 多版本并发控制(MVCC)
 */
enum class ConcurrencyMode { TWO_PHASE_LOCKING = 0, BASIC_TO, MVCC };

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
    UndoLink prev_;            // 指向前一个版本的链接
    bool in_progress_{false};  // 标记该版本是否正在被修改

    friend auto operator==(const VersionUndoLink &a, const VersionUndoLink &b) {
        return a.prev_ == b.prev_ && a.in_progress_ == b.in_progress_;
    }

    friend auto operator!=(const VersionUndoLink &a, const VersionUndoLink &b) { return !(a == b); }

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
     * @description: 获取事务ID为txn_id的事务对象
     * @return {Transaction*} 事务对象的指针
     * @param {txn_id_t} txn_id 事务ID
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
     * @brief 更新一个撤销链接，该链接将表堆元组与第一个撤销日志连接起来。
     * 在更新之前，将调用 `check` 函数以确保有效性。
     */
    bool UpdateUndoLink(Rid rid, std::optional<UndoLink> prev_link,
                        std::function<bool(std::optional<UndoLink>)> &&check = nullptr);

    /**
     * @brief 更新一个撤销链接，该链接将表堆元组与第一个撤销日志连接起来。
     * 在更新之前，将调用 `check` 函数以确保有效性。
     */
    bool UpdateVersionLink(Rid rid, std::optional<VersionUndoLink> prev_version,
                           std::function<bool(std::optional<VersionUndoLink>)> &&check = nullptr);

    /** @brief 获取表堆元组的第一个撤销日志。 */
    std::optional<UndoLink> GetUndoLink(Rid rid);

    /** @brief 获取表堆元组的第一个撤销日志。*/
    std::optional<VersionUndoLink> GetVersionLink(Rid rid);

    /** @brief 访问事务撤销日志缓冲区并获取撤销日志。如果事务不存在，返回 nullopt。
     * 如果索引超出范围仍然会抛出异常。 */
    std::optional<UndoLog> GetUndoLogOptional(UndoLink link);

    /** @brief 访问事务撤销日志缓冲区并获取撤销日志。除非访问当前事务缓冲区，
     * 否则应该始终调用此函数以获取撤销日志，而不是手动检索事务 shared_ptr 并访问缓冲区。 */
    UndoLog GetUndoLog(UndoLink link);

    /** @brief 获取系统中的最低读时间戳。 */
    timestamp_t GetWatermark();

    /** @brief 垃圾回收。仅在所有事务都未访问时调用。 */
    void GarbageCollection();

    struct PageVersionInfo {
        std::shared_mutex mutex_;
        /** 存储所有槽的先前版本信息。注意：不要使用 `[x]` 来访问它，因为
         * 即使不存在也会创建新元素。请使用 `find` 来代替。
         */
        std::unordered_map<slot_offset_t, VersionUndoLink> prev_version_;
    };

    /** 保护版本信息 */
    std::shared_mutex version_info_mutex_;
    /** 存储表堆中每个元组的先前版本。 */
    std::unordered_map<page_id_t, std::shared_ptr<PageVersionInfo>> version_info_;

   private:
    ConcurrencyMode concurrency_mode_;            // 当前使用的并发控制模式
    std::atomic<txn_id_t> next_txn_id_{0};        // 事务ID生成器
    std::atomic<timestamp_t> next_timestamp_{0};  // 事务时间戳生成器
    std::mutex latch_;                            // 保护事务相关数据结构的互斥锁
    SmManager *sm_manager_;                       // 系统管理器指针
    LockManager *lock_manager_;                   // 锁管理器指针

    // MVCC相关成员
    std::atomic<timestamp_t> last_commit_ts_{0};  // 最近提交事务的时间戳
    Watermark running_txns_{0};                   // 活跃事务的时间水位线，用于垃圾回收
};