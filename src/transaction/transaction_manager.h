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

/* 系统采用的并发控制算法，当前题目中要求两阶段封锁并发控制算法 */
enum class ConcurrencyMode {
    TWO_PHASE_LOCKING = 0,  // 两阶段封锁协议
    BASIC_TO,               // 基本时间戳排序
    MVCC                    // 多版本并发控制
};

/// 版本链中的第一个撤销链接，将表堆元组链接到撤销日志
struct VersionUndoLink {
    /** 版本链中的下一个版本。 */
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
     * @brief 从可选的 UndoLink 创建一个 VersionUndoLink。
     * @param undo_link 可选的 UndoLink。
     * @return 一个可选的 VersionUndoLink，如果 undo_link 为 nullopt，则为 std::nullopt。
     */
    inline static std::optional<VersionUndoLink> FromOptionalUndoLink(std::optional<UndoLink> undo_link) {
        if (undo_link.has_value()) {
            return VersionUndoLink{*undo_link};
        }
        return std::nullopt;
    }
};

class TransactionManager {
public:
    // 全局事务表，存放事务ID与事务对象的映射关系
    static std::unordered_map<txn_id_t, Transaction *> txn_map;
    // 保护事务表的读写锁
    std::shared_mutex txn_map_mutex_;

    /**
     * @brief 为 MVCC 存储单个页面内所有槽的版本信息。
     */
    struct PageVersionInfo {
        /** @brief 保护此页面版本信息访问的互斥锁。 */
        std::shared_mutex mutex_;
        /**
         * @brief 将槽偏移量映射到其对应的前一个版本链接。
         * @note 请使用 `find()` 而不是 `[]` 进行访问，以避免创建默认元素。
         */
        std::unordered_map<slot_offset_t, VersionUndoLink> prev_version_;
    };

    /** 保护版本信息 */
    std::shared_mutex version_info_mutex_;
    /** 存储表堆中每个元组的先前版本。 */
    std::unordered_map<page_id_t, std::shared_ptr<PageVersionInfo>> version_info_;
private:
    // 事务使用的并发控制算法，目前只需要考虑2PL
    ConcurrencyMode concurrency_mode_;

    // 用于分发事务ID
    std::atomic<txn_id_t> next_txn_id_{0};

    // 用于分发事务时间戳
    std::atomic<timestamp_t> next_timestamp_{0};

    // 用于txn_map的并发
    std::mutex latch_;

    SmManager *sm_manager_;

    LockManager *lock_manager_;

    // 最后提交的时间戳,仅用于MVCC
    std::atomic<timestamp_t> last_commit_ts_{0};

    /// 存储所有正在运行事务的读取时间戳，以便于垃圾回收，仅用于MVCC
    Watermark running_txns_{0};

public:
    explicit TransactionManager(LockManager *lock_manager, SmManager *sm_manager,
                                ConcurrencyMode concurrency_mode = ConcurrencyMode::TWO_PHASE_LOCKING);

    ~TransactionManager() = default;

    Transaction *begin(Transaction *txn, LogManager *log_manager);

    void commit(Transaction *txn, LogManager *log_manager);

    void abort(Context *context, LogManager *log_manager);

    ConcurrencyMode get_concurrency_mode() { return concurrency_mode_; }

    void set_concurrency_mode(ConcurrencyMode concurrency_mode) { concurrency_mode_ = concurrency_mode; }

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


private:

    static void record_link_management(LogRecord *log_record, LogManager *log_manager, Transaction *txn);

    void delete_index_record(TabMeta tab_,RmRecord* rec, Rid rid, Context *context);

    void insert_index_record(TabMeta tab_,RmRecord* rec, Rid rid, Context *context);
};