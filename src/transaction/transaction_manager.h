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

enum class ConcurrencyMode {
    TWO_PHASE_LOCKING = 0,  // 两阶段封锁协议
    BASIC_TO,               // 基本时间戳排序
    MVCC                    // 多版本并发控制
};

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
    // 全局事务表，维护所有活跃事务
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
    // 当前使用的并发控制模式，决定使用哪种并发控制策略
    ConcurrencyMode concurrency_mode_;

    // 事务ID生成器，保证每个事务有唯一的ID 使用atomic保证多线程下的原子递增
    std::atomic<txn_id_t> next_txn_id_{0};

    // 事务时间戳生成器，用于MVCC和时间戳排序 使用atomic保证多线程下的原子递增
    std::atomic<timestamp_t> next_timestamp_{0};

    // 保护事务相关数据结构的互斥锁 用于保护事务表等共享数据结构的并发访问
    std::mutex latch_;

    SmManager *sm_manager_;

    LockManager *lock_manager_;

    // MVCC相关成员
    // 最近提交事务的时间戳，用于实现快照隔离
    std::atomic<timestamp_t> last_commit_ts_{0};

    // 活跃事务的时间水位线
    // 用于垃圾回收：低于水位线的版本可以安全删除
    // 因为它们不会再被任何活跃事务访问
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
     * @brief 更新元组的撤销链接，将其连接到其第一个撤销日志。
     * 可选地在更新前执行检查。
     * @param rid 元组的记录ID。
     * @param prev_link 要设置的前一个撤销链接。
     * @param check 在更新前验证当前链接的可选函数。
     * @return 如果更新成功则返回 true，否则返回 false。
     */
    bool UpdateUndoLink(Rid rid, std::optional<UndoLink> prev_link,
                        std::function<bool(std::optional<UndoLink>)> &&check = nullptr);

    /**
     * @brief 更新元组的版本链接，用于 MVCC。
     * 可选地在更新前执行检查。
     * @param rid 元组的记录ID。
     * @param prev_version 要设置的前一个版本链接。
     * @param check 在更新前验证当前版本链接的可选函数。
     * @return 如果更新成功则返回 true，否则返回 false。
     */
    bool UpdateVersionLink(Rid rid, std::optional<VersionUndoLink> prev_version,
                           std::function<bool(std::optional<VersionUndoLink>)> &&check = nullptr);

    /**
     * @brief 获取与元组关联的第一个撤销链接。
     * @param rid 元组的记录ID。
     * @return 一个可选的 UndoLink。
     */
    std::optional<UndoLink> GetUndoLink(Rid rid);

    /**
     * @brief 获取与元组关联的第一个版本链接 (用于 MVCC)。
     * @param rid 元组的记录ID。
     * @return 一个可选的 VersionUndoLink。
     */
    std::optional<VersionUndoLink> GetVersionLink(Rid rid);

    /**
     * @brief 从事务的撤销缓冲区中检索撤销日志。
     * @param link 指向所需日志的 UndoLink。
     * @return 一个可选的 UndoLog。如果事务不存在，则返回 nullopt。
     * @throws std::out_of_range 如果链接的索引超出范围。
     */
    std::optional<UndoLog> GetUndoLogOptional(UndoLink link);

    /**
     * @brief 从事务的撤销缓冲区中检索撤销日志。
     * 这是访问撤销日志的首选方法，除非访问当前事务的缓冲区。
     * @param link 指向所需日志的 UndoLink。
     * @return UndoLog。
     * @throws TransactionAbortException 如果事务不存在或发生其他错误。
     */
    UndoLog GetUndoLog(UndoLink link);

    /**
     * @brief 获取所有正在运行的事务中的最低读取时间戳 (水位线)。
     * @return 当前水位线时间戳。
     */
    timestamp_t GetWatermark();

    /** @brief 在 MVCC 中为旧版本执行垃圾回收。仅应在没有活动事务时调用。 */
    void GarbageCollection();


private:

    static void record_link_management(LogRecord *log_record, LogManager *log_manager, Transaction *txn);

    void delete_index_record(TabMeta tab_,RmRecord* rec, Rid rid, Context *context);

    void insert_index_record(TabMeta tab_,RmRecord* rec, Rid rid, Context *context);
};