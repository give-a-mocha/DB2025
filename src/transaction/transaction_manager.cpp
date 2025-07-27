/**
 * @file transaction_manager.cpp
 * @author RMDB Development Team
 * @brief 事务管理器实现文件
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * 该文件实现了事务管理器的核心功能，包括：
 * - 事务的创建、提交和回滚
 * - 并发控制（2PL、MVCC）
 * - 日志管理与恢复
 * - 锁管理
 *
 * 事务管理器是数据库系统的核心组件之一，负责保证：
 * 1. 原子性(Atomicity)：事务要么完全执行，要么完全不执行
 * 2. 一致性(Consistency)：事务执行前后数据库状态保持一致
 * 3. 隔离性(Isolation)：并发事务互不干扰
 * 4. 持久性(Durability)：已提交事务的修改永久保存
 */

#include "transaction_manager.h"

#include "common/print.hpp"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"
#include "execution/executor_abstract.h"

extern SmManager sm_manager;
extern LockManager lock_manager;

std::unordered_map<txn_id_t, Transaction*> TransactionManager::txn_map = {};

/**
 * @brief 事务管理器构造函数
 *
 * @param lock_manager 锁管理器指针，用于并发控制
 * @param sm_manager 系统管理器指针，用于访问数据库资源
 * @param concurrency_mode 并发控制模式，默认为两阶段封锁
 */
TransactionManager::TransactionManager(ConcurrencyMode concurrency_mode) {
    concurrency_mode_ = concurrency_mode;
}

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction* TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    // 如果需要支持MVCC请在上述过程中添加代码

    if (txn == nullptr) {
        // 创建新事务，并分配递增的事务ID
        txn = new Transaction(get_next_txn_id());
    }

    // 将当前事务添加到全局事务映射表中，便于后续通过事务ID查找
    std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);
    txn_map.emplace(txn->get_transaction_id(), txn);
    txn->set_read_ts(last_commit_ts_);  // 设置读取时间戳该事务早的最后一次提交时间戳
    txn->set_txn_mode(false);
    txn->set_state(TransactionState::DEFAULT);
    // log_manager->add_begin_log(txn->get_transaction_id());

    // running_txns_.AddTxn(txn->get_read_ts());  // 添加事务到水位线
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    // 如果需要支持MVCC请在上述过程中添加代码

    std::scoped_lock<std::mutex> lock(commit_mutex_);

    // FOCC Validation Phase 1: Collect conditions under a shared lock
    std::unordered_map<int, std::vector<Condition>> conditions_by_fd;
    lock_manager.lock_gap_set_shared();

    std::unordered_set<int> processed_fds;
    for (const auto& write_record : *txn->get_write_set()) {
        const auto& table_name = write_record->GetTableName();
        std::unique_ptr<RmFileHandle>& fh_ = sm_manager.fhs_.at(table_name);
        int fd = fh_->GetFd();
        if (processed_fds.find(fd) == processed_fds.end()) {
            conditions_by_fd[fd] = lock_manager.get_gap_condition(fd, txn);
            processed_fds.insert(fd);
        }
    }
    lock_manager.unlock_gap_set_shared();

    // FOCC Validation Phase 2: Fetch records and evaluate lock-free
    for (size_t i = 0; i < txn->get_write_set()->size(); ++i) {
        const auto& write_record = (*txn->get_write_set())[i];
        const auto& table_name = write_record->GetTableName();
        const auto& rid = write_record->GetRid();
        TabMeta& tab_ = sm_manager.db_.get_table(table_name);
        std::unique_ptr<RmFileHandle>& fh_ = sm_manager.fhs_.at(table_name);
        int fd = fh_->GetFd();

        if (conditions_by_fd.find(fd) == conditions_by_fd.end() || conditions_by_fd.at(fd).empty()) {
            continue;
        }
        const auto& conds = conditions_by_fd.at(fd);

        auto [base_meta, rec] = fh_->get_record(rid);
        auto pre_link = txn->GetUndoLog(i)->prev_version_;
        bool is_deleted = base_meta.is_deleted_;

        if (is_deleted && pre_link.IsValid() && !GetUndoLog(pre_link)->is_deleted_) {
            is_deleted = false;
        }
        if (is_deleted) {
            continue;
        }

        if (AbstractExecutor::eval_conds(tab_.cols, conds, rec)) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
        }
    }

    txn->set_state(TransactionState::COMMITTED);
    txn->set_commit_ts(get_next_timestamp());                                       // 设置提交时间戳
    txn->CommitUndoLogs();                                                          // 提交事务的撤销日志
    last_commit_ts_.store(std::max(last_commit_ts_.load(), txn->get_commit_ts()));  // 更新最后提交时间戳

    std::shared_ptr<std::unordered_set<LockDataId>> lock_set = txn->get_lock_set();

    auto lock_set_copy = *lock_set;  // 复制锁集合以避免迭代时修改
    for (const LockDataId& lock : lock_set_copy) {
        lock_manager.unlock(txn, lock);
    }

    auto lock_gap_set = txn->get_lock_gap_set();
    auto lock_gap_set_copy = *lock_gap_set;  // 复制间隙锁集合以避免迭代时修改
    for (const int& tab_fd : lock_gap_set_copy) {
        lock_manager.unlock_gap(txn, tab_fd);
    }

    // 清空事务相关的集合
    txn->clear_lock_set();

    // log_manager->add_commit_log(txn->get_transaction_id());
    // log_manager->flush_log_to_disk();

    // running_txns_.UpdateCommitTs(txn->get_commit_ts());  // 更新水位线的提交时间戳
    // running_txns_.RemoveTxn(txn->get_read_ts());         // 从水位线中移除事务
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Context* context, LogManager* log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    // 如果需要支持MVCC请在上述过程中添加代码

    Transaction* txn = context->txn_;

    auto write_set = txn->get_write_set();
    for (size_t i = 0; i < write_set->size(); ++i) {
        const auto table_name = (*write_set)[i]->GetTableName();
        const auto rid = (*write_set)[i]->GetRid();
        const UndoLog* undolog = txn->GetUndoLog(i);
        std::unique_ptr<RmFileHandle>& fh_ = sm_manager.fhs_.at(table_name);

        DeleteUndoLink(fh_->GetFd(), rid, context->txn_);
        if (undolog->is_deleted_) {
            TupleMeta delete_meta;
            delete_meta.is_deleted_ = true;  // 插入的记录不标记为
            fh_->update_tuple_meta(rid, delete_meta);
            // log_manager->add_delete_log(txn->get_transaction_id(), std::make_unique<RmRecord>(undolog->record_), rid,
            // table_name);
        } else {
            TupleMeta base_meta;
            auto pre_link = undolog->prev_version_;
            if (!pre_link.IsValid()) {
                base_meta = {0, false};
            } else {
                const UndoLog* pre_log = GetUndoLog(pre_link);
                TupleMeta base_meta(pre_log->ts_, false);
            }
            fh_->insert_record_force(rid, base_meta, undolog->record_.data);
            // log_manager->add_insert_log(txn->get_transaction_id(), std::make_unique<RmRecord>(undolog->record_), rid,
            // table_name);
        }
    }
    txn->get_write_set()->clear();  // 清空写集合

    std::shared_ptr<std::unordered_set<LockDataId>> lock_set = txn->get_lock_set();
    auto lock_set_copy = *lock_set;  // 复制锁集合以避免迭代时修改
    for (const LockDataId& lock : lock_set_copy) {
        lock_manager.unlock(txn, lock);
    }

    auto lock_gap_set = txn->get_lock_gap_set();
    auto lock_gap_set_copy = *lock_gap_set;  // 复制间隙锁集合以避免迭代时修改
    for (const int& tab_fd : lock_gap_set_copy) {
        lock_manager.unlock_gap(txn, tab_fd);
    }

    txn->clear_lock_set();
    txn->set_state(TransactionState::ABORTED);
    // log_manager->add_abort_log(txn->get_transaction_id());

    // running_txns_.RemoveTxn(txn->get_read_ts());  // 从水位线中移除事务
}

auto TransactionManager::GetVersionInfoShard(const PageId& page_id) -> PageVersionInfoShard& {
    return version_info_shards_[std::hash<PageId>{}(page_id) % VERSION_INFO_SHARDS];
}

/**
 * @brief 更新一个撤销链接，该链接将表堆元组与第一个撤销日志连接起来。
 * 在更新之前，将调用 `check` 函数以确保有效性。
 */
bool TransactionManager::UpdateUndoLink(const int& fd, Rid rid, UndoLink link) {
    PageId page_id{fd, rid.page_no};
    auto& shard = GetVersionInfoShard(page_id);
    std::unique_lock<std::shared_mutex> lock(shard.mutex_);
    auto it = shard.version_info_.find(page_id);
    if (it == shard.version_info_.end()) {
        // 如果没有找到对应的版本信息，则创建一个新的
        auto new_version_info = std::make_shared<PageVersionInfo>();
        shard.version_info_[page_id] = new_version_info;
        it = shard.version_info_.find(page_id);
    }
    auto& version_info = it->second;
    // auto version_info = it->second;
    // lock.unlock();
    std::unique_lock<std::shared_mutex> version_lock(version_info->mutex_);
    // 更新版本链接
    auto& prev_version_map = version_info->prev_version_;
    prev_version_map[rid.slot_no] = link;
    return true;  // 更新成功，返回 true
}

void TransactionManager::DeleteUndoLink(const int& fd, Rid rid, Transaction* txn) {
    PageId page_id{fd, rid.page_no};
    auto& shard = GetVersionInfoShard(page_id);
    // 获取对应的版本信息
    std::unique_lock<std::shared_mutex> lock(shard.mutex_);
    auto it = shard.version_info_.find(page_id);
    if (it == shard.version_info_.end()) {
        return;
    }
    auto& version_info = it->second;
    // auto version_info = it->second;
    // lock.unlock();
    std::unique_lock<std::shared_mutex> version_lock(version_info->mutex_);
    // 更新版本链接
    auto& prev_version_map = version_info->prev_version_;
    auto prev_version_it = prev_version_map.find(rid.slot_no);
    if (prev_version_it != prev_version_map.end()) {
        UndoLink undolink = prev_version_it->second;
        undolink = GetUndoLog(undolink)->prev_version_;
        if (undolink.IsValid()) {
            // 如果撤销链接有效，则更新版本链接
            prev_version_it->second = undolink;
        } else {
            // 如果撤销链接无效，则删除该版本链接
            prev_version_map.erase(prev_version_it);
        }
        return;
    }
    return;
}

/** @brief 获取表堆元组的第一个撤销日志。 */
UndoLink TransactionManager::GetUndoLink(const int& fd, Rid rid) {
    PageId page_id{fd, rid.page_no};
    auto& shard = GetVersionInfoShard(page_id);
    std::shared_lock<std::shared_mutex> lock(shard.mutex_);
    auto it = shard.version_info_.find(page_id);
    if (it == shard.version_info_.end()) {
        return UndoLink{};  // 如果没有找到对应的版本信息，则返回空的 UndoLink
    }
    auto& version_info = it->second;
    // auto version_info = it->second;
    // lock.unlock();
    std::shared_lock<std::shared_mutex> version_lock(version_info->mutex_);
    auto prev_version_it = version_info->prev_version_.find(rid.slot_no);
    if (prev_version_it == version_info->prev_version_.end()) {
        return UndoLink{};  // 如果没有找到对应的版本信息，则返回空的 UndoLink
    }
    return prev_version_it->second;  // 返回前一个版本链接
}

/** @brief 访问事务撤销日志缓冲区并获取撤销日志。如果事务不存在，返回 nullptr。
 * 如果索引超出范围仍然会抛出异常。 */
const UndoLog* TransactionManager::GetUndoLogOptional(UndoLink link) {
    // 检查事务是否存在
    if (link.prev_txn_ == INVALID_TXN_ID) return nullptr;
    std::shared_lock<std::shared_mutex> lock(txn_map_mutex_);
    auto it = TransactionManager::txn_map.find(link.prev_txn_);
    if (it == TransactionManager::txn_map.end()) {
        return nullptr;  // 如果事务不存在，则返回 nullptr
    }
    auto txn = it->second;

    // 检查撤销日志索引是否有效
    if (link.prev_log_idx_ < 0 || static_cast<size_t>(link.prev_log_idx_) >= txn->GetUndoLogNum()) {
        throw RangeError("Invalid undo log index: " + std::to_string(link.prev_log_idx_));
    }

    // 返回对应的撤销日志指针
    return txn->GetUndoLog(link.prev_log_idx_);
}

/** @brief 访问事务撤销日志缓冲区并获取撤销日志。除非访问当前事务缓冲区，
 * 否则应该始终调用此函数以获取撤销日志，而不是手动检索事务 shared_ptr 并访问缓冲区。 */
const UndoLog* TransactionManager::GetUndoLog(UndoLink link) {
    // 检查事务是否存在
    if (link.prev_txn_ == INVALID_TXN_ID) {
        throw RMDBError("Invalid transaction ID: " + std::to_string(link.prev_txn_));
    }
    std::shared_lock<std::shared_mutex> lock(txn_map_mutex_);
    return GetUndoLogWithoutLock(link);
}

const UndoLog* TransactionManager::GetUndoLogWithoutLock(UndoLink link) {
    auto it = TransactionManager::txn_map.find(link.prev_txn_);
    if (it == TransactionManager::txn_map.end()) {
        throw RMDBError("Transaction not found: " + std::to_string(link.prev_txn_));
    }
    auto txn = it->second;
    // 检查撤销日志索引是否有效
    if (link.prev_log_idx_ < 0 || static_cast<size_t>(link.prev_log_idx_) >= txn->GetUndoLogNum()) {
        throw RangeError("Invalid undo log index: " + std::to_string(link.prev_log_idx_));
    }

    // 返回对应的撤销日志
    return txn->GetUndoLog(link.prev_log_idx_);
}

/** @brief 获取系统中的最低读时间戳。 */
timestamp_t TransactionManager::GetWatermark() { return running_txns_.GetWatermark(); }

void TransactionManager::do_delete(Transaction* txn) {}

bool TransactionManager::is_transaction_expired(Transaction* txn, timestamp_t watermark) const {
    if (txn == nullptr) return false;

    TransactionState state = txn->get_state();
    if (state == TransactionState::COMMITTED && txn->get_commit_ts() <= watermark) {
        return true;
    }
    if (state == TransactionState::ABORTED && txn->get_read_ts() <= watermark) {
        return true;
    }
    return false;
}

void TransactionManager::cleanup_expired_versions(timestamp_t watermark) {
    for (auto& shard : version_info_shards_) {
        std::unique_lock<std::shared_mutex> lock(shard.mutex_);
        for (auto it = shard.version_info_.begin(); it != shard.version_info_.end();) {
            auto& version_info = it->second;
            std::unique_lock<std::shared_mutex> version_lock(version_info->mutex_);

            // 清理过期的版本链接
            for (auto version_it = version_info->prev_version_.begin();
                 version_it != version_info->prev_version_.end();) {
                const UndoLog* undo_log = GetUndoLog(version_it->second);
                if (undo_log->ts_ <= GetWatermark()) {
                    version_it = version_info->prev_version_.erase(version_it);
                } else {
                    ++version_it;
                }
            }

            // 如果版本信息为空，删除整个版本信息
            if (version_info->prev_version_.empty()) {
                it = shard.version_info_.erase(it);  // 删除空的版本信息
            } else {
                ++it;
            }
        }
    }
}

/** @brief 垃圾回收。仅在所有事务都未访问时调用。 */
void TransactionManager::GarbageCollection() {
    timestamp_t watermark = GetWatermark();
    std::vector<Transaction*> expired_txns;

    // 第一阶段：收集可以回收的事务
    {
        std::shared_lock<std::shared_mutex> lock(txn_map_mutex_);
        for (auto it = txn_map.begin(); it != txn_map.end(); ++it) {
            Transaction* txn = it->second;
            if (is_transaction_expired(txn, watermark)) {
                expired_txns.push_back(txn);
            }
        }
    }

    // 第二阶段：清理版本链
    cleanup_expired_versions(watermark);

    // 第三阶段：处理过期事务的删除操作并清理事务对象
    {
        std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);
        for (Transaction* txn : expired_txns) {
            // 只对已提交的事务执行删除操作
            if (txn->get_state() == TransactionState::COMMITTED) {
                do_delete(txn);
            }

            // 从事务映射表中删除
            txn_map.erase(txn->get_transaction_id());
            delete txn;  // 删除事务对象
        }
    }

    // 第四阶段：刷新到磁盘
    if (!expired_txns.empty()) {
        sm_manager.flush_to_disk();
    }
}

bool TransactionManager::should_perform_gc() {
    // 检查是否需要执行垃圾回收的条件
    std::shared_lock<std::shared_mutex> lock(txn_map_mutex_);

    // 条件1：事务数量过多（超过1000个）
    if (txn_map.size() > 1000) {
        return true;
    }

    // 条件2：检查是否有大量已终止的事务
    size_t terminated_count = 0;
    timestamp_t watermark = running_txns_.GetWatermark();

    for (const auto& pair : txn_map) {
        Transaction* txn = pair.second;
        if (is_transaction_expired(txn, watermark)) {
            terminated_count++;
        }
    }

    // 如果已终止的事务数量超过总数的30%，则需要GC
    return terminated_count > txn_map.size() * 0.3;
}

/** @brief 生成新的撤销日志并更新版本链接。
 * @details 根据磁盘现在的TupleMeta和RmRecord生成新的撤销日志。
 */
auto TransactionManager::GenerateNewUndoLog(int fd, Rid rid, const std::unique_ptr<RmRecord>& value,
                                            const TupleMeta& base_meta, Transaction* txn) -> bool {
    TRACE_FUNCTION
    auto log = std::make_unique<UndoLog>();
    if (base_meta.is_deleted_) {
        log->is_deleted_ = true;  // 如果是删除操作
    } else {
        log->is_deleted_ = false;  // 如果是插入或更新操作
        log->record_ = RmRecord(*value);
    }
    INFO("Generate new undo log for rid: {}", rid);
    INFO("Undo log ts: {}", txn->get_transaction_id());
    log->ts_ = txn->get_transaction_id();       // 设置时间戳为当前事务ID
    log->prev_version_ = GetUndoLink(fd, rid);  //
    auto link = txn->AppendUndoLog(std::move(log));
    UpdateUndoLink(fd, rid, link);  // 更新版本链接
    return true;
}

auto TransactionManager::CollectUndoLogs(Rid rid, UndoLink undo_link, Transaction* txn) -> std::vector<const UndoLog*> {
    if (!undo_link.IsValid()) {
        return {};
    }
    INFO("Collecting undo log for rid: {}", rid);
    INFO("Transaction ID: {}", txn->get_transaction_id());
    INFO("Transaction Read TS: {}", txn->get_read_ts());
    std::vector<const UndoLog*> undo_logs;
    while (undo_link.IsValid()) {
        const UndoLog* undo_log = GetUndoLog(undo_link);
        INFO("Undo log ts: {}", undo_log->ts_);
        if (undo_log->ts_ == txn->get_transaction_id() || undo_log->ts_ <= txn->get_read_ts()) {
            // 如果是当前事务的修改或者是已提交的事务
            break;
        }
        WARN("APPEND Undo log ts: {}", undo_log->ts_);
        undo_logs.push_back(undo_log);
        undo_link = undo_log->prev_version_;
    }
    return undo_logs;
}

auto TransactionManager::GetTupleAndUndoLink(RmFileHandle* fh_, const Rid& rid)
    -> std::tuple<TupleMeta, std::unique_ptr<RmRecord>, UndoLink> {
    auto page_guard = fh_->AcquirePageReadLock(rid);
    auto [base_meta, rec] = fh_->GetTupleWithLockAcquired(rid, page_guard.GetData());
    auto link = GetUndoLink(fh_->GetFd(), rid);
    return std::make_tuple(base_meta, std::move(rec), link);
}

auto TransactionManager::UpdateTupleAndUndoLink(const std::string& tab_name_, RmFileHandle* fh_, const Rid& rid,
                                                TupleMeta& base_meta, TupleMeta& new_meta,
                                                const std::unique_ptr<RmRecord>& old_rec,
                                                const std::unique_ptr<RmRecord>& new_rec, Transaction* txn) -> bool {
    auto page_guard = fh_->AcquirePageWriteLock(rid);
    auto meta = fh_->GetTupleMetaWithLockAcquired(rid, page_guard.GetData());
    if (meta != base_meta) {
        return false;  // 如果元数据不匹配，返回 false
    }

    // 更新当前记录
    if (new_meta.is_deleted_) {
        fh_->UpdateTupleMetaWithLockAcquired(rid, new_meta, page_guard.GetDataMut());
    } else {
        fh_->UpdateTupleWithLockAcquired(rid, new_meta, new_rec, page_guard.GetDataMut());
    }

    // 添加撤销日志
    if (meta.ts_ != txn->get_transaction_id()) {
        // 如果元数据的时间戳不是当前事务的时间戳，生成新的撤销日志
        GenerateNewUndoLog(fh_->GetFd(), rid, old_rec, meta, txn);
        txn->append_write_record(std::make_unique<WriteRecord>(tab_name_, rid));
    }
    return true;  // 更新成功
}

auto TransactionManager::AtomicUpdate(const std::string& tab_name, RmFileHandle* fh_, Rid& delete_rid,
                                      TupleMeta& delete_base_meta, const std::unique_ptr<RmRecord>& delete_rec,
                                      Rid& insert_rid, TupleMeta& insert_base_meta,
                                      const std::unique_ptr<RmRecord>& insert_old_rec,
                                      const std::unique_ptr<RmRecord>& insert_new_rec, Transaction* txn) -> bool {
    auto page_guard = fh_->AcquirePageWriteLock(delete_rid);
    auto meta = fh_->GetTupleMetaWithLockAcquired(delete_rid, page_guard.GetData());
    if (meta != delete_base_meta) {
        return false;  // 如果元数据不匹配，返回 false
    }
    // 先delete
    TupleMeta delete_new_meta(txn->get_transaction_id(), true);
    fh_->UpdateTupleMetaWithLockAcquired(delete_rid, delete_new_meta, page_guard.GetDataMut());

    if (meta.ts_ != txn->get_transaction_id()) {
        // 如果元数据的时间戳不是当前事务的时间戳，生成新的撤销日志
        GenerateNewUndoLog(fh_->GetFd(), delete_rid, delete_rec, meta, txn);
        txn->append_write_record(std::make_unique<WriteRecord>(tab_name, delete_rid));
    }

    if (insert_rid == delete_rid) {
        insert_base_meta = delete_new_meta;  // 如果插入和删除的RID相同，使用删除的元数据
    }

    // 然后这里的rid 是index中的rid
    if (delete_rid.page_no != insert_rid.page_no) {
        auto new_page_guard = fh_->AcquirePageWriteLock(insert_rid);
        meta = fh_->GetTupleMetaWithLockAcquired(insert_rid, new_page_guard.GetData());
        if (meta != insert_base_meta) {
            return false;  // 如果新元数据不匹配，返回 false
        }
        TupleMeta insert_new_meta(txn->get_transaction_id(), false);
        fh_->UpdateTupleWithLockAcquired(insert_rid, insert_new_meta, insert_new_rec, new_page_guard.GetDataMut());
        if (meta.ts_ != txn->get_transaction_id()) {
            // 如果元数据的时间戳不是当前事务的时间戳，生成新的撤销日志
            GenerateNewUndoLog(fh_->GetFd(), insert_rid, insert_old_rec, meta, txn);
            txn->append_write_record(std::make_unique<WriteRecord>(tab_name, insert_rid));
        }
    } else {
        meta = fh_->GetTupleMetaWithLockAcquired(insert_rid, page_guard.GetData());
        if (meta != insert_base_meta) {
            return false;  // 如果新元数据不匹配，返回 false
        }
        TupleMeta insert_new_meta(txn->get_transaction_id(), false);
        fh_->UpdateTupleWithLockAcquired(insert_rid, insert_new_meta, insert_new_rec, page_guard.GetDataMut());
        if (meta.ts_ != txn->get_transaction_id()) {
            // 如果元数据的时间戳不是当前事务的时间戳，生成新的撤销日志
            GenerateNewUndoLog(fh_->GetFd(), insert_rid, insert_old_rec, meta, txn);
            txn->append_write_record(std::make_unique<WriteRecord>(tab_name, insert_rid));
        }
    }

    return true;
}