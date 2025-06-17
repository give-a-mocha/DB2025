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

#include "record/rm_file_handle.h"
#include "system/sm_manager.h"
#include "common/print.hpp"

std::unordered_map<txn_id_t, Transaction*> TransactionManager::txn_map = {};

/**
 * @brief 事务管理器构造函数
 *
 * @param lock_manager 锁管理器指针，用于并发控制
 * @param sm_manager 系统管理器指针，用于访问数据库资源
 * @param concurrency_mode 并发控制模式，默认为两阶段封锁
 */
TransactionManager::TransactionManager(LockManager* lock_manager, SmManager* sm_manager,
                                       ConcurrencyMode concurrency_mode) {
    // 初始化成员变量
    lock_manager_ = lock_manager;
    sm_manager_ = sm_manager;
    concurrency_mode_ = concurrency_mode;

    // 初始化事务计数器和时间戳
    next_txn_id_ = 0;
    next_timestamp_ = 0;
    last_commit_ts_ = 0;
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
    txn_map.emplace(txn->get_transaction_id(), txn);
    timestamp_t start_ts = get_next_timestamp();
    txn->set_start_ts(start_ts);  // 设置事务开始时间戳
    txn->set_read_ts(last_commit_ts_); // 设置读取时间戳该事务早的最后一次提交时间戳
    txn->set_txn_mode(false);
    txn->set_state(TransactionState::DEFAULT);
    // MVCC: 将新事务添加到水位线跟踪
    // if (concurrency_mode_ == ConcurrencyMode::MVCC) {
    //     running_txns_.AddTxn(start_ts); // 使用 AddTxn 和 start_ts
    // }
    // 创建BEGIN事务日志记录，记录事务开始的操作
    auto* log = new BeginLogRecord(txn->get_transaction_id());
    record_link_management(log, log_manager, txn);
    running_txns_.AddTxn(start_ts); // 添加事务到水位线
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

    ERROR("COMMIT 开始");
    // MVCC: 分配提交时间戳
    txn->set_state(TransactionState::COMMITTED);
    timestamp_t commit_ts = INVALID_TS;
    if (concurrency_mode_ == ConcurrencyMode::MVCC) {
        commit_ts = get_next_timestamp();
        txn->set_commit_ts(commit_ts); // 设置提交时间戳
        last_commit_ts_.store(std::max(last_commit_ts_.load(), commit_ts));
    }
    int index = 0;
    for(const auto& write_record : *txn->get_write_set()) {
        INFO("COMMIT 写操作: {}, {}", write_record->GetWriteType(), write_record->GetTableName());
        auto undoLog = txn->GetUndoLog(index);
        undoLog.ts_ = commit_ts;
        txn->ModifyUndoLog(index, undoLog);
        if (write_record->GetWriteType() == WType::INSERT_TUPLE) {
            index++;
            continue;
        } else {
            //更新undo日志
            UndoLink undoLink = {txn->get_transaction_id(), index};
            UpdateUndoLink(write_record->GetRid(), undoLink);
            auto& fh_ = sm_manager_->fhs_.at(write_record->GetTableName());
            if(write_record->GetWriteType() == WType::DELETE_TUPLE) {
                // fh_->delete_record(write_record->GetRid(), nullptr);
            } else if(write_record->GetWriteType() == WType::UPDATE_TUPLE) {
                fh_->update_record(write_record->GetRid(), write_record->GetRecord().data, nullptr);
            }

        }
        index++;
    }
    INFO("COMMIT 提交阶段结束");

    std::shared_ptr<std::unordered_set<LockDataId>> lock_set = txn->get_lock_set();
    INFO("事务的锁集合大小: {}", txn->get_lock_set()->size());

    auto lock_set_copy = *lock_set; // 复制锁集合以避免迭代时修改
    for (const LockDataId& lock : lock_set_copy) {
        ERROR("开始释放锁");
        lock_manager_->unlock(txn, lock);
    }
    INFO("COMMIT 释放锁结束");

    // 清空事务相关的集合
    txn->clear();
    INFO("COMMIT 清空事务相关集合结束");
    //?对于write_set中的所有数据项刷新buffer_pool

    auto log = new CommitLogRecord(txn->get_transaction_id());
    record_link_management(log, log_manager, txn);

    INFO("COMMIT 水位线");
    running_txns_.RemoveTxn(txn->get_start_ts()); // 从水位线中移除事务
    running_txns_.UpdateCommitTs(commit_ts); // 更新水位线的提交时间戳
    ERROR("COMMIT 结束");
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

    for (auto iter = write_set->rbegin(); iter != write_set->rend(); iter++) {
        const auto write_type = (*iter)->GetWriteType();
        const auto table_name = (*iter)->GetTableName();
        const auto rid = (*iter)->GetRid();
        std::unique_ptr<RmFileHandle>& handle = sm_manager_->fhs_.at(table_name);
        if(write_type == WType::INSERT_TUPLE) {
            handle->delete_record(rid, context);
            //在版本链中删除insert的
            std::unique_lock<std::shared_mutex> lock(version_info_mutex_);
            auto pageversion_info = version_info_.find(rid.page_no);
            if (pageversion_info != version_info_.end()) {
                // 如果存在版本信息，则删除对应的版本链接
                std::unique_lock<std::shared_mutex> lock(pageversion_info->second->mutex_);
                auto& prev_version = pageversion_info->second->prev_version_;
                auto it = prev_version.find(rid.slot_no);
                if (it != prev_version.end()) {
                    prev_version.erase(it); // 删除对应的版本链接
                }
            }
        }
    }

    std::shared_ptr<std::unordered_set<LockDataId>> lock_set = txn->get_lock_set();
    auto lock_set_copy = *lock_set; // 复制锁集合以避免迭代时修改
    for (const LockDataId& lock : lock_set_copy) {
        ERROR("开始释放锁");
        lock_manager_->unlock(txn, lock);
    }
    txn->clear();
    AbortLogRecord* log = new AbortLogRecord(txn->get_transaction_id());
    record_link_management(log, log_manager, txn);
    txn->set_state(TransactionState::ABORTED);
}

/**
 * @brief 管理日志记录的链接关系
 *
 * @details
 * 该函数的主要作用是维护事务日志链的顺序关系，确保日志记录能够按照正确的顺序被写入和回放。
 * 具体操作包括：
 * 1. 设置当前日志记录的前序LSN（Log Sequence Number）。
 * 2. 将日志记录添加到日志管理器的缓冲区中。
 * 3. 更新事务的前序LSN为当前日志记录的LSN，以便后续日志能够正确链接。
 *
 * @param log_record 当前的日志记录指针
 * @param log_manager 日志管理器指针，用于管理日志缓冲区
 * @param txn 当前事务指针，用于更新事务的状态
 */
void TransactionManager::record_link_management(LogRecord* log_record, LogManager* log_manager, Transaction* txn) {
    // 设置当前日志记录的前序LSN为事务的前序LSN，建立日志链
    log_record->prev_lsn_ = txn->get_prev_lsn();

    // 将当前日志记录添加到日志管理器的缓冲区中，等待刷盘
    log_manager->add_log_to_buffer(log_record);

    // 更新事务的前序LSN为当前日志记录的LSN，以便后续日志能够正确链接
    txn->set_prev_lsn(log_record->lsn_);
}

void TransactionManager::delete_index_record(TabMeta tab_, RmRecord* rec, Rid rid, Context *context){
    for(const auto& index : tab_.indexes) {
        auto ix_ = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(index.tab_name, index.cols)).get();
        auto key = std::make_unique<char[]>(index.col_tot_len);
        int offset = 0;
        for (size_t i = 0; i < static_cast<size_t>(index.col_num); ++i) {
            memcpy(key.get() + offset, rec->data + index.cols[i].offset, index.cols[i].len);
            offset += index.cols[i].len;
        }
        ix_->delete_entry(key.get(), context->txn_);
    }
}

void TransactionManager::insert_index_record(TabMeta tab_, RmRecord* rec, Rid rid, Context *context){
    for(const auto& index : tab_.indexes) {
        auto ix_ = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(index.tab_name, index.cols)).get();
        auto key = std::make_unique<char[]>(index.col_tot_len);
        int offset = 0;
        for (size_t i = 0; i < static_cast<size_t>(index.col_num); ++i) {
            memcpy(key.get() + offset, rec->data + index.cols[i].offset, index.cols[i].len);
            offset += index.cols[i].len;
        }
        ix_->insert_entry(key.get(), rid, context->txn_);
    }
}


/**
* @brief 更新一个撤销链接，该链接将表堆元组与第一个撤销日志连接起来。
* 在更新之前，将调用 `check` 函数以确保有效性。
*/
bool TransactionManager::UpdateUndoLink(
    Rid rid,
    std::optional<UndoLink> prev_link,
    std::function<bool(std::optional<UndoLink>)> &&check)
{
    if(check != nullptr) {
        // 如果提供了检查函数，则先执行检查
        if (!check(prev_link)) {
            return false;  // 检查失败，返回 false
        }
    }
    std::optional<VersionUndoLink> prev_version = VersionUndoLink::FromOptionalUndoLink(prev_link);
    return UpdateVersionLink(rid, prev_version);
}

/**
 * @brief 更新一个撤销链接，该链接将表堆元组与第一个撤销日志连接起来。
 * 在更新之前，将调用 `check` 函数以确保有效性。
 */
bool TransactionManager::UpdateVersionLink(
    Rid rid,
    std::optional<VersionUndoLink> prev_version,
    std::function<bool(std::optional<VersionUndoLink>)> &&check)
{
    if(check != nullptr) {
        // 如果提供了检查函数，则先执行检查
        if (!check(prev_version)) {
            return false;  // 检查失败，返回 false
        }
    }
    // 获取对应的版本信息
    std::shared_lock<std::shared_mutex> lock(version_info_mutex_);
    auto it = version_info_.find(rid.page_no);
    if (it == version_info_.end()) {
        // 如果没有找到对应的版本信息，则创建一个新的
        auto new_version_info = std::make_shared<PageVersionInfo>();
        version_info_[rid.page_no] = new_version_info;
        it = version_info_.find(rid.page_no);
    }
    auto &version_info = it->second;
    lock.unlock();
    std::unique_lock<std::shared_mutex> version_lock(version_info->mutex_);
    // 更新版本链接
    auto &prev_version_map = version_info->prev_version_;
    if (prev_version.has_value()) {
        // 如果提供了版本链接，则更新或插入
        prev_version_map[rid.slot_no] = *prev_version;
    } else {
        return false;
    }
    // 如果是 MVCC 模式，则需要更新版本状态
    // if (concurrency_mode_ == ConcurrencyMode::MVCC) {
    //     // 如果是 MVCC 模式，设置版本状态为不在进行中
    //     if (prev_version.has_value()) {
    //         prev_version_map[rid.slot_no].in_progress_ = false;
    //     }
    // }
    return true;  // 更新成功，返回 true
}

/** @brief 获取表堆元组的第一个撤销日志。 */
std::optional<UndoLink> TransactionManager::GetUndoLink(Rid rid){
    std::optional<VersionUndoLink> version_link = GetVersionLink(rid);
    if (!version_link.has_value()) {
        return std::nullopt;  // 如果没有找到对应的版本链接，则返回 nullopt
    }
    return version_link->prev_;  // 返回前一个版本链接的撤销日志
}

/** @brief 获取表堆元组的第一个撤销日志。 */
std::optional<VersionUndoLink> TransactionManager::GetVersionLink(Rid rid){
    std::shared_lock<std::shared_mutex> lock(version_info_mutex_);
    auto it = version_info_.find(rid.page_no);
    if (it == version_info_.end()) {
        return std::nullopt;  // 如果没有找到对应的版本信息，则返回 nullopt
    }
    auto &version_info = it->second;
    lock.unlock();
    std::shared_lock<std::shared_mutex> version_lock(version_info->mutex_);
    auto prev_version_it = version_info->prev_version_.find(rid.slot_no);
    if (prev_version_it == version_info->prev_version_.end()) {
        return std::nullopt;  // 如果没有找到对应的前一个版本链接，则返回 nullopt
    }
    return prev_version_it->second;  // 返回前一个版本链接
}

/** @brief 访问事务撤销日志缓冲区并获取撤销日志。如果事务不存在，返回 nullopt。
 * 如果索引超出范围仍然会抛出异常。 */
std::optional<UndoLog> TransactionManager::GetUndoLogOptional(UndoLink link){
    // 检查事务是否存在
    if (link.prev_txn_ == INVALID_TXN_ID) return std::nullopt;
    std::unique_lock<std::mutex> lock(latch_);
    auto it = TransactionManager::txn_map.find(link.prev_txn_);
    if (it == TransactionManager::txn_map.end()) {
        lock.unlock();
        return std::nullopt;  // 如果事务不存在，则返回 nullopt
    }
    auto txn = it->second;
    lock.unlock();

    // 检查撤销日志索引是否有效
    if (link.prev_log_idx_ < 0 || link.prev_log_idx_ >= txn->GetUndoLogNum()) {
        throw RangeError("Invalid undo log index: " + std::to_string(link.prev_log_idx_));
    }

    // 返回对应的撤销日志
    return txn->GetUndoLog(link.prev_log_idx_);
}

/** @brief 访问事务撤销日志缓冲区并获取撤销日志。除非访问当前事务缓冲区，
 * 否则应该始终调用此函数以获取撤销日志，而不是手动检索事务 shared_ptr 并访问缓冲区。 */
UndoLog TransactionManager::GetUndoLog(UndoLink link){
    // 检查事务是否存在
    if (link.prev_txn_ == INVALID_TXN_ID){
        throw RMDBError("Invalid transaction ID: " + std::to_string(link.prev_txn_));
    }
    std::unique_lock<std::mutex> lock(latch_);
    auto it = TransactionManager::txn_map.find(link.prev_txn_);
    if (it == TransactionManager::txn_map.end()) {
        lock.unlock();
        throw RMDBError("Transaction not found: " + std::to_string(link.prev_txn_));
    }
    auto txn = it->second;
    lock.unlock();

    // 检查撤销日志索引是否有效
    if (link.prev_log_idx_ < 0 || link.prev_log_idx_ >= txn->GetUndoLogNum()) {
        throw RangeError("Invalid undo log index: " + std::to_string(link.prev_log_idx_));
    }

    // 返回对应的撤销日志
    return txn->GetUndoLog(link.prev_log_idx_);
}

/** @brief 获取系统中的最低读时间戳。 */
timestamp_t TransactionManager::GetWatermark(){
    return running_txns_.GetWatermark();
}
/** @brief 垃圾回收。仅在所有事务都未访问时调用。 */
void TransactionManager::GarbageCollection(){
    std::unique_lock<std::shared_mutex> lock(version_info_mutex_);
    for (auto it = version_info_.begin(); it != version_info_.end();) {
        auto& page_version_info = it->second;
        std::unique_lock<std::shared_mutex> version_lock(page_version_info->mutex_);
        
        // 遍历每个槽的版本链接
        for (auto version_it = page_version_info->prev_version_.begin();
             version_it != page_version_info->prev_version_.end();) {
            if (!version_it->second.in_progress_) {
                // 如果版本不在进行中，则可以安全删除
                version_it = page_version_info->prev_version_.erase(version_it);
            } else {
                ++version_it;  // 否则继续下一个版本
            }
        }

        // 如果该页面没有任何版本链接，则删除该页面的版本信息
        if (page_version_info->prev_version_.empty()) {
            it = version_info_.erase(it);
        } else {
            ++it;  // 否则继续下一个页面
        }
    }
    lock.unlock();

    txn_map.clear();  // 清空全局事务表
    next_txn_id_ = 0;  // 重置事务ID计数器
    next_timestamp_ = 0;  // 重置时间戳计数器
    last_commit_ts_ = 0;  // 重置最近提交时间戳 
}