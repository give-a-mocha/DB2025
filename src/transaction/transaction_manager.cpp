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
    log_manager->add_begin_log(txn->get_transaction_id());

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

    // MVCC: 分配提交时间戳
    txn->set_state(TransactionState::COMMITTED);
    timestamp_t commit_ts = INVALID_TS;
    if (concurrency_mode_ == ConcurrencyMode::MVCC) {
        commit_ts = get_next_timestamp();
        txn->set_commit_ts(commit_ts); // 设置提交时间戳
        last_commit_ts_.store(std::max(last_commit_ts_.load(), commit_ts));
    }

    std::shared_ptr<std::unordered_set<LockDataId>> lock_set = txn->get_lock_set();

    auto lock_set_copy = *lock_set; // 复制锁集合以避免迭代时修改
    for (const LockDataId& lock : lock_set_copy) {
        lock_manager_->unlock(txn, lock);
    }
    
    auto lock_gap_set = txn->get_lock_gap_set();
    auto lock_gap_set_copy = *lock_gap_set; // 复制间隙锁集合以避免迭代时修改
    for (const int& tab_fd : lock_gap_set_copy) {
        lock_manager_->unlock_gap(txn, tab_fd);
    }

    // 清空事务相关的集合
    txn->clear();

    log_manager->add_commit_log(txn->get_transaction_id());
    log_manager->flush_log_to_disk();

    running_txns_.RemoveTxn(txn->get_start_ts()); // 从水位线中移除事务
    running_txns_.UpdateCommitTs(commit_ts); // 更新水位线的提交时间戳
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
            auto rec = (*iter)->GetRecord();
            handle->delete_record(rid, context);
            sm_manager_->delete_index(table_name, rec, context);
            log_manager->add_delete_log(context->txn_->get_transaction_id(), (*iter)->GetRecord(), rid, table_name);
        } else if(write_type == WType::UPDATE_TUPLE) {
            //! 按道理来说不应该出现这种情况，因为更新操作是insert + delete
            auto new_rec = handle->get_record(rid, context);
            auto old_rec = (*iter)->GetRecord();
            handle->update_record(rid, old_rec.data, context);
            sm_manager_->delete_index(table_name, *new_rec, context);
            sm_manager_->insert_index(table_name, old_rec, rid, context);
            log_manager->add_update_log(context->txn_->get_transaction_id(), *new_rec, old_rec, rid, table_name);
        } else if(write_type == WType::DELETE_TUPLE) {
            auto rec = (*iter)->GetRecord();
            handle->insert_record_force(rid, rec.data);
            log_manager->add_insert_log(context->txn_->get_transaction_id(), rec, rid, table_name);
        }
        // 删除版本链记录
        DeleteUpdateVersionLink(rid, context->txn_);
    }

    std::shared_ptr<std::unordered_set<LockDataId>> lock_set = txn->get_lock_set();
    auto lock_set_copy = *lock_set; // 复制锁集合以避免迭代时修改
    for (const LockDataId& lock : lock_set_copy) {
        lock_manager_->unlock(txn, lock);
    }

    auto lock_gap_set = txn->get_lock_gap_set();
    auto lock_gap_set_copy = *lock_gap_set; // 复制间隙锁集合以避免迭代时修改
    for (const int& tab_fd : lock_gap_set_copy) {
        lock_manager_->unlock_gap(txn, tab_fd);
    }

    txn->clear();
    txn->set_state(TransactionState::ABORTED);
    txn->ClearUndoLogs();
    // 从全局事务表中删除
    std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);
    txn_map.erase(txn->get_transaction_id());  
    log_manager->add_abort_log(txn->get_transaction_id());
}

/**
* @brief 更新一个撤销链接，该链接将表堆元组与第一个撤销日志连接起来。
* 在更新之前，将调用 `check` 函数以确保有效性。
*/
bool TransactionManager::UpdateUndoLink(
    Rid rid,
    std::optional<UndoLink> prev_link
) {
    std::optional<VersionUndoLink> prev_version = VersionUndoLink::FromOptionalUndoLink(prev_link);
    return UpdateVersionLink(rid, prev_version);
}

/**
 * @brief 更新一个撤销链接，该链接将表堆元组与第一个撤销日志连接起来。
 * 在更新之前，将调用 `check` 函数以确保有效性。
 */
bool TransactionManager::UpdateVersionLink(
    Rid rid,
    std::optional<VersionUndoLink> prev_version
) {
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
    return true;  // 更新成功，返回 true
}

UndoLink TransactionManager::DeleteUpdateVersionLink(Rid rid, Transaction *txn) {

    // 获取对应的版本信息
    std::unique_lock<std::shared_mutex> lock(version_info_mutex_);
    auto it = version_info_.find(rid.page_no);
    if (it == version_info_.end()) {
        return UndoLink{};
    }
    auto &version_info = it->second;
    std::unique_lock<std::shared_mutex> version_lock(version_info->mutex_);
    // 更新版本链接
    auto &prev_version_map = version_info->prev_version_;
    auto prev_version_it = prev_version_map.find(rid.slot_no);
    if (prev_version_it != prev_version_map.end()) {
        VersionUndoLink version_link = prev_version_it->second;
        UndoLink undo_link = version_link.prev_;
        while (undo_link.prev_txn_ == txn->get_transaction_id()) {
            undo_link = GetUndoLog(undo_link).prev_version_;
            if(!undo_link.IsValid()) {
                break;
            }
        }
        if(undo_link.IsValid()) {
            // 如果撤销链接有效，则更新版本链接
            prev_version_it->second.prev_ = undo_link;
        } else {
            // 如果撤销链接无效，则删除该版本链接
            prev_version_map.erase(prev_version_it);
        }
        return undo_link;
    }
    return UndoLink{};
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
    return GetUndoLogWithoutLock(link);
}

UndoLog TransactionManager::GetUndoLogWithoutLock(UndoLink link){
    auto it = TransactionManager::txn_map.find(link.prev_txn_);
    if(it == TransactionManager::txn_map.end()) {
        throw RMDBError("Transaction not found: " + std::to_string(link.prev_txn_));
    }
    auto txn = it->second;
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
    
}

/**
 * @brief 添加插入操作的撤销日志
 * @param txn 事务指针
 * @param rid 插入的记录的RID
 * @param values 插入的记录值
 */
void TransactionManager::add_insert_undo_log(
    Transaction *txn, 
    Rid rid, 
    std::vector<Value> values
) {
    UndoLog log;
    log.is_deleted_ = false;
    log.modified_fields_ = std::vector<bool>(values.size(), true);
    log.tuple_ = std::move(values);
    log.ts_ = get_next_timestamp();
    log.prev_version_ = UndoLink{}; // insert undo log 没有前一个版本
    auto undo_link = txn->AppendUndoLog(log);
    UpdateUndoLink(rid, undo_link);
}

/**
 * @brief 添加修改操作的撤销日志
 * @param txn 事务指针
 * @param rid 要删除的记录的RID
 * @param values 修改前的记录值
 * @param modified_fields 修改的字段
 */
void TransactionManager::add_update_undo_log(
    Transaction *txn,
    Rid rid, 
    std::vector<Value> values, 
    std::vector<bool> modified_fields
) {
    UndoLog log;
    log.is_deleted_ = false;
    log.modified_fields_ = std::move(modified_fields);
    log.tuple_ = std::move(values);
    log.ts_ = get_next_timestamp();
    log.prev_version_ = DeleteUpdateVersionLink(rid, txn);
    auto undo_link = txn->AppendUndoLog(log);
    UpdateUndoLink(rid, undo_link);
}

/**
 * @brief 添加删除操作的撤销日志
 * @param txn 事务指针
 * @param rid 要删除的记录的RID
 * @param values 删除前的记录值
 */
void TransactionManager::add_delete_undo_log(
    Transaction *txn, 
    Rid rid, 
    std::vector<Value> values
) {
    UndoLog log;
    log.is_deleted_ = true;
    log.modified_fields_ = std::vector<bool>(values.size(), true);
    log.tuple_ = std::move(values);
    log.ts_ = get_next_timestamp();
    log.prev_version_ = DeleteUpdateVersionLink(rid, txn);
    auto undo_link = txn->AppendUndoLog(log);
    UpdateUndoLink(rid, undo_link);
}