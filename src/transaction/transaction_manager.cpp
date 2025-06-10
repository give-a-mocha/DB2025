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
 * @brief 开始一个新事务
 *
 * @param txn 传入的事务指针，如果为 nullptr 则创建新事务
 * @param log_manager 日志管理器指针，用于记录事务日志
 * @return Transaction* 返回当前事务指针
 */
Transaction* TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针

    if (txn == nullptr) {
        // 创建新事务，并分配递增的事务ID
        txn = new Transaction(next_txn_id_.fetch_add(1), IsolationLevel::Snapshot_Isolation);
    }

    // 将当前事务添加到全局事务映射表中，便于后续通过事务ID查找
    txn_map.emplace(txn->get_transaction_id(), txn);
    timestamp_t start_ts = next_timestamp_.fetch_add(1);
    txn->set_start_ts(start_ts);  // 设置事务开始时间戳
    txn->read_ts_.store(start_ts); // MVCC: 设置读时间戳为开始时间戳
    txn->set_txn_mode(false);
    txn->set_state(TransactionState::DEFAULT);
    // MVCC: 将新事务添加到水位线跟踪
    if (concurrency_mode_ == ConcurrencyMode::MVCC) {
        running_txns_.AddTxn(start_ts); // 使用 AddTxn 和 start_ts
    }
    // 创建BEGIN事务日志记录，记录事务开始的操作
    auto* log = new BeginLogRecord(txn->get_transaction_id());
    record_link_management(log, log_manager, txn);

    return txn;
}

/**
 * @brief 提交事务

 * @param txn 要提交的事务指针
 * @param log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态


    // MVCC: 分配提交时间戳
    timestamp_t commit_ts = INVALID_TS;
    if (concurrency_mode_ == ConcurrencyMode::MVCC) {
        commit_ts = next_timestamp_.fetch_add(1);
        txn->commit_ts_.store(commit_ts);
        last_commit_ts_.store(std::max(last_commit_ts_.load(), commit_ts));
    }

    std::shared_ptr<std::unordered_set<LockDataId>> lock_set = txn->get_lock_set();
    for (const LockDataId& lock : *lock_set) {
        lock_manager_->unlock(txn, lock);
    }

    // 清空事务相关的集合
    txn->clear();
    //?对于write_set中的所有数据项刷新buffer_pool

    auto log = new CommitLogRecord(txn->get_transaction_id());
    record_link_management(log, log_manager, txn);

    txn->set_state(TransactionState::COMMITTED);

    // 从全局事务表中移除
    {
        std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);
        txn_map.erase(txn->get_transaction_id());
    }

    // MVCC: 从水位线中移除事务
    if (concurrency_mode_ == ConcurrencyMode::MVCC) {
        running_txns_.RemoveTxn(txn->get_read_ts()); // 使用 RemoveTxn 和 read_ts (即 start_ts)
    }
}

/**
 * @brief 回滚事务
 *
 * @param context 执行上下文，包含当前事务信息
 * @param log_manager 日志管理器指针，用于记录回滚日志
 */
void TransactionManager::abort(Context* context, LogManager* log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态


    Transaction* txn = context->txn_;

    auto write_set = txn->get_write_set();

    for (auto iter = write_set->rbegin(); iter != write_set->rend(); iter++) {
        const auto write_type = (*iter)->GetWriteType();
        const auto table_name = (*iter)->GetTableName();
        const auto rid = (*iter)->GetRid();
        std::unique_ptr<RmFileHandle>& handle = sm_manager_->fhs_.at(table_name);

        switch (write_type) {
            case WType::INSERT_TUPLE: {
                auto record = handle->get_record(rid, context);
                // Pass *record (RmRecord) instead of record (unique_ptr<RmRecord>)
                auto log_record = std::make_unique<DeleteLogRecord>(txn->get_transaction_id(), *record, rid, table_name);
                record_link_management(log_record.get(), log_manager, txn);

                delete_index_record(sm_manager_->db_.get_table(table_name), record.get(), rid, context );
                handle->delete_record(rid, context);
                break;
            }
            case WType::UPDATE_TUPLE: {
                auto old_record = (*iter)->GetRecord();
                auto new_record = handle->get_record(rid, context);
                // Pass *new_record (RmRecord) instead of new_record (unique_ptr<RmRecord>)
                auto log_record =
                    std::make_unique<UpdateLogRecord>(txn->get_transaction_id(), old_record, *new_record, rid, table_name);
                record_link_management(log_record.get(), log_manager, txn);


                delete_index_record(sm_manager_->db_.get_table(table_name), new_record.get(), rid, context);
                handle->update_record(rid, old_record.data, context);
                insert_index_record(sm_manager_->db_.get_table(table_name), &old_record, rid, context);
                break;
            }
            case WType::DELETE_TUPLE: {
                auto record = (*iter)->GetRecord();
                auto log_record = std::make_unique<InsertLogRecord>(txn->get_transaction_id(), record, rid, table_name);
                record_link_management(log_record.get(), log_manager, txn);

                insert_index_record(sm_manager_->db_.get_table(table_name), &record, rid, context);
                handle->insert_record(rid, record.data);
                break;
            }
            default:
                throw InternalError("Invalid write type");
                break;
        }
    }
    write_set->clear();


    std::shared_ptr<std::unordered_set<LockDataId>> lock_set = txn->get_lock_set();
    for (const LockDataId& lock : *lock_set) {
        lock_manager_->unlock(txn, lock);
    }


    txn->clear();


    AbortLogRecord* log = new AbortLogRecord(txn->get_transaction_id());
    record_link_management(log, log_manager, txn);


    txn->set_state(TransactionState::ABORTED);

    // 从全局事务表中移除
    {
        std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);
        txn_map.erase(txn->get_transaction_id());
    }

    // MVCC: 从水位线中移除事务
    if (concurrency_mode_ == ConcurrencyMode::MVCC) {
        running_txns_.RemoveTxn(txn->get_read_ts()); // 使用 RemoveTxn 和 read_ts (即 start_ts)
    }
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
 * @brief 更新元组的撤销链接，将其连接到其第一个撤销日志。
 * 可选地在更新前执行检查。
 * @param rid 元组的记录ID。
 * @param prev_link 要设置的前一个撤销链接。
 * @param check 在更新前验证当前链接的可选函数。
 * @return 如果更新成功则返回 true，否则返回 false。
 */
bool TransactionManager::UpdateUndoLink(
    Rid rid,
    std::optional<UndoLink> prev_link,
    std::function<bool(std::optional<UndoLink>)> &&check = nullptr)
{
    if(check != nullptr) {
        // 如果提供了检查函数，则先执行检查
        if (!check(prev_link)) {
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
    // 更新撤销链接
    auto &prev_version_map = version_info->prev_version_;
    std::optional<VersionUndoLink> prev_version = VersionUndoLink::FromOptionalUndoLink(prev_link);
    if (prev_link.has_value()) {
        // 如果提供了前一个撤销链接，则更新或插入
        prev_version_map[rid.slot_no] = *prev_version;
    } else {
        // 如果没有提供前一个撤销链接，则删除对应的条目
        prev_version_map.erase(rid.slot_no);
    }
    return true;  // 更新成功，返回 true
}

/**
 * @brief 更新元组的版本链接，用于 MVCC。
 * 可选地在更新前执行检查。
 * @param rid 元组的记录ID。
 * @param prev_version 要设置的前一个版本链接。
 * @param check 在更新前验证当前版本链接的可选函数。
 * @return 如果更新成功则返回 true，否则返回 false。
 */
bool TransactionManager::UpdateVersionLink(
    Rid rid,
    std::optional<VersionUndoLink> prev_version,
    std::function<bool(std::optional<VersionUndoLink>)> &&check = nullptr)
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
        // 如果提供了前一个版本链接，则更新或插入
        prev_version_map[rid.slot_no] = *prev_version;
    } else {
        // 如果没有提供前一个版本链接，则删除对应的条目
        prev_version_map.erase(rid.slot_no);
    }
    // 如果是 MVCC 模式，则需要更新版本状态
    if (concurrency_mode_ == ConcurrencyMode::MVCC) {
        // 如果是 MVCC 模式，设置版本状态为不在进行中
        if (prev_version.has_value()) {
            prev_version_map[rid.slot_no].in_progress_ = false;
        }
    }
    return true;  // 更新成功，返回 true
}

/**
 * @brief 获取与元组关联的第一个撤销链接。
 * @param rid 元组的记录ID。
 * @return 一个可选的 UndoLink。
 */
std::optional<UndoLink> TransactionManager::GetUndoLink(Rid rid){
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
    return prev_version_it->second.prev_;  // 返回前一个撤销链接
}

/**
 * @brief 获取与元组关联的第一个版本链接 (用于 MVCC)。
 * @param rid 元组的记录ID。
 * @return 一个可选的 VersionUndoLink。
 */
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

/**
 * @brief 从事务的撤销缓冲区中检索撤销日志。
 * @param link 指向所需日志的 UndoLink。
 * @return 一个可选的 UndoLog。如果事务不存在，则返回 nullopt。
 * @throws std::out_of_range 如果链接的索引超出范围。
 */
std::optional<UndoLog> TransactionManager::GetUndoLogOptional(UndoLink link){
    // 检查事务是否存在
    auto txn = get_transaction(link.prev_txn_);
    if (txn == nullptr) {
        return std::nullopt;  // 如果事务不存在，则返回 nullopt
    }

    // 检查撤销日志索引是否有效
    if (link.prev_log_idx_ < 0 || link.prev_log_idx_ >= txn->GetUndoLogNum()) {
        throw std::out_of_range("Invalid undo log index");
    }

    // 返回对应的撤销日志
    return txn->GetUndoLog(link.prev_log_idx_);
}

/**
 * @brief 从事务的撤销缓冲区中检索撤销日志。
 * 这是访问撤销日志的首选方法，除非访问当前事务的缓冲区。
 * @param link 指向所需日志的 UndoLink。
 * @return UndoLog。
 * @throws TransactionAbortException 如果事务不存在或发生其他错误。
 */
UndoLog TransactionManager::GetUndoLog(UndoLink link){
    // 检查事务是否存在
    auto txn = get_transaction(link.prev_txn_);
    if (txn == nullptr) {
        throw TransactionAbortException(link.prev_txn_, AbortReason::LOCK_ON_SHIRINKING);
    }

    // 检查撤销日志索引是否有效
    if (link.prev_log_idx_ < 0 || link.prev_log_idx_ >= txn->GetUndoLogNum()) {
        throw std::out_of_range("Invalid undo log index");
    }

    // 返回对应的撤销日志
    return txn->GetUndoLog(link.prev_log_idx_);
}

/**
 * @brief 获取所有正在运行的事务中的最低读取时间戳 (水位线)。
 * @return 当前水位线时间戳。
 */
timestamp_t TransactionManager::GetWatermark(){
    std::shared_lock<std::shared_mutex> lock(txn_map_mutex_);
    timestamp_t min_ts = std::numeric_limits<timestamp_t>::max();
    for (const auto& [txn_id, txn] : txn_map) {
        if (txn->get_state() == TransactionState::DEFAULT) {
            min_ts = std::min(min_ts, txn->get_start_ts());
        }
    }
    return min_ts;
}

/** @brief 在 MVCC 中为旧版本执行垃圾回收。仅应在没有活动事务时调用。 */
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
    // 更新活跃事务水位线
    // running_txns_.Update(GetWatermark());
    // 更新最近提交事务的时间戳
    // last_commit_ts_ = std::max(last_commit_ts_, GetWatermark());
    // 清理过期的事务
    // std::unique_lock<std::shared_mutex> txn_lock(txn_map_mutex_);
    // for (auto txn_it = txn_map.begin(); txn_it != txn_map.end();) {
    //     Transaction* txn = txn_it->second;
    //     if (txn->get_state() == TransactionState::COMMITTED || txn->get_state() == TransactionState::ABORTED) {
    //         // 如果事务已提交或回滚，则从全局事务表中移除
    //         delete txn;  // 释放事务对象内存
    //         txn_it = txn_map.erase(txn_it);
    //     } else {
    //         ++txn_it;  // 否则继续下一个事务
    //     }
    // }
    // 清理过期的撤销日志
    // for (auto txn_it = txn_map.begin(); txn_it != txn_map.end();) {
    //     Transaction* txn = txn_it->second;
    //     if (txn->GetUndoLogNum() > 0) {
    //         // 如果事务有撤销日志，则清理过期的日志
    //         txn->ClearExpiredUndoLogs(last_commit_ts_);
    //     }
    //     ++txn_it;  // 继续下一个事务
    // }
    // 清理过期的锁
    // lock_manager_->GarbageCollection(last_commit_ts_);
    // 清理过期的索引
    // sm_manager_->get_ix_manager()->GarbageCollection(last_commit_ts_);
    // 清理过期的文件句柄
    // sm_manager_->GarbageCollection(last_commit_ts_);
    // 清理过期的文件
    // sm_manager_->fhs_.GarbageCollection(last_commit_ts_);
    // 清理过期的索引句柄
    // sm_manager_->ihs_.GarbageCollection(last_commit_ts_);
    // 清理过期的表元数据
    // sm_manager_->db_.tabs_.GarbageCollection(last_commit_ts_);
    // 清理过期的数据库元数据
    // sm_manager_->db_.GarbageCollection(last_commit_ts_);
    // 清理过期的日志
    // log_manager_->GarbageCollection(last_commit_ts_);
    // 清理过期的缓冲池页面
    // sm_manager_->get_bpm()->GarbageCollection(last_commit_ts_);
    // 清理过期的上下文
    // Context::GarbageCollection(last_commit_ts_);
    // 清理过期的锁数据
    // lock_manager_->GetLockDataManager()->GarbageCollection(last_commit_ts_);
    // 清理过期的锁数据
    // sm_manager_->get_rm_manager()->GarbageCollection(last_commit_ts_);
    // 清理过期的索引数据
    // sm_manager_->get_ix_manager()->GarbageCollection(last_commit_ts_);
    // 清理过期的系统管理器
    // sm_manager_->GarbageCollection(last_commit_ts_);
    // 清理过期的磁盘管理器
    // sm_manager_->get_disk_manager()->GarbageCollection(last_commit_ts_);
    // 清理过期的缓冲池管理器
    // sm_manager_->get_buffer_pool_manager()->GarbageCollection(last_commit_ts_);
    // 清理过期的日志管理器
    // log_manager_->GarbageCollection(last_commit_ts_);
    // 清理过期的执行上下文
    // Context::GarbageCollection(last_commit_ts_);
    // 清理过期的锁管理器
    // lock_manager_->GarbageCollection(last_commit_ts_);
    // 清理过期的事务管理器
    txn_map.clear();  // 清空全局事务表
    next_txn_id_ = 0;  // 重置事务ID计数器
    next_timestamp_ = 0;  // 重置时间戳计数器
    last_commit_ts_ = 0;  // 重置最近提交时间戳 
}