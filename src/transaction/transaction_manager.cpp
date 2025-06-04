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
 * @brief 开始一个新事务或继续一个已有事务
 *
 * @details
 * 该函数执行以下操作：
 * 1. 如果是新事务：
 *    - 创建新的事务对象
 *    - 分配唯一的事务ID
 * 2. 将事务添加到全局事务表：
 *    - 使用互斥锁保护并发访问
 *    - 建立事务ID到事务对象的映射
 * 3. 写入事务日志：
 *    - 创建BEGIN类型的日志记录
 *    - 维护日志序列号(LSN)链
 *
 * @param txn 事务指针，nullptr表示需要创建新事务
 * @param log_manager 日志管理器指针
 * @return 初始化后的事务指针
 */
Transaction* TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // 1. 判断传入事务参数是否为空指针，为空则创建新事务
    if (txn == nullptr) {
        // 创建新事务，并分配递增的事务ID
        txn = new Transaction(next_txn_id_++);
    }

    // 将当前事务添加到全局事务映射表中，便于后续通过事务ID查找
    txn_map.emplace(txn->get_transaction_id(), txn);

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
    // Todo: 实现事务提交逻辑

    // 1. 提交所有未完成的写操作
    // - 遍历write_set中的所有数据项
    // - 对于每个修改，将其写入磁盘
    // - 清空write_set

    // 2. 释放所有持有的锁
    // - 遍历lock_set中的所有锁
    // - 按照2PL协议释放锁
    // - 更新锁管理器状态

    std::shared_ptr<std::unordered_set<LockDataId>> lock_set = txn->get_lock_set();
    for (const LockDataId& lock : *lock_set) {
        lock_manager_->unlock(txn, lock);
    }

    // 清空事务相关的集合
    txn->clear();

    // 3. 资源清理
    // - 释放事务占用的内存
    // - 清空事务的write_set和lock_set

    // 4. 日志处理
    // - 创建COMMIT类型日志记录
    // - 更新日志序列号链
    // - 确保日志被刷入磁盘

    auto log = new CommitLogRecord(txn->get_transaction_id());
    record_link_management(log, log_manager, txn);

    // 5. 更新事务状态
    // - 将状态设置为COMMITTED
    // - 从全局事务表中移除

    txn->set_state(TransactionState::COMMITTED);

    // 6. MVCC支持（如果启用）
    // - 更新记录的版本链
    // - 更新提交时间戳
    // - 维护活跃事务水位线
}

/**
 * @brief 终止（回滚）事务
 *
 * @details
 * 该函数需要执行以下操作：
 * 1. 回滚所有修改：
 *    - 根据撤销日志逆序执行
 *    - 恢复修改前的数据状态
 * 2. 锁的处理：
 *    - 释放事务持有的所有锁
 *    - 更新锁管理器状态
 * 3. 资源清理：
 *    - 释放事务占用的内存
 *    - 清空事务的锁集合
 * 4. 日志处理：
 *    - 写入ABORT类型的日志记录
 *    - 确保日志被刷入磁盘
 * 5. 状态更新：
 *    - 将事务状态设置为ABORTED
 * 6. MVCC支持（如果启用）：
 *    - 清理版本链
 *    - 回退时间戳相关操作
 *
 * @param txn 要回滚的事务指针
 * @param log_manager 日志管理器指针
 */
void TransactionManager::abort(Context* context, LogManager* log_manager) {
    // Todo: 实现事务回滚逻辑

    // 1. 回滚所有写操作
    // - 按照撤销日志的逆序回滚
    // - 对每条日志记录执行补偿操作
    // - 恢复修改前的数据状态

    Transaction* txn = context->txn_;

    std::shared_ptr<std::deque<WriteRecord*>> write_set = txn->get_write_set();

    for (auto iter = write_set->rbegin(); iter != write_set->rend(); iter++) {
        const auto [write_type, table_name, rid, record] = (*iter)->GetAll();
        std::unique_ptr<RmFileHandle>& handle = sm_manager_->fhs_.at(table_name);

        switch (write_type) {
            case WType::INSERT_TUPLE: {
                auto log_record = std::make_unique<DeleteLogRecord>(txn->get_transaction_id(), record, rid, table_name);
                record_link_management(log_record.get(), log_manager, txn);

                //  TODO: 删除索引
                handle->delete_record(rid, context);
                break;
            }
            case WType::UPDATE_TUPLE: {
                auto old_record = handle->get_record(rid, context);
                auto log_record =
                    std::make_unique<UpdateLogRecord>(txn->get_transaction_id(), *old_record, record, rid, table_name);
                record_link_management(log_record.get(), log_manager, txn);

                //  TODO: 删除索引

                handle->update_record(rid, record.data, context);
                //  TODO: 添加索引

                break;
            }
            case WType::DELETE_TUPLE: {
                auto log_record = std::make_unique<InsertLogRecord>(txn->get_transaction_id(), record, rid, table_name);
                record_link_management(log_record.get(), log_manager, txn);

                // TODO: 添加索引
                handle->insert_record(rid, record.data);

                break;
            }
            default:
                throw InternalError("Invalid write type");
                break;
        }
    }
    write_set->clear();

    // 2. 释放所有锁
    // - 遍历并释放lock_set中的锁
    // - 通知锁管理器更新状态

    std::shared_ptr<std::unordered_set<LockDataId>> lock_set = txn->get_lock_set();
    for (const LockDataId& lock : *lock_set) {
        lock_manager_->unlock(txn, lock);
    }

    // 3. 资源清理
    // - 清空write_set和lock_set
    // - 释放相关内存

    txn->clear();

    // 4. 日志处理
    // - 创建ABORT类型日志记录
    // - 更新日志序列号链
    // - 将日志刷入磁盘

    AbortLogRecord* log = new AbortLogRecord(txn->get_transaction_id());
    record_link_management(log, log_manager, txn);

    // 5. 更新事务状态
    // - 将状态设置为ABORTED
    // - 从全局事务表中移除

    txn->set_state(TransactionState::ABORTED);

    // 6. MVCC相关清理（如果启用）
    // - 清理版本链
    // - 恢复时间戳状态
    // - 更新活跃事务水位线
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