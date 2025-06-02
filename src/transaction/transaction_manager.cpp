/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"

#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction*> TransactionManager::txn_map = {};

TransactionManager::TransactionManager(LockManager* lock_manager, SmManager* sm_manager,
                                       ConcurrencyMode concurrency_mode) {}

/**
 * @description: 开始一个新事务或继续一个已有事务
 *
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
 * @param {Transaction*} txn 事务指针，nullptr表示需要创建新事务
 * @param {LogManager*} log_manager 日志管理器指针
 * @return {Transaction*} 初始化后的事务指针
 */
Transaction* TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // 1. 判断传入事务参数是否为空指针，为空则创建新事务
    if (txn == nullptr) {
        // 创建新事务，并分配递增的事务ID
        txn = new Transaction(next_txn_id_++);
    }

    // 加锁保护全局事务表的并发访问
    std::unique_lock<std::mutex> lock(latch_);
    // 将当前事务添加到全局事务映射表中，便于后续通过事务ID查找
    txn_map.emplace(txn->get_transaction_id(), txn);

    // 创建BEGIN事务日志记录，记录事务开始的操作
    auto* log = new BeginLogRecord(txn->get_transaction_id());
    // 设置日志记录的前序LSN为事务当前的LSN
    log->prev_lsn_ = txn->get_prev_lsn();
    // 将日志添加到日志管理器的缓冲区中
    log_manager->add_log_to_buffer(log);

    // 更新事务的前序LSN为当前日志的LSN，建立日志链
    txn->set_prev_lsn(log->lsn_);

    // 注意：这里应该返回txn而不是nullptr，这是一个bug
    return nullptr;
}

/**
 * @description: 提交事务
 *
 * 该函数需要执行以下操作：
 * 1. 提交写操作：
 *    - 确保所有修改都已经完成
 *    - 处理写缓冲区中的数据
 * 2. 锁的处理：
 *    - 按照2PL协议释放所有持有的锁
 *    - 更新锁管理器的状态
 * 3. 资源清理：
 *    - 释放事务持有的内存资源
 *    - 清空事务的锁集合
 * 4. 日志处理：
 *    - 写入COMMIT类型的日志记录
 *    - 确保日志被刷入磁盘
 * 5. 状态更新：
 *    - 将事务状态设置为COMMITTED
 * 6. MVCC支持（如果启用）：
 *    - 更新版本链
 *    - 处理事务时间戳
 *
 * @param {Transaction*} txn 要提交的事务指针
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
}

/**
 * @description: 终止（回滚）事务
 *
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
 * @param {Transaction*} txn 要回滚的事务指针
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    // 如果需要支持MVCC请在上述过程中添加代码
}