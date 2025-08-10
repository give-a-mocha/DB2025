/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

#include <algorithm>
#include <mutex>
#include <tuple>
#include <utility>

#include "transaction/txn_defs.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) { return true; }

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 * @throws TransactionAbortException 如果事务状态不允许加锁
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);

    // 创建锁数据标识符（行级锁）
    LockDataId lock_data_id(tab_fd, rid);

    auto lock_it = lock_table_.find(lock_data_id);
    if (lock_it == lock_table_.end()) {
        // 如果锁表中没有该锁数据标识符，则创建一个新的锁信息
        lock_it = lock_table_
                       .emplace(std::piecewise_construct,
                                std::forward_as_tuple(lock_data_id),  // 构造 key
                                std::forward_as_tuple()               // 默认构造 value (LockInfo)
                                )
                       .first;
    }
    LockInfo& lock_info = lock_it->second;

    // 检查是否已经获得锁
    if (lock_info.exclusive_holder_ == txn->get_transaction_id()) {
        return true;
    }

    // 检查是否有冲突
    if (lock_info.exclusive_holder_ != -1) {
        // no-wait策略：如果有冲突直接返回false
        return false;
    }

    // 没有冲突，直接获得锁
    lock_info.exclusive_holder_ = txn->get_transaction_id();
    txn->get_lock_set().emplace_back(lock_data_id);  // 将锁添加到事务的锁集合中
    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) { return true; }

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) { return true; }

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) { return true; }

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) { return true; }

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
void LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock(latch_);

    auto lock_table_it = lock_table_.find(lock_data_id);
    if (lock_table_it == lock_table_.end()) {
        return ;
    }
    LockInfo& lock_info = lock_table_it->second;
    // 释放锁
    lock_info.exclusive_holder_ = -1;
    // 通知等待的线程
    lock_info.cv_.notify_all();
    lock_table_.erase(lock_table_it);
    return ;
}

void LockManager::wait_for_lock_release(LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock(latch_);

    auto lock_it = lock_table_.find(lock_data_id);
    if (lock_it == lock_table_.end()) {
        return;
    }

    LockInfo& lock_info = lock_it->second;
    if (lock_info.exclusive_holder_ == -1) {
        return;
    }

    lock_info.cv_.wait(lock, [&] {
        auto current_it = lock_table_.find(lock_data_id);
        if (current_it == lock_table_.end()) {
            return true;
        }
        return current_it->second.exclusive_holder_ == -1;
    });
}