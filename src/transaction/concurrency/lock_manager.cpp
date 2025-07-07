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
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);  // 使用unique_lock以支持condition_variable

    // 创建锁数据标识符( 行级锁
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);

    auto queue_it = lock_table_.find(lock_data_id);
    if (queue_it == lock_table_.end()) {
        // 如果锁表中没有该锁数据标识符，则创建一个新的锁请求队列
        queue_it = lock_table_
                       .emplace(std::piecewise_construct,
                                std::forward_as_tuple(lock_data_id),  // 构造 key
                                std::forward_as_tuple()               // 默认构造 value (LockRequestQueue)
                                )
                       .first;
    }
    LockRequestQueue& request_queue = queue_it->second;

    // 检查是否已经获得锁
    for (const auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id() && req.granted_) {
            if (req.lock_mode_ == LockManager::LockMode::EXCLUSIVE || req.lock_mode_ == LockManager::LockMode::SHARED) {
                return true;
            }
        }
    }

    // 检查是否冲突
    bool conflict = request_queue.exclusive_holder_ == -1 ? false : true;

    LockRequest current_request(txn->get_transaction_id(), LockManager::LockMode::SHARED);

    if (!conflict) {
        current_request.granted_ = true;
        request_queue.request_queue_.push_back(current_request);
        txn->get_lock_set()->insert(lock_data_id);
        return true;
    } else {
        // no-wait策略
        // return false;

        // wait-die策略
        request_queue.request_queue_.push_back(current_request);
        auto current_request_it = std::prev(request_queue.request_queue_.end());

        while (true) {
            // 首先检查wait-die策略：如果存在更老的事务（较小txn_id）持有排他锁，当前事务应该死亡
            bool should_die = false;
            if (request_queue.exclusive_holder_ != -1 && request_queue.exclusive_holder_ < txn->get_transaction_id()) {
                // 存在更老的事务持有排他锁，当前事务应该死亡
                should_die = true;
            }

            if (should_die) {
                // 移除当前请求并返回false
                if (current_request_it != request_queue.request_queue_.end() && !current_request_it->granted_) {
                    request_queue.request_queue_.erase(current_request_it);
                }
                return false;
            }

            request_queue.cv_.wait(lock, [&] {
                if (txn->get_state() == TransactionState::ABORTED) {
                    return true;
                }

                // 检查是否可以授予共享锁（只有排他锁会阻塞共享锁）
                return request_queue.exclusive_holder_ == -1;
            });

            // 检查事务是否被中止
            if (txn->get_state() == TransactionState::ABORTED) {
                if (current_request_it != request_queue.request_queue_.end() && !current_request_it->granted_) {
                    request_queue.request_queue_.erase(current_request_it);
                }
                return false;
            }

            // 再次检查是否存在冲突

            if (request_queue.exclusive_holder_ == -1) {
                current_request_it->granted_ = true;
                txn->get_lock_set()->insert(lock_data_id);
                return true;
            }
        }
    }
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 * @throws TransactionAbortException 如果事务状态不允许加锁
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);  // 1. Acquire global latch

    // 创建锁数据标识符( 行级锁
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);

    auto queue_it = lock_table_.find(lock_data_id);
    if (queue_it == lock_table_.end()) {
        // 如果锁表中没有该锁数据标识符，则创建一个新的锁请求队列
        queue_it = lock_table_
                       .emplace(std::piecewise_construct,
                                std::forward_as_tuple(lock_data_id),  // 构造 key
                                std::forward_as_tuple()               // 默认构造 value (LockRequestQueue)
                                )
                       .first;
    }
    LockRequestQueue& request_queue = queue_it->second;

    // 检查是否已经获得锁
    if (request_queue.exclusive_holder_ != -1 && request_queue.exclusive_holder_ == txn->get_transaction_id()) {
        return true;
    }

    // bool conflict = false;
    // for (const auto& req : request_queue.request_queue_) {
    //     if (req.granted_) {
    //         if (req.txn_id_ != txn->get_transaction_id()) {
    //             conflict = true;
    //             break;
    //         }

    //         if (req.txn_id_ == txn->get_transaction_id() && req.lock_mode_ == LockMode::SHARED) {
    //             conflict = true;
    //             break;
    //         }
    //     }
    // }
    //! MVCC 不加读锁
    bool conflict = request_queue.exclusive_holder_ == -1 ? false : true;

    LockRequest current_request(txn->get_transaction_id(), LockManager::LockMode::EXCLUSIVE);

    if (!conflict) {
        current_request.granted_ = true;
        request_queue.request_queue_.push_back(current_request);
        request_queue.exclusive_holder_ = txn->get_transaction_id();
        request_queue.exclusive_holder_it_ = std::prev(request_queue.request_queue_.end());
        txn->get_lock_set()->insert(lock_data_id);
        return true;
    } else {
        // no-wait策略
        // return false;

        // wait-die策略
        request_queue.request_queue_.push_back(current_request);
        auto current_request_it = std::prev(request_queue.request_queue_.end());

        while (true) {
            // 首先检查wait-die策略：如果存在更老的事务（较小txn_id）持有锁，当前事务应该死亡
            bool should_die = false;
            // 检查排他锁持有者
            if (request_queue.exclusive_holder_ != -1 && request_queue.exclusive_holder_ < txn->get_transaction_id()) {
                should_die = true;
            }
            //! MVCC没有读锁
            // else {
            //     // 检查共享锁持有者
            //     for (const auto& req : request_queue.request_queue_) {
            //         if (req.granted_ && req.lock_mode_ == LockManager::LockMode::SHARED && req.txn_id_ <
            //         txn->get_transaction_id()) {
            //             should_die = true;
            //             break;
            //         }
            //     }
            // }

            if (should_die) {
                // 移除当前请求并返回false
                if (current_request_it != request_queue.request_queue_.end() && !current_request_it->granted_) {
                    request_queue.request_queue_.erase(current_request_it);
                }
                return false;
            }

            request_queue.cv_.wait(lock, [&] {
                if (txn->get_state() == TransactionState::ABORTED) {
                    return true;
                }

                // 检查是否可以授予锁
                // bool can_grant = true;
                // for (const auto& req : request_queue.request_queue_) {
                //     if (req.granted_) {
                //         // 如果有其他事务持有锁（排他锁与任何锁冲突）
                //         if (req.txn_id_ != txn->get_transaction_id()) {
                //             can_grant = false;
                //             break;
                //         }
                //         // 如果当前事务持有共享锁，不能升级为排他锁
                //         if (req.txn_id_ == txn->get_transaction_id() && req.lock_mode_ == LockMode::SHARED) {
                //             can_grant = false;
                //             break;
                //         }
                //     }
                // }
                return request_queue.exclusive_holder_ == -1;
            });

            // 检查事务是否被中止
            if (txn->get_state() == TransactionState::ABORTED) {
                if (current_request_it != request_queue.request_queue_.end() && !current_request_it->granted_) {
                    request_queue.request_queue_.erase(current_request_it);
                }
                return false;
            }

            // 再次检查是否存在冲突
            // bool still_conflict = false;
            // for (const auto& req : request_queue.request_queue_) {
            //      if (req.granted_) {
            //          if (req.txn_id_ != txn->get_transaction_id()) {
            //              still_conflict = true;
            //              break;
            //          }
            //          if (req.txn_id_ == txn->get_transaction_id() && req.lock_mode_ == LockMode::SHARED) {
            //              still_conflict = true;
            //              break;
            //          }
            //      }
            // }

            if (request_queue.exclusive_holder_ == -1) {
                current_request_it->granted_ = true;
                request_queue.exclusive_holder_ = txn->get_transaction_id();
                request_queue.exclusive_holder_it_ = current_request_it;
                txn->get_lock_set()->insert(lock_data_id);
                return true;
            }
        }
    }
}

bool LockManager::is_lock_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::scoped_lock<std::mutex> lock(latch_);

    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);

    auto queue_it = lock_table_.find(lock_data_id);
    if (queue_it == lock_table_.end()) {
        return false;
    }
    LockRequestQueue& request_queue = queue_it->second;

    return request_queue.exclusive_holder_ == -1 || request_queue.exclusive_holder_ == txn->get_transaction_id();
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
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::scoped_lock<std::mutex> lock(latch_);

    auto lock_table_it = lock_table_.find(lock_data_id);
    if (lock_table_it == lock_table_.end()) {
        size_t locks_removed_from_txn = txn->get_lock_set()->erase(lock_data_id);
        return locks_removed_from_txn > 0;
    }

    LockRequestQueue& request_queue = lock_table_it->second;
    txn_id_t txn_id = txn->get_transaction_id();
    bool is_find = false;

    // 如果要释放的是排他锁，直接使用保存的迭代器
    if (request_queue.exclusive_holder_ == txn_id &&
        request_queue.exclusive_holder_it_ != request_queue.request_queue_.end()) {
        request_queue.request_queue_.erase(request_queue.exclusive_holder_it_);
        request_queue.exclusive_holder_ = -1;
        request_queue.exclusive_holder_it_ = request_queue.request_queue_.end();
        is_find = true;
    } else {
        // 否则遍历查找其他类型的锁
        for (auto it = request_queue.request_queue_.begin(); it != request_queue.request_queue_.end();) {
            if (it->txn_id_ == txn_id) {
                it = request_queue.request_queue_.erase(it);
                is_find = true;
                break;
            } else {
                ++it;
            }
        }
    }

    size_t is_erase = txn->get_lock_set()->erase(lock_data_id);

    if (is_find) {
        request_queue.cv_.notify_all();
    }

    if (request_queue.request_queue_.empty()) {
        lock_table_.erase(lock_table_it);
    }

    return is_erase > 0;
}

bool LockManager::lock_gap(Transaction* txn, int tab_fd, std::vector<Condition> conds) {
    std::scoped_lock<std::mutex> lock(latch_);

    auto queue_it = gap_lock_table_.find(tab_fd);
    if (queue_it == gap_lock_table_.end()) {
        queue_it =
            gap_lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(tab_fd), std::forward_as_tuple())
                .first;
    }
    GapLockRequestQueue& request_queue = queue_it->second;
    request_queue.request_queue_.emplace_back(txn->get_transaction_id(), std::move(conds));
    txn->get_lock_gap_set()->insert(tab_fd);
    return true;
}

std::vector<Condition> LockManager::get_gap_condition(int tab_fd, Transaction* txn) {
    std::scoped_lock<std::mutex> lock(latch_);
    std::vector<Condition> gap_conditions;
    auto table_queue_it = gap_lock_table_.find(tab_fd);
    if (table_queue_it == gap_lock_table_.end()) {
        return gap_conditions;
    }
    GapLockRequestQueue& request_queue = table_queue_it->second;
    for (const auto& gap_request : request_queue.request_queue_) {
        if (gap_request.txn_id_ == txn->get_transaction_id()) {
            continue;  // 只返回其他事务的间隙锁条件
        }
        gap_conditions.insert(gap_conditions.end(), gap_request.conds.begin(), gap_request.conds.end());
    }
    return gap_conditions;
}

bool LockManager::unlock_gap(Transaction* txn, int tab_fd) {
    std::scoped_lock<std::mutex> lock(latch_);

    auto table_queue_it = gap_lock_table_.find(tab_fd);
    if (table_queue_it == gap_lock_table_.end()) {
        txn->get_lock_gap_set()->erase(tab_fd);
        return false;
    }

    GapLockRequestQueue& request_queue = table_queue_it->second;
    for (auto it = request_queue.request_queue_.begin(); it != request_queue.request_queue_.end();) {
        if (it->txn_id_ == txn->get_transaction_id()) {
            it = request_queue.request_queue_.erase(it);
        } else {
            ++it;
        }
    }
    txn->get_lock_gap_set()->erase(tab_fd);
    if (request_queue.request_queue_.empty()) {
        gap_lock_table_.erase(table_queue_it);
    }

    return true;
}