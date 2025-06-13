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

#include "transaction/txn_defs.h"
#include <mutex>
#include <algorithm>
#include <tuple>
#include <utility>

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_); // 1. Acquire global latch

    // 两阶段锁协议
    if (txn->get_state() == TransactionState::SHRINKING ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::ABORTED) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    if (txn->get_state() == TransactionState::DEFAULT) {
        txn->set_state(TransactionState::GROWING);
    }

    //创建锁数据标识符( 行级锁
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);

    auto queue_it = lock_table_.find(lock_data_id);
    if(queue_it == lock_table_.end()) {
        // 如果锁表中没有该锁数据标识符，则创建一个新的锁请求队列
        queue_it = lock_table_.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(lock_data_id), // 构造 key
            std::forward_as_tuple()             // 默认构造 value (LockRequestQueue)
        ).first;
    }
    LockRequestQueue& request_queue = queue_it->second;

    // 检查是否已经获得锁
    for(const auto& req : request_queue.request_queue_) {
        if(req.txn_id_ == txn->get_transaction_id() && req.granted_) {
            if (req.lock_mode_ == LockManager::LockMode::EXCLUSIVE || req.lock_mode_ == LockManager::LockMode::SHARED) {
                return true;
            }
        }
    }


    // 检查是否冲突
    bool conflict = false;
    for (const auto& req : request_queue.request_queue_) {
        if (req.granted_ && req.lock_mode_ == LockManager::LockMode::EXCLUSIVE) {
            conflict = true;
            break;
        }
    }

    LockRequest current_request(txn->get_transaction_id(), LockManager::LockMode::SHARED);

    if (!conflict) {
        current_request.granted_ = true;
        request_queue.request_queue_.push_back(current_request);
        txn->get_lock_set()->insert(lock_data_id);
        return true;
    } else {
        //等待
        request_queue.request_queue_.push_back(current_request);
        
        auto current_request_it = std::prev(request_queue.request_queue_.end());

        while (true) {
            //等待知道锁被授予或事务被中止
            //wait-die策略
            request_queue.cv_.wait(lock, [&] {
                if (txn->get_state() == TransactionState::ABORTED) {
                    return true;
                }
                bool can_grant = true;
                for (const auto& req : request_queue.request_queue_) {
                    if (req.granted_ && req.lock_mode_ == LockManager::LockMode::EXCLUSIVE) {
                        if(req.txn_id_ > txn->get_transaction_id()) {
                            can_grant = false;
                        }else{
                            can_grant = true;
                        }
                        break;
                    }
                }
                return can_grant;
            });

            bool is_aborted = false;
            if(txn->get_state() == TransactionState::ABORTED) {
                is_aborted = true;
            }
            for (const auto& req : request_queue.request_queue_) {
                 if (req.granted_ && req.lock_mode_ == LockManager::LockMode::EXCLUSIVE) {
                    if(req.txn_id_ < txn->get_transaction_id()) {
                        is_aborted = true;
                        break;
                    }
                 }
            }

            if (is_aborted) {
                 if (current_request_it != request_queue.request_queue_.end() && !current_request_it->granted_) {
                     request_queue.request_queue_.erase(current_request_it);
                 }
                return false;
            }

            bool still_conflict = false;
            for (const auto& req : request_queue.request_queue_) {
                 if (req.granted_ && req.lock_mode_ == LockManager::LockMode::EXCLUSIVE) {
                    still_conflict = true;
                    break;
                 }
            }

            if (!still_conflict) {
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
    std::unique_lock<std::mutex> lock(latch_); // 1. Acquire global latch

    // 两阶段锁协议
    if (txn->get_state() == TransactionState::SHRINKING ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::ABORTED) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    if (txn->get_state() == TransactionState::DEFAULT) {
        txn->set_state(TransactionState::GROWING);
    }

    //创建锁数据标识符( 行级锁
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);

    auto queue_it = lock_table_.find(lock_data_id);
    if(queue_it == lock_table_.end()) {
        // 如果锁表中没有该锁数据标识符，则创建一个新的锁请求队列
        queue_it = lock_table_.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(lock_data_id), // 构造 key
            std::forward_as_tuple()             // 默认构造 value (LockRequestQueue)
        ).first;
    }
    LockRequestQueue& request_queue = queue_it->second;

    // 检查是否已经获得锁
    for(const auto& req : request_queue.request_queue_) {
        if(req.txn_id_ == txn->get_transaction_id() && req.granted_) {
            if (req.lock_mode_ == LockManager::LockMode::EXCLUSIVE) {
                return true;
            }
        }
    }

    
    bool conflict = false;
    for (const auto& req : request_queue.request_queue_) {
        if (req.granted_) {
             if (req.txn_id_ != txn->get_transaction_id()) {
                 conflict = true;
                 break;
             }
             
             if (req.txn_id_ == txn->get_transaction_id() && req.lock_mode_ == LockMode::SHARED) {
                  conflict = true;
                  break;
             }
        }
    }

    LockRequest current_request(txn->get_transaction_id(), LockManager::LockMode::EXCLUSIVE);

    if (!conflict) {
        current_request.granted_ = true;
        request_queue.request_queue_.push_back(current_request);
        txn->get_lock_set()->insert(lock_data_id);
        return true;
    } else {
        request_queue.request_queue_.push_back(current_request);
        auto current_request_it = std::prev(request_queue.request_queue_.end());

        while (true) {
            
            request_queue.cv_.wait(lock, [&] {
                if (txn->get_state() == TransactionState::ABORTED) {
                    return true;
                }
                bool can_grant = true;
                for (const auto& req : request_queue.request_queue_) {
                    if (req.granted_) {
                        if(req.txn_id_ < txn->get_transaction_id()) {
                            can_grant = true;
                            break;
                        }
                        if (req.txn_id_ != txn->get_transaction_id()) {
                            can_grant = false; 
                            break;
                        }
                        if (req.txn_id_ == txn->get_transaction_id() && req.lock_mode_ == LockMode::SHARED) {
                            can_grant = false;
                            break;
                        }
                    }
                }
                 return can_grant; 
            });

            bool is_aborted = false;
            if(txn->get_state() == TransactionState::ABORTED) {
                is_aborted = true;
            }
            for (const auto& req : request_queue.request_queue_) {
                 if (req.granted_ && req.lock_mode_ == LockManager::LockMode::EXCLUSIVE) {
                    if(req.txn_id_ < txn->get_transaction_id()) {
                        is_aborted = true;
                        break;
                    }
                 }
            }

            if (is_aborted) {
                 if (current_request_it != request_queue.request_queue_.end() && !current_request_it->granted_) {
                     request_queue.request_queue_.erase(current_request_it);
                 }
                return false;
            }

            bool still_conflict = false;
            for (const auto& req : request_queue.request_queue_) {
                 if (req.granted_) {
                     if (req.txn_id_ != txn->get_transaction_id()) {
                         still_conflict = true; 
                         break;
                     }
                     if (req.txn_id_ == txn->get_transaction_id() && req.lock_mode_ == LockMode::SHARED) {
                         still_conflict = true; 
                         break;
                     }
                 }
            }

            if (!still_conflict) {
                current_request_it->granted_ = true; 
                txn->get_lock_set()->insert(lock_data_id);
                return true;
            }
        }
    }
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    
    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock(latch_); 

    auto lock_table_it = lock_table_.find(lock_data_id);
    if (lock_table_it == lock_table_.end()) {
        return false;
    }

    LockRequestQueue& request_queue = lock_table_it->second;
    txn_id_t txn_id = txn->get_transaction_id();
    bool is_find = false;

    
    for (auto it = request_queue.request_queue_.begin(); it != request_queue.request_queue_.end(); ) {
        if (it->txn_id_ == txn_id) {
            it = request_queue.request_queue_.erase(it);
            is_find = true;
            break;
        } else {
            ++it;
        }
    }
    
    if (is_find) {
        request_queue.cv_.notify_all();
    }

    if (request_queue.request_queue_.empty()) {
        lock_table_.erase(lock_table_it);
    }

    return true;
}