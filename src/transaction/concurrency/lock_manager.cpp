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

/**
 * @description: 申请行级共享锁(S锁)
 *
 * 该方法实现以下功能：
 * 1. 检查锁的合法性：
 *    - 确认事务状态
 *    - 验证是否符合两阶段锁协议
 * 2. 检查锁的兼容性：
 *    - 与已授予的锁检查兼容性
 *    - S锁可以与其他S锁共存
 *    - S锁与X锁互斥
 * 3. 处理锁请求：
 *    - 如果兼容，直接授予锁
 *    - 如果不兼容，将请求加入等待队列
 * 4. 死锁预防：
 *    - 实现等待-死亡机制
 *    - 或实现wound-wait机制
 *
 * @param {Transaction*} txn 申请加锁的事务
 * @param {Rid&} rid 目标记录的ID
 * @param {int} tab_fd 表文件描述符
 * @return {bool} true表示加锁成功，false表示加锁失败
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) { return true; }

/**
 * @description: 申请行级排他锁(X锁)
 *
 * 该方法实现以下功能：
 * 1. 检查锁的合法性：
 *    - 确认事务状态
 *    - 验证是否符合两阶段锁协议
 * 2. 检查锁的兼容性：
 *    - X锁与任何其他锁都互斥
 *    - 必须等待所有已授予的锁释放
 * 3. 处理锁请求：
 *    - 如果记录上没有任何锁，直接授予
 *    - 否则将请求加入等待队列
 * 4. 死锁预防：
 *    - 检查是否会造成死锁
 *    - 必要时回滚事务
 *
 * @param {Transaction*} txn 申请加锁的事务
 * @param {Rid&} rid 目标记录的ID
 * @param {int} tab_fd 表文件描述符
 * @return {bool} true表示加锁成功，false表示加锁失败
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) { return true; }

/**
 * @description: 申请表级共享锁(表级S锁)
 *
 * 该方法实现以下功能：
 * 1. 检查锁的合法性：
 *    - 验证事务状态
 *    - 检查是否符合意向锁协议
 * 2. 检查锁的兼容性：
 *    - 与表上已有的锁检查兼容性
 *    - S锁与IS、S锁兼容
 *    - S锁与IX、X、SIX锁互斥
 * 3. 锁升级处理：
 *    - 如果事务已持有IS锁，可以升级为S锁
 *    - 处理锁升级过程中的并发问题
 *
 * @param {Transaction*} txn 申请加锁的事务
 * @param {int} tab_fd 目标表的文件描述符
 * @return {bool} true表示加锁成功，false表示加锁失败
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) { return true; }

/**
 * @description: 申请表级排他锁(表级X锁)
 *
 * 该方法实现以下功能：
 * 1. 检查锁的合法性：
 *    - 验证事务状态
 *    - 检查是否符合意向锁协议
 * 2. 检查锁的兼容性：
 *    - X锁与任何其他类型的锁都互斥
 *    - 必须等待表上所有锁释放
 * 3. 锁升级处理：
 *    - 如果事务已持有IX锁，可以升级为X锁
 *    - 确保升级过程的原子性
 *
 * @param {Transaction*} txn 申请加锁的事务
 * @param {int} tab_fd 目标表的文件描述符
 * @return {bool} true表示加锁成功，false表示加锁失败
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) { return true; }

/**
 * @description: 申请表级意向共享锁(IS锁)
 *
 * 该方法实现以下功能：
 * 1. 意向锁规则检查：
 *    - 在对记录加S锁之前必须先获得IS锁
 *    - IS锁与IS、IX、S锁兼容
 *    - IS锁与X锁互斥
 * 2. 处理锁请求：
 *    - 检查当前表上的锁兼容性
 *    - 必要时将请求加入等待队列
 *
 * @param {Transaction*} txn 申请加锁的事务
 * @param {int} tab_fd 目标表的文件描述符
 * @return {bool} true表示加锁成功，false表示加锁失败
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) { return true; }

/**
 * @description: 申请表级意向排他锁(IX锁)
 *
 * 该方法实现以下功能：
 * 1. 意向锁规则检查：
 *    - 在对记录加X锁之前必须先获得IX锁
 *    - IX锁与IS、IX锁兼容
 *    - IX锁与S、X、SIX锁互斥
 * 2. 处理锁请求：
 *    - 检查当前表上的锁兼容性
 *    - 处理锁等待和死锁问题
 *
 * @param {Transaction*} txn 申请加锁的事务
 * @param {int} tab_fd 目标表的文件描述符
 * @return {bool} true表示加锁成功，false表示加锁失败
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) { return true; }

/**
 * @description: 释放指定的锁
 *
 * 该方法实现以下功能：
 * 1. 验证锁的所有权：
 *    - 确认是否是当前事务持有的锁
 *    - 检查锁释放是否符合两阶段锁协议
 * 2. 释放锁：
 *    - 从锁表中移除锁请求
 *    - 更新锁请求队列的状态
 * 3. 处理等待的锁请求：
 *    - 按照锁的兼容性唤醒等待的事务
 *    - 确保公平性和无饥饿
 *
 * @param {Transaction*} txn 要释放锁的事务
 * @param {LockDataId} lock_data_id 要释放的锁标识符
 * @return {bool} true表示解锁成功，false表示解锁失败
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) { return true; }