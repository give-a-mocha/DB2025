/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "recovery/log_manager.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction.h"

// 事务管理器类的前向声明

// 用于数据发送的默认偏移量
static int const_offset = -1;

/**
 * @brief 上下文类，用于在数据库操作过程中传递关键组件和状态
 */
class Context {
   public:
    /**
     * @brief 构造函数
     * @param lock_mgr 锁管理器指针
     * @param log_mgr 日志管理器指针
     * @param txn 当前事务指针
     * @param data_send 数据发送缓冲区指针
     * @param offset 数据偏移量指针
     */
    Context(LockManager *lock_mgr, LogManager *log_mgr, Transaction *txn, char *data_send = nullptr,
            int *offset = &const_offset)
        : lock_mgr_(lock_mgr), log_mgr_(log_mgr), txn_(txn), data_send_(data_send), offset_(offset) {
        ellipsis_ = false;
    }

    // TransactionManager *txn_mgr_;  // 事务管理器指针
    LockManager *lock_mgr_;  // 锁管理器指针，用于并发控制
    LogManager *log_mgr_;    // 日志管理器指针，用于事务恢复
    Transaction *txn_;       // 当前事务指针
    char *data_send_;        // 数据发送缓冲区指针，用于返回数据
    int *offset_;            // 数据偏移量指针，指示数据在缓冲区中的位置
    bool ellipsis_;          // 省略标志，表示是否省略部分输出内容
};