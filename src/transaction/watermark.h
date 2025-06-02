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

#include <map>
#include <unordered_map>

#include "transaction/transaction.h"

/**
 * @brief 水位线管理类，用于追踪事务的时间戳并维护全局一致性
 *
 * 主要功能：
 * 1. 追踪活跃事务的读时间戳
 * 2. 维护全局水位线，确保数据版本的可见性
 * 3. 管理事务提交时间戳的更新
 *
 * 水位线的作用：
 * - 确保并发事务的正确性
 * - 支持MVCC的垃圾回收
 * - 维护数据一致性
 */
class Watermark {
   public:
    /**
     * @brief 构造函数，初始化水位线
     * @param commit_ts 初始提交时间戳
     */
    explicit Watermark(timestamp_t commit_ts) : commit_ts_(commit_ts), watermark_(commit_ts) {}

    /**
     * @brief 添加一个事务的读时间戳到水位线追踪中
     * @param read_ts 要添加的读时间戳
     */
    void AddTxn(timestamp_t read_ts);

    /**
     * @brief 从水位线追踪中移除一个事务的读时间戳
     * @param read_ts 要移除的读时间戳
     */
    void RemoveTxn(timestamp_t read_ts);

    /**
     * @brief 更新提交时间戳
     *
     * 调用者必须在从水位线中移除事务之前更新提交时间戳，
     * 这样才能正确追踪和维护水位线。
     *
     * @param commit_ts 新的提交时间戳
     */
    void UpdateCommitTs(timestamp_t commit_ts);

    /**
     * @brief 获取当前的水位线值
     * @return 返回当前的水位线时间戳
     */
    timestamp_t GetWatermark();

    mutable timestamp_t commit_ts_;             // 当前的提交时间戳
    timestamp_t watermark_;                     // 当前的水位线值
    std::map<timestamp_t, int> current_reads_;  // 当前活跃的读操作时间戳计数
};