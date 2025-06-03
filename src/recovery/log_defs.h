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

#include <atomic>
#include <chrono>

#include "common/config.h"
#include "defs.h"
#include "storage/disk_manager.h"

/**
 * @brief 日志刷盘超时时间，超过此时间的日志需要强制刷盘
 */
static constexpr std::chrono::duration<int64_t> FLUSH_TIMEOUT = std::chrono::seconds(3);

/**
 * @brief 日志记录头部的偏移量定义
 * 用于序列化和反序列化日志记录时定位各字段位置
 */
// 日志类型偏移量(LogType)
static constexpr int OFFSET_LOG_TYPE = 0;
// 日志序列号偏移量(LSN)
static constexpr int OFFSET_LSN = sizeof(int);
// 日志总长度偏移量(包含头部和数据)
static constexpr int OFFSET_LOG_TOT_LEN = OFFSET_LSN + sizeof(lsn_t);
// 事务ID偏移量
static constexpr int OFFSET_LOG_TID = OFFSET_LOG_TOT_LEN + sizeof(uint32_t);
// 前一条日志LSN偏移量(用于事务日志链)
static constexpr int OFFSET_PREV_LSN = OFFSET_LOG_TID + sizeof(txn_id_t);
// 日志数据部分起始偏移量
static constexpr int OFFSET_LOG_DATA = OFFSET_PREV_LSN + sizeof(lsn_t);
// 日志头部总大小
static constexpr int LOG_HEADER_SIZE = OFFSET_LOG_DATA;
