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

#include "log_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"

/**
 * @brief 页面重做日志管理类
 *
 * 用于管理针对单个页面的重做日志操作，包含：
 * 1. 对应表文件的句柄
 * 2. 需要在该页面上重做的日志序列号列表
 *
 * 主要用于：
 * - 崩溃恢复时的REDO阶段
 * - 跟踪每个页面待重做的操作
 * - 优化重做操作的执行顺序
 */
class RedoLogsInPage {
   public:
    RedoLogsInPage() { table_file_ = nullptr; }
    RmFileHandle* table_file_;      // 表文件句柄
    std::vector<lsn_t> redo_logs_;  // 需要在页面上重做的日志LSN列表
};

/**
 * @brief 数据库恢复管理器类

 */
class RecoveryManager {
   public:
    /**
     * @brief 构造函数
     * @param disk_manager 磁盘管理器，用于日志文件读写
     * @param buffer_pool_manager 缓冲池管理器，用于页面访问
     * @param sm_manager 系统管理器，用于访问数据库元数据
     */
    RecoveryManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, SmManager* sm_manager) {
        disk_manager_ = disk_manager;
        buffer_pool_manager_ = buffer_pool_manager;
        sm_manager_ = sm_manager;
    }

    /**
     * @brief 执行分析阶段，重建崩溃时的状态信息
     */
    void analyze();

    /**
     * @brief 执行重做阶段，重演历史操作
     */
    void redo();

    /**
     * @brief 执行撤销阶段，回滚未完成事务
     */
    void undo();

   private:
    LogBuffer buffer_;                        // 日志缓冲区，用于读取日志
    DiskManager* disk_manager_;               // 磁盘管理器，处理日志文件IO
    BufferPoolManager* buffer_pool_manager_;  // 缓冲池管理器，处理页面操作
    SmManager* sm_manager_;                   // 系统管理器，访问数据库元数据
};