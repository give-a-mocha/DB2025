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
#include "transaction/transaction_manager.h"

extern DiskManager disk_manager;
extern BufferPoolManager buffer_pool_manager;
extern SmManager sm_manager;
extern LogManager log_manager;

class RedoLogsInPage {
   public:
    RedoLogsInPage() { table_file_ = nullptr; }
    RmFileHandle* table_file_;
    std::vector<lsn_t> redo_logs_;  // 在该page上需要redo的操作的lsn
};

class RecoveryManager {
   private:
    LogBuffer buffer_;  // 读入日志

    std::shared_mutex latch_;

   public:
    RecoveryManager() = default;

    void recovery();

    void flush_to_disk();

    void redo(LogRecord* log_record);

    void undo(LogRecord* log_record);

    void create_static_check_point();
};