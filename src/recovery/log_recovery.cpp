/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_recovery.h"
#include "common/print.hpp"

void RecoveryManager::recovery() {
    size_t log_start_offset = sm_manager_->db_.get_log_offset();
    auto log_records_ = log_mgr_->read_logs_from_disk(log_start_offset);
    std::unordered_set<txn_id_t> uncommitted_txns;  // 用于记录未提交的事务ID
    std::unordered_map<std::string, int> tab_page_num;

    // 辅助lambda函数：处理表操作日志记录（INSERT、DELETE、UPDATE）
    auto process_table_operation = [&](auto *log_record_) {
        std::string table_name = std::string(log_record_->table_name_, log_record_->table_name_size_);
        if (sm_manager_->fhs_.find(table_name) != sm_manager_->fhs_.end()) {
            tab_page_num[table_name] = std::max(tab_page_num[table_name], log_record_->rid_.page_no);
        }
    };

    for (const auto &log_record : log_records_) {
        switch (log_record->log_type_) {
            case LogType::BEGIN: {
                uncommitted_txns.insert(log_record->log_tid_);
                break;
            }
            case LogType::COMMIT: {
                uncommitted_txns.erase(log_record->log_tid_);
                break;
            }
            case LogType::ABORT: {
                uncommitted_txns.erase(log_record->log_tid_);
                break;
            }
            case LogType::INSERT: {
                auto log_record_ = dynamic_cast<InsertLogRecord *>(log_record.get());
                process_table_operation(log_record_);
                break;
            }
            case LogType::DELETE: {
                auto log_record_ = dynamic_cast<DeleteLogRecord *>(log_record.get());
                process_table_operation(log_record_);
                break;
            }
            case LogType::UPDATE: {
                auto log_record_ = dynamic_cast<UpdateLogRecord *>(log_record.get());
                process_table_operation(log_record_);
                break;
            }
            default: {
                throw RMDBError("not supported log type");
            }
        }
    }
    // 新建页
    for (const auto &[tab_name, page_number] : tab_page_num) {
        auto fh_ = sm_manager_->fhs_.at(tab_name).get();
        while (fh_->get_file_hdr().num_pages <= page_number) {
            auto page_hdr_ = fh_->create_new_page_handle();
            buffer_pool_manager_->unpin_page(page_hdr_.page->get_page_id(), false);
        }
    }

    for (const auto &log_record : log_records_) {
        if (log_record->log_type_ == LogType::BEGIN || log_record->log_type_ == LogType::COMMIT ||
            log_record->log_type_ == LogType::ABORT) {
            continue;
        }
        redo(log_record.get());
    }

    const int size = log_records_.size();
    for (int i = size - 1; i >= 0; --i) {
        auto &log_record = log_records_[i];
        if (log_record->log_type_ == LogType::BEGIN || log_record->log_type_ == LogType::COMMIT ||
            log_record->log_type_ == LogType::ABORT) {
            continue;
        }
        if (uncommitted_txns.find(log_record->log_tid_) != uncommitted_txns.end()) {
            undo(log_record.get());
        }
    }
    for (const auto &[tab_name, tab_meta] : sm_manager_->db_.tabs_) {
        std::vector<IndexMeta> indexes;
        indexes.reserve(tab_meta.indexes.size());
        for (auto &index_ : tab_meta.indexes) {
            indexes.emplace_back(index_);
        }
        for (const auto &index_ : indexes) {
            sm_manager_->drop_index(index_.tab_name, index_.cols, nullptr);
            std::vector<std::string> col_names_;
            col_names_.reserve(index_.cols.size());
            for (const auto &col : index_.cols) {
                col_names_.emplace_back(col.name);
            }
            sm_manager_->create_index(index_.tab_name, col_names_, nullptr);
        }
    }

    flush_to_disk();
    log_records_.clear();
}

void RecoveryManager::flush_to_disk() {
    sm_manager_->flush_to_disk();
    char *STATIC_CHECK_POINT_STR = "[[STATIC_CHECK_POINT]]\n\n";
    disk_manager_->write_log(STATIC_CHECK_POINT_STR, std::strlen(STATIC_CHECK_POINT_STR));
    sm_manager_->set_log_offset(disk_manager_->get_file_size(LOG_FILE_NAME));
}
/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo(LogRecord *log_record) {
    switch (log_record->log_type_) {
        case LogType::INSERT: {
            auto insert_log_record_ = dynamic_cast<InsertLogRecord *>(log_record);
            std::string table_name = std::string(insert_log_record_->table_name_, insert_log_record_->table_name_size_);
            if (sm_manager_->fhs_.find(table_name) == sm_manager_->fhs_.end()) {
                return;
            }
            // 在原本位置插入值
            sm_manager_->fhs_.at(table_name)
                ->insert_record_force(insert_log_record_->rid_, insert_log_record_->insert_value_.data);
            break;
        }
        case LogType::DELETE: {
            auto delete_log_record_ = dynamic_cast<DeleteLogRecord *>(log_record);
            std::string table_name = std::string(delete_log_record_->table_name_, delete_log_record_->table_name_size_);
            if (sm_manager_->fhs_.find(table_name) == sm_manager_->fhs_.end()) {
                return;
            }
            sm_manager_->fhs_.at(table_name)->delete_record(delete_log_record_->rid_, nullptr);
            break;
        }
        case LogType::UPDATE: {
            auto update_log_record_ = dynamic_cast<UpdateLogRecord *>(log_record);
            std::string table_name = std::string(update_log_record_->table_name_, update_log_record_->table_name_size_);
            if (sm_manager_->fhs_.find(table_name) == sm_manager_->fhs_.end()) {
                return;
            }
            // 在原本位置更新值
            sm_manager_->fhs_.at(table_name)
                ->update_record(update_log_record_->rid_, update_log_record_->after_value_.data, nullptr);
            break;
        }
        default: {
            throw RMDBError("not supported log type");
        }
    }
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo(LogRecord *log_record) {
    switch (log_record->log_type_) {
        case LogType::INSERT: {
            auto insert_log_record_ = dynamic_cast<InsertLogRecord *>(log_record);
            std::string table_name = std::string(insert_log_record_->table_name_, insert_log_record_->table_name_size_);
            if (sm_manager_->fhs_.find(table_name) == sm_manager_->fhs_.end()) {
                return;
            }
            // 在原本位置插入值
            sm_manager_->fhs_.at(table_name)->delete_record(insert_log_record_->rid_, nullptr);
            break;
        }
        case LogType::DELETE: {
            auto delete_log_record_ = dynamic_cast<DeleteLogRecord *>(log_record);
            std::string table_name = std::string(delete_log_record_->table_name_, delete_log_record_->table_name_size_);
            if (sm_manager_->fhs_.find(table_name) == sm_manager_->fhs_.end()) {
                return;
            }
            sm_manager_->fhs_.at(table_name)
                ->insert_record_force(delete_log_record_->rid_, delete_log_record_->delete_value_.data);
            break;
        }
        case LogType::UPDATE: {
            auto update_log_record_ = dynamic_cast<UpdateLogRecord *>(log_record);
            std::string table_name = std::string(update_log_record_->table_name_, update_log_record_->table_name_size_);
            if (sm_manager_->fhs_.find(table_name) == sm_manager_->fhs_.end()) {
                return;
            }
            // 在原本位置更新值
            sm_manager_->fhs_.at(table_name)
                ->update_record(update_log_record_->rid_, update_log_record_->before_value_.data, nullptr);
            break;
        }
        default: {
            throw RMDBError("not supported log type");
        }
    }
}

void RecoveryManager::create_static_check_point() {
    // （1）停止接收新事务和正在运行事务
    // （2）将仍保留在日志缓冲区中的内容写到日志文件中；
    // （3）在日志文件中写入一个“检查点记录”；
    // （4）将当前数据库缓冲区中的内容写到数据库中；
    // （5）把日志文件中检查点记录的地址写到“重新启动文件”中。

    std::unique_lock lock_(latch_);
    std::unique_lock lock(log_mgr_->latch_);
    auto log_records_ = log_mgr_->read_logs_from_disk(sm_manager_->db_.get_log_offset());
    log_mgr_->flush_log_to_disk_without_lock();
    flush_to_disk();
    // 在静态检查点之前的，未提交事务，应该添加
    std::unordered_set<txn_id_t> committed_txns;
    for (const auto &log_record : log_records_) {
        if (log_record->log_type_ == LogType::COMMIT || log_record->log_type_ == LogType::ABORT) {
            committed_txns.insert(log_record->log_tid_);
        }
    }
    for (const auto &log_record : log_records_) {
        if (committed_txns.find(log_record->log_tid_) == committed_txns.end()) {
            log_mgr_->add_log_to_buffer_without_lock(log_record.get());
        }
    }
    log_mgr_->flush_log_to_disk_without_lock();
}