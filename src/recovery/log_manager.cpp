/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_manager.h"

#include <cstring>
#include <memory>

/**
 * @description: 添加日志记录到日志缓冲区中
 *
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回分配给该日志的序列号
 *
 * @thread_safety 通过互斥锁保护并发访问
 */
void LogManager::add_log_to_buffer(LogRecord *log_record) {
    std::scoped_lock<std::mutex> lock(latch_);  // 加锁以确保线程安全
    add_log_to_buffer_without_lock(log_record);
}

// 优化的日志添加方法，类型安全且高效
void LogManager::add_log_to_buffer_without_lock(LogRecord *log_record) {
    // 检查缓冲区空间是否足够
    if (log_buffer_.is_full(log_record->log_tot_len_)) {
        flush_log_to_disk_without_lock();
    }
    
    // 使用虚函数多态机制，这比dynamic_cast更高效
    // 每个子类都正确重写了serialize方法，会自动调用正确的版本
    log_record->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
    log_buffer_.offset_ += log_record->log_tot_len_;
    
    // 超过一半写入磁盘
    if (log_buffer_.offset_ > (LOG_BUFFER_SIZE >> 1)) {
        flush_log_to_disk_without_lock();
    }
}

/**
 * @description: 将日志缓冲区内容刷写到磁盘
 * @warning 由于系统只有一个日志缓冲区：
 * 1. 此操作会暂时阻塞其他日志写入
 * 2. 调用此函数时需要持有缓冲区的互斥锁
 * 3. 确保在适当的时机触发刷盘以平衡性能和持久性
 */
void LogManager::flush_log_to_disk() {
    std::scoped_lock<std::mutex> lock(latch_);
    flush_log_to_disk_without_lock();
}

void LogManager::flush_log_to_disk_without_lock() {
    if (log_buffer_.offset_ == 0) return;  // 优化：避免不必要的磁盘写入
    
    // 将缓冲区内容写入磁盘
    disk_manager_->write_log(log_buffer_.buffer_, static_cast<int>(log_buffer_.offset_));
    log_buffer_.reset();
}

void LogManager::add_insert_log(txn_id_t txn_id, std::unique_ptr<RmRecord> insert_value, const Rid &rid,
                                const std::string &table_name) {
    InsertLogRecord insert_log(txn_id, std::move(insert_value), rid, table_name);
    add_log_to_buffer(&insert_log);
}

void LogManager::add_delete_log(txn_id_t txn_id, std::unique_ptr<RmRecord> delete_value, const Rid &rid,
                                const std::string &table_name) {
    DeleteLogRecord delete_log(txn_id, std::move(delete_value), rid, table_name);
    add_log_to_buffer(&delete_log);
}

void LogManager::add_update_log(txn_id_t txn_id, std::unique_ptr<RmRecord> new_rec, std::unique_ptr<RmRecord> old_rec, const Rid &rid,
                                const std::string &table_name) {
    UpdateLogRecord update_log(txn_id, std::move(old_rec), std::move(new_rec), rid, table_name);
    add_log_to_buffer(&update_log);
}

void LogManager::add_begin_log(txn_id_t txn_id) {
    BeginLogRecord begin_log(txn_id);
    add_log_to_buffer(&begin_log);
}

void LogManager::add_commit_log(txn_id_t txn_id) {
    CommitLogRecord commit_log(txn_id);
    add_log_to_buffer(&commit_log);
}

void LogManager::add_abort_log(txn_id_t txn_id) {
    AbortLogRecord abort_log(txn_id);
    add_log_to_buffer(&abort_log);
}

std::vector<std::unique_ptr<LogRecord>> LogManager::read_logs_from_disk(size_t offset) {
    std::vector<std::unique_ptr<LogRecord>> log_records_;
    const auto file_size_ = static_cast<size_t>(disk_manager_->get_file_size(LOG_FILE_NAME));
    if (file_size_ <= offset) return log_records_;  // 优化：早期返回
    
    // 优化：预估容器大小，减少重新分配
    log_records_.reserve((file_size_ - offset) / LOG_HEADER_SIZE);
    
    auto buffer = std::make_unique<char[]>(file_size_);
    disk_manager_->read_log(buffer.get(), file_size_, static_cast<int>(offset));

    auto start_offset = offset;
    while (offset < file_size_) {
        // 优化：直接读取日志类型，避免创建临时对象
        LogType log_type = *reinterpret_cast<const LogType*>(buffer.get() + offset - start_offset);
        
        std::unique_ptr<LogRecord> log_record_;
        switch (log_type) {
            case LogType::BEGIN: {
                log_record_ = std::make_unique<BeginLogRecord>();
                break;
            }
            case LogType::COMMIT: {
                log_record_ = std::make_unique<CommitLogRecord>();
                break;
            }
            case LogType::ABORT: {
                log_record_ = std::make_unique<AbortLogRecord>();
                break;
            }
            case LogType::DELETE: {
                log_record_ = std::make_unique<DeleteLogRecord>();
                break;
            }
            case LogType::INSERT: {
                log_record_ = std::make_unique<InsertLogRecord>();
                break;
            }
            case LogType::UPDATE: {
                log_record_ = std::make_unique<UpdateLogRecord>();
                break;
            }
            default: {
                throw RMDBError("not supported log type");
            }
        }
        
        // 统一反序列化和处理
        log_record_->deserialize(buffer.get() + offset - start_offset);
        offset += log_record_->log_tot_len_;
        log_records_.emplace_back(std::move(log_record_));
    }

    return log_records_;
}
