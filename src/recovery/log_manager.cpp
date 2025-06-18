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
lsn_t LogManager::add_log_to_buffer(LogRecord *log_record) {
	std::lock_guard<std::mutex> lock(latch_); // 加锁以确保线程安全
    return add_log_to_buffer_without_lock(log_record);
}

lsn_t LogManager::add_log_to_buffer_without_lock(LogRecord *log_record) {
	switch (log_record->log_type_) {
		case LogType::BEGIN: {
			auto begin_log_record_ = dynamic_cast<BeginLogRecord *>(log_record);
			begin_log_record_->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
			log_buffer_.offset_ += begin_log_record_->log_tot_len_;
			break;
		}
        case LogType::COMMIT: {
            auto commit_log_record_ = dynamic_cast<CommitLogRecord *>(log_record);
            commit_log_record_->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
            log_buffer_.offset_ += commit_log_record_->log_tot_len_;
            break;
        }
        case LogType::ABORT: {
            auto abort_log_record_ = dynamic_cast<AbortLogRecord *>(log_record);
            abort_log_record_->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
            log_buffer_.offset_ += abort_log_record_->log_tot_len_;
            break;
        }
        case LogType::INSERT: {
            auto insert_log_record_ = dynamic_cast<InsertLogRecord *>(log_record);
            insert_log_record_->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
            log_buffer_.offset_ += insert_log_record_->log_tot_len_;
            break;
        }
        case LogType::DELETE: {
            auto delete_log_record_ = dynamic_cast<DeleteLogRecord *>(log_record);
            delete_log_record_->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
            log_buffer_.offset_ += delete_log_record_->log_tot_len_;
            break;
        }
        case LogType::UPDATE: {
            auto update_log_record_ = dynamic_cast<UpdateLogRecord *>(log_record);
            update_log_record_->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
            log_buffer_.offset_ += update_log_record_->log_tot_len_;
            break;
        }
		default: {
			throw RMDBError("not supported log type");
		}
    }
	// 超过一半写入
    if (log_buffer_.offset_ > (LOG_BUFFER_SIZE >> 1)) {
		// 已经锁上
        flush_log_to_disk_without_lock();
    }
	return 0;
}
/**
 * @description: 将日志缓冲区内容刷写到磁盘
 * @warning 由于系统只有一个日志缓冲区：
 * 1. 此操作会暂时阻塞其他日志写入
 * 2. 调用此函数时需要持有缓冲区的互斥锁
 * 3. 确保在适当的时机触发刷盘以平衡性能和持久性
 */
void LogManager::flush_log_to_disk() {
	std::lock_guard<std::mutex> lock(latch_);
	flush_log_to_disk_without_lock();
}

void LogManager::flush_log_to_disk_without_lock() {
	// 将缓冲区内容写入磁盘
	disk_manager_->write_log(log_buffer_.buffer_, static_cast<int>(log_buffer_.offset_));
	// 清空缓冲区
	log_buffer_.offset_ = 0;
}

lsn_t LogManager::add_insert_log(
	txn_id_t txn_id, 
	const RmRecord &insert_value, 
	const Rid &rid, 
	const std::string &table_name
) {
	InsertLogRecord *insert_log = new InsertLogRecord(txn_id, insert_value, rid, table_name);
	lsn_t lsn = add_log_to_buffer(insert_log);
	delete insert_log;
	return lsn;
}

lsn_t LogManager::add_delete_log(
	txn_id_t txn_id, 
	const RmRecord &delete_value, 
	const Rid &rid, 
	const std::string &table_name
) {
	DeleteLogRecord *delete_log = new DeleteLogRecord(txn_id, delete_value, rid, table_name);
	lsn_t lsn = add_log_to_buffer(delete_log);
	delete delete_log;
	return lsn;
}

lsn_t LogManager::add_update_log(
	txn_id_t txn_id, 
	const RmRecord &new_rec,
	const RmRecord &old_rec, 
	const Rid &rid, 
	const std::string &table_name
) {
	UpdateLogRecord *update_log = new UpdateLogRecord(txn_id, new_rec, old_rec, rid, table_name);
	lsn_t lsn = add_log_to_buffer(update_log);
	delete update_log;
	return lsn;
}

lsn_t LogManager::add_begin_log(txn_id_t txn_id) {
	BeginLogRecord *begin_log = new BeginLogRecord(txn_id);
	lsn_t lsn = add_log_to_buffer(begin_log);
	delete begin_log;
	return lsn;
}

lsn_t LogManager::add_commit_log(txn_id_t txn_id) {
	CommitLogRecord *commit_log = new CommitLogRecord(txn_id);
	lsn_t lsn = add_log_to_buffer(commit_log);
	delete commit_log;
	return lsn;
}

lsn_t LogManager::add_abort_log(txn_id_t txn_id) {
	AbortLogRecord *abort_log = new AbortLogRecord(txn_id);
	lsn_t lsn = add_log_to_buffer(abort_log);
	delete abort_log;
	return lsn;
}

std::vector<LogRecord *> LogManager::read_logs_from_disk(size_t offset) {
    std::vector<LogRecord *> log_records_;
    const auto file_size_ = disk_manager_->get_file_size(LOG_FILE_NAME);
    auto buffer = std::make_unique<char[]>(std::max(file_size_, 1));
    disk_manager_->read_log(buffer.get(), file_size_, static_cast<int>(offset));
    auto *tmp_log_ = new LogRecord();

    auto start_offset = offset;
    while (offset < file_size_) {
        tmp_log_->deserialize(buffer.get() + offset - start_offset);
        switch (tmp_log_->log_type_) {
            case LogType::BEGIN: {
                auto log_record_ = new BeginLogRecord();
                log_record_->deserialize(buffer.get() + offset - start_offset);
                offset += log_record_->log_tot_len_;
                log_records_.emplace_back(log_record_);
                break;
            }
            case LogType::COMMIT: {
                auto log_record_ = new CommitLogRecord();
                log_record_->deserialize(buffer.get() + offset - start_offset);
                offset += log_record_->log_tot_len_;
                log_records_.emplace_back(log_record_);
                break;
            }
            case LogType::ABORT: {
                auto log_record_ = new AbortLogRecord();
                log_record_->deserialize(buffer.get() + offset - start_offset);
                offset += log_record_->log_tot_len_;
                log_records_.emplace_back(log_record_);
                break;
            }
            case LogType::DELETE: {
                auto log_record_ = new DeleteLogRecord();
                log_record_->deserialize(buffer.get() + offset - start_offset);
                offset += log_record_->log_tot_len_;
                log_records_.emplace_back(log_record_);
                break;
            }
            case LogType::INSERT: {
                auto log_record_ = new InsertLogRecord();
                log_record_->deserialize(buffer.get() + offset - start_offset);
                offset += log_record_->log_tot_len_;
                log_records_.emplace_back(log_record_);
                break;
            }
            case LogType::UPDATE: {
                auto log_record_ = new UpdateLogRecord();
                log_record_->deserialize(buffer.get() + offset - start_offset);
                offset += log_record_->log_tot_len_;
                log_records_.emplace_back(log_record_);
                break;
            }
			default: {
				throw RMDBError("not supported log type");
			}
        }
    }

    delete tmp_log_;
    return log_records_;
}
