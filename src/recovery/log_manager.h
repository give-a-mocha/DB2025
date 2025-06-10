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

#include <iostream>
#include <mutex>
#include <vector>

#include "common/config.h"
#include "common/print.hpp"
#include "log_defs.h"
#include "record/rm_defs.h"

/* 日志记录对应操作的类型 */
enum LogType : int {
    UPDATE = 0,
    INSERT,
    DELETE,
    begin,
    commit,
    ABORT
};

static std::string LogTypeStr[] = {
    "UPDATE",
    "INSERT",
    "DELETE",
    "BEGIN",
    "COMMIT",
    "ABORT"
};


class LogRecord {
public:
    LogType log_type_;         /* 日志对应操作的类型 */
    lsn_t lsn_;                /* 当前日志的lsn */
    uint32_t log_tot_len_;     /* 整个日志记录的长度 */
    txn_id_t log_tid_;         /* 创建当前日志的事务ID */
    lsn_t prev_lsn_;           /* 事务创建的前一条日志记录的lsn，用于undo */

    LogRecord() {
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    // !添加虚析构防止内存泄漏
    virtual ~LogRecord() = default;

    // 把日志记录序列化到dest中
    virtual void serialize(char* dest) const {
        // 按照固定的偏移量依次写入各字段
        memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));         // 写入日志类型
        memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_t));                     // 写入日志序列号
        memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(uint32_t));  // 写入日志总长度
        memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(txn_id_t));          // 写入事务ID
        memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(lsn_t));           // 写入前一条日志的序列号
    }

    template <typename T>
    void serialize_data(char* dest, int offset, const T* data, const int data_size = sizeof(T)) const {
        memcpy(dest + offset, data, data_size);
        offset += data_size;
    }

    // 从src中反序列化出一条日志记录
    virtual void deserialize(const char* src) {
        // 按照固定的偏移量依次读取各字段
        log_type_ = *reinterpret_cast<const LogType*>(src);                           // 读取日志类型
        lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_LSN);                     // 读取日志序列号
        log_tot_len_ = *reinterpret_cast<const uint32_t*>(src + OFFSET_LOG_TOT_LEN);  // 读取日志总长度
        log_tid_ = *reinterpret_cast<const txn_id_t*>(src + OFFSET_LOG_TID);          // 读取事务ID
        prev_lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_PREV_LSN);           // 读取前一条日志的序列号
    }

    template <typename... Args>
    void logINFO(std::string_view fmt_str, Args&&... args) const {
        INFO(fmt_str, std::forward<Args>(args)...);
    }

    // used for debug
    virtual void format_print() const {
        logINFO("log type in father_function: {}", LogTypeStr[log_type_]);
        logINFO("Print Log Record:");
        logINFO("log_type_: {}", LogTypeStr[log_type_]);  // 打印日志类型
        logINFO("lsn: {}", lsn_);                         // 打印日志序列号
        logINFO("log_tot_len: {}", log_tot_len_);         // 打印日志总长度
        logINFO("log_tid: {}", log_tid_);                 // 打印事务ID
        logINFO("prev_lsn: {}", prev_lsn_);               // 打印前一条日志的序列号
    }
};


class BeginLogRecord : public LogRecord {
   public:
    BeginLogRecord() : LogRecord() { log_type_ = LogType::begin; }
    BeginLogRecord(const txn_id_t txn_id) : BeginLogRecord() {
        log_tid_ = txn_id;  // 设置事务ID
    }
    virtual ~BeginLogRecord() = default;
    // 序列化Begin日志记录到dest中
    void serialize(char* dest) const override { LogRecord::serialize(dest); }
    // 从src中反序列化出一条Begin日志记录
    void deserialize(const char* src) override { LogRecord::deserialize(src); }
    virtual void format_print() const override {
        logINFO("log type in son_function: {}", LogTypeStr[log_type_]);
        LogRecord::format_print();  // 调用基类的打印方法
    }
};

/**
 * TODO: commit操作的日志记录
 */
class CommitLogRecord : public LogRecord {
   public:
    CommitLogRecord() : LogRecord() { log_type_ = LogType::commit; }
    CommitLogRecord(const txn_id_t txn_id) : CommitLogRecord() { log_tid_ = txn_id; }
    virtual ~CommitLogRecord() = default;
    void serialize(char* dest) const override { LogRecord::serialize(dest); }
    void deserialize(const char* src) override { LogRecord::deserialize(src); }
    virtual void format_print() const override {
        logINFO("log type in son_function: {}", LogTypeStr[log_type_]);
        LogRecord::format_print();
    }
};

/**
 * TODO: abort操作的日志记录
*/
class AbortLogRecord : public LogRecord {
   public:
    AbortLogRecord() : LogRecord() { log_type_ = LogType::ABORT; }
    AbortLogRecord(const txn_id_t txn_id) : AbortLogRecord() { log_tid_ = txn_id; }
    void serialize(char* dest) const override { LogRecord::serialize(dest); }
    void deserialize(const char* src) override { LogRecord::deserialize(src); }
    virtual void format_print() const override {
        logINFO("log type in son_function: {}", LogTypeStr[log_type_]);
        LogRecord::format_print();
    }
};

class InsertLogRecord : public LogRecord {
   public:
    InsertLogRecord() : LogRecord() {
        log_type_ = LogType::INSERT;
        table_name_ = nullptr;
    }
    InsertLogRecord(const txn_id_t txn_id, const RmRecord& insert_value, const Rid& rid, const std::string& table_name)
        : InsertLogRecord() {
        log_tid_ = txn_id;
        insert_value_ = insert_value;
        rid_ = rid;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += insert_value_.size;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_;
    }

    // 把insert日志记录序列化到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        auto Serialize = [this, dest, &offset]<typename T>(const T* data, const int data_size = sizeof(T)) -> void {
            serialize_data(dest, offset, data, data_size);
        };
        Serialize(&insert_value_.size);
        Serialize(insert_value_.data, insert_value_.size);
        Serialize(&rid_);
        Serialize(&table_name_size_);
        Serialize(table_name_, table_name_size_);
    }
    // 从src中反序列化出一条Insert日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        insert_value_.Deserialize(src + OFFSET_LOG_DATA);
        int offset = OFFSET_LOG_DATA + insert_value_.size + sizeof(int);
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
    }
    void format_print() const override {
        logINFO("insert record");
        LogRecord::format_print();
        logINFO("insert_value: {}", insert_value_.data);
        logINFO("insert rid: {}, {}", rid_.page_no, rid_.slot_no);
        logINFO("table name: {}", table_name_);
    }

    RmRecord insert_value_;   // 插入的记录
    Rid rid_;                 // 记录插入的位置
    char* table_name_;        // 插入记录的表名称
    size_t table_name_size_;  // 表名称的大小
};

/**
 * TODO: delete操作的日志记录
*/
class DeleteLogRecord : public LogRecord {
   public:
    DeleteLogRecord() : LogRecord() {
        log_type_ = LogType::DELETE;
        table_name_ = nullptr;
    }
    DeleteLogRecord(const txn_id_t txn_id, const RmRecord& delete_value, const Rid& rid, const std::string& table_name)
        : DeleteLogRecord() {
        log_tid_ = txn_id;
        delete_value_ = delete_value;
        rid_ = rid;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += delete_value_.size;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_;
    }
    // 把delete日志记录序列化到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        auto Serialize = [this, dest, &offset]<typename T>(const T* data, const int data_size = sizeof(T)) -> void {
            serialize_data(dest, offset, data, data_size);
        };
        Serialize(&delete_value_.size);
        Serialize(delete_value_.data, delete_value_.size);
        Serialize(&rid_);
        Serialize(&table_name_size_);
        Serialize(table_name_, table_name_size_);
    }
    // 从src中反序列化出一条Delete日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        delete_value_.Deserialize(src + OFFSET_LOG_DATA);
        int offset = OFFSET_LOG_DATA + delete_value_.size + sizeof(int);
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
    }
    void format_print() const override {
        logINFO("delete record");
        LogRecord::format_print();
        logINFO("delete_value: {}", delete_value_.data);
        logINFO("delete rid: {}, {}", rid_.page_no, rid_.slot_no);
        logINFO("table name: {}", table_name_);
    }

    RmRecord delete_value_;   // 删除的记录
    Rid rid_;                 // 记录删除的位置
    char* table_name_;        // 删除记录的表名称
    size_t table_name_size_;  // 表名称的大小
};

/**
 * TODO: update操作的日志记录
*/
class UpdateLogRecord : public LogRecord {
   public:
    UpdateLogRecord() : LogRecord() {
        log_type_ = LogType::UPDATE;
        table_name_ = nullptr;
    }
    UpdateLogRecord(const txn_id_t txn_id, const RmRecord& before_value, const RmRecord& after_value, const Rid& rid,
                    const std::string& table_name)
        : UpdateLogRecord() {
        log_tid_ = txn_id;
        before_value_ = before_value;
        after_value_ = after_value;
        rid_ = rid;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += before_value_.size;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += after_value_.size;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_;
    }
    // 把update日志记录序列化到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        auto Serialize = [this, dest, &offset]<typename T>(const T* data, const int data_size = sizeof(T)) -> void {
            serialize_data(dest, offset, data, data_size);
        };
        Serialize(&before_value_.size);
        Serialize(before_value_.data, before_value_.size);
        Serialize(&after_value_.size);
        Serialize(after_value_.data, after_value_.size);
        Serialize(&rid_);
        Serialize(&table_name_size_);
        Serialize(table_name_, table_name_size_);
    }
    // 从src中反序列化出一条Update日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        before_value_.Deserialize(src + OFFSET_LOG_DATA);
        int offset = OFFSET_LOG_DATA + before_value_.size + sizeof(int);
        after_value_.Deserialize(src + offset);
        offset += after_value_.size + sizeof(int);
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
    }
    void format_print() const override {
        logINFO("update record");
        LogRecord::format_print();
        logINFO("before_value: {}", before_value_.data);
        logINFO("after_value: {}", after_value_.data);
        logINFO("update rid: {}, {}", rid_.page_no, rid_.slot_no);
        logINFO("table name: {}", table_name_);
    }

    RmRecord before_value_;   // 更新前的记录
    RmRecord after_value_;    // 更新后的记录
    Rid rid_;                 // 记录插入的位置
    char* table_name_;        // 插入记录的表名称
    size_t table_name_size_;  // 表名称的大小
};

/* 日志缓冲区，只有一个buffer，因此需要阻塞地去把日志写入缓冲区中 */
class LogBuffer {
   public:
    LogBuffer() {
        offset_ = 0;
        memset(buffer_, 0, sizeof(buffer_));
    }

    bool is_full(int append_size) { return offset_ + append_size > LOG_BUFFER_SIZE; }

    char buffer_[LOG_BUFFER_SIZE + 1];
    int offset_;  // 写入log的offset
};

/* 日志管理器，负责把日志写入日志缓冲区，以及把日志缓冲区中的内容写入磁盘中 */
class LogManager {
private:
    std::atomic<lsn_t> global_lsn_{0};  // 全局lsn，递增，用于为每条记录分发lsn
    std::mutex latch_;                  // 用于对log_buffer_的互斥访问
    LogBuffer log_buffer_;              // 日志缓冲区
    lsn_t persist_lsn_;                 // 记录已经持久化到磁盘中的最后一条日志的日志号
    DiskManager* disk_manager_;
public:
    LogManager(DiskManager* disk_manager) { disk_manager_ = disk_manager; }

    lsn_t add_log_to_buffer(LogRecord* log_record);

    void flush_log_to_disk();

    LogBuffer* get_log_buffer() { return &log_buffer_; }

};
