/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "execution_common.h"

#include "system/sm.h"

auto ReconstructTuple(const std::vector<ColMeta> &cols, const RmRecord &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<RmRecord> {
    // 如果元组已被删除，返回空
    if (base_meta.is_deleted_) {
        return std::nullopt;
    }

    // 创建结果记录的副本
    RmRecord result(base_tuple.size);
    memcpy(result.data, base_tuple.data, base_tuple.size);

    // 应用撤销日志，从最新到最旧
    for (auto it = undo_logs.rbegin(); it != undo_logs.rend(); ++it) {
        const auto &undo_log = *it;

        if (undo_log.is_deleted_) {
            // 如果撤销日志标记为删除，返回空
            return std::nullopt;
        }

        // 应用撤销日志中的字段修改
        if (undo_log.tuple_test_) {
            memcpy(result.data, undo_log.tuple_test_->data, result.size);
        } else if (!undo_log.tuple_.empty()) {
            // 使用Value数组重建记录
            for (size_t i = 0; i < undo_log.tuple_.size() && i < cols.size(); ++i) {
                if (i < undo_log.modified_fields_.size() && undo_log.modified_fields_[i]) {
                    const auto &col = cols[i];
                    const auto &val = undo_log.tuple_[i];
                    if (val.raw && val.raw->data) {
                        memcpy(result.data + col.offset, val.raw->data, col.len);
                    }
                }
            }
        }
    }
    return result;
}

auto IsWriteWriteConflict(timestamp_t tuple_ts, Transaction *txn) -> bool {
    // 检查写-写冲突
    // 如果元组的时间戳大于事务的开始时间戳，则存在写-写冲突
    return tuple_ts > txn->get_start_ts();
}

auto message_out(Context *context_, const std::string &output) -> void {
    if (context_ && context_->data_send_ && context_->offset_ && *context_->offset_ + output.length() < BUFFER_LENGTH) {
        memcpy(context_->data_send_ + *context_->offset_, output.c_str(), output.length());
        *context_->offset_ += output.length();
    }
}

auto message_out(Context *context_, const char *output, size_t output_size) -> void {
    if (context_ && context_->data_send_ && context_->offset_ && *context_->offset_ + output_size < BUFFER_LENGTH) {
        memcpy(context_->data_send_ + *context_->offset_, output, output_size);
        *context_->offset_ += output_size;
    }
}


std::vector<Value> convert_record_to_values(
    const std::unique_ptr<RmRecord> &record, 
    const std::vector<ColMeta> &cols_
) {
    std::vector<Value> values;
    for (const auto &col : cols_) {
        Value value;
        value.set_col_data(col.type, record->data + col.offset, col.len);
        value.init_raw(col.len); // 确保每个Value都有原始数据缓冲区
        values.push_back(value);
    }
    return values;
}

std::unique_ptr<RmRecord> mvcc_get_record(
    const Rid &rid,
    Context *context_, 
    RmFileHandle *fh_,
    TransactionManager *txn_mgr_,
    const std::vector<ColMeta> &cols_
) {
    INFO("mvcc_get_record");
    auto rec = fh_->get_record(rid, context_);
    auto pre_undo_link = txn_mgr_->GetUndoLink(rid);
    while(pre_undo_link.has_value()){
        auto undo_log = txn_mgr_->GetUndoLog(pre_undo_link.value());
        //可见性检查
        //如果是自己修改的存下
        if(pre_undo_link.value().prev_txn_ == context_->txn_->get_transaction_id()) {
            return rec;
        }
        INFO("mvcc_get_record_while");
        // 如果是已提交事物
        if(txn_mgr_->get_txn_state(pre_undo_link.value().prev_txn_) == TransactionState::COMMITTED){
            if(undo_log.ts_ <= context_->txn_->get_read_ts()){
                if(undo_log.is_deleted_) {
                    rec = nullptr;
                }
                break;
            }
            for(size_t i = 0; i < cols_.size(); i++) {
                if(undo_log.modified_fields_[i]){
                    Value val = undo_log.tuple_[i];
                    if(!val.raw) {
                        val.init_raw(cols_[i].len);
                    }
                    memcpy(rec->data + cols_[i].offset, val.raw->data, cols_[i].len);
                }
            }
            
        }
        INFO("mvcc_get_record_while2");
        pre_undo_link = undo_log.prev_version_;
        if(!pre_undo_link->IsValid()) {
            rec = nullptr;
            break;
        }
    }
    auto write_set = context_->txn_->get_write_set();
    for(const auto &write_record : *write_set) {
        if(write_record->GetRid() == rid) {
            // 如果是当前事务的写操作，直接返回
            if(write_record->GetWriteType() == WType::INSERT_TUPLE) {
                return std::make_unique<RmRecord>(write_record->GetRecord());
            } else if(write_record->GetWriteType() == WType::UPDATE_TUPLE) {
                return std::make_unique<RmRecord>(write_record->GetRecord());
            } else if(write_record->GetWriteType() == WType::DELETE_TUPLE) {
                return nullptr; // 删除操作返回空
            }
        }
    }
    return rec;
}
// insert 的话应该只有这个版本
Rid mvcc_insert_record(
    char *buf, 
    Context *context_,
    RmFileHandle *fh_,
    TransactionManager *txn_mgr_,
    const std::vector<Value> &valus_
) {
    auto res = fh_->insert_record(buf, context_);
    // 插入记录后，创建UndoLog
    UndoLog undo_log;
    undo_log.is_deleted_ = false;
    std::vector<Value> values(valus_.size());
    for (size_t i = 0; i < valus_.size(); ++i) {
        values[i] = valus_[i];
        if (!values[i].raw) {
            values[i].init_raw(); // 确保每个Value都有原始数据缓冲区
        }
    }
    undo_log.tuple_ = std::move(values);
    //此时commit_ts 应该是还未提交
    // undo_log.ts_ = txn_mgr_->get_next_timestamp();
    // 插入时没有前一个版本
    undo_log.prev_version_ = UndoLink{}; 
    undo_log.modified_fields_.resize(valus_.size(), true); // 全部字段都被修改
    auto undo_link = context_->txn_->AppendUndoLog(undo_log);
    txn_mgr_->UpdateUndoLink(res, undo_link);
    return res;
}

void mvcc_delete_record(
    const Rid &rid,
    Context *context_,
    RmFileHandle *fh_,
    TransactionManager *txn_mgr_,
    const std::vector<ColMeta> &cols_
) {
    auto rec = mvcc_get_record(rid, context_, fh_, txn_mgr_, cols_);
    UndoLog undo_log;
    undo_log.is_deleted_ = true;
    std::vector<Value> values = convert_record_to_values(rec, cols_);
    undo_log.tuple_ = std::move(values);
    undo_log.modified_fields_.resize(cols_.size(), true); // 全部字段都被修改
    //此时commit_ts 应该是还未提交
    // undo_log.ts_ = txn_mgr_->get_next_timestamp();
    auto pre = txn_mgr_->GetUndoLink(rid);
    if(pre.has_value()) {
        undo_log.prev_version_ = pre.value(); // 获取前一个版本的撤销链接
    } else {
        undo_log.prev_version_ = UndoLink{}; // 没有前一个版本
    }
    context_->txn_->AppendUndoLog(undo_log);
}

void mvcc_update_record(
    const Rid &rid,
    std::unique_ptr<RmRecord> &new_rec,
    std::unique_ptr<RmRecord> &old_rec,
    Context *context_,
    TransactionManager *txn_mgr_,
    const std::vector<ColMeta> &cols_,
    std::vector<bool> is_modify
) {
    std::vector<Value> values(cols_.size());
    for (size_t i = 0; i < cols_.size(); ++i) {
        if(is_modify[i] == false) {
            continue; // 如果该字段没有被修改，则跳过
        }
        Value value;
        value.set_col_data(cols_[i].type, old_rec->data + cols_[i].offset, cols_[i].len);
        value.init_raw(cols_[i].len); // 确保每个Value都有原始数据缓冲区
        values[i] = value;
    }
    // 创建UndoLog
    UndoLog undo_log;
    undo_log.is_deleted_ = false; // 更新不是删除操作
    undo_log.tuple_ = values;
    //此时commit_ts 应该是还未提交
    // undo_log.ts_ = txn_mgr_->get_next_timestamp();
    auto pre = txn_mgr_->GetUndoLink(rid);
    if(pre.has_value()) {
        undo_log.prev_version_ = pre.value(); // 获取前一个版本的撤销链接
    } else {
        undo_log.prev_version_ = UndoLink{}; // 没有前一个版本
    }
    undo_log.modified_fields_ = std::move(is_modify); // 使用传入的修改标志
    context_->txn_->AppendUndoLog(undo_log);
}
