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
#include <stdexcept> // 用于抛出异常
#include <limits>    // 用于检查浮点数零
#include <cmath>     // 用于 std::fabs

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
    TRACE_FUNCTION
    std::vector<Value> values;
    for (const auto &col : cols_) {
        Value value;
        value.set_col_data(col.type, record->data + col.offset, col.len);
        value.init_raw(col.len); // 确保每个Value都有原始数据缓冲区
        values.push_back(value);
    }
    return values;
}
bool check_conflict(
    Transaction *txn,
    TransactionManager *txn_mgr,
    RmFileHandle *fh,
    const Rid &rid
) {
    TRACE_FUNCTION
    // 检查是否有未提交事务修改
    bool ok = txn_mgr->get_lock_manager()->lock_exclusive_on_record(txn, rid, fh->GetFd());
    if(ok == false){
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    
    auto pre_undo_link = txn_mgr->GetUndoLink(rid);
    if(pre_undo_link.has_value()) {
        auto pre_txn = pre_undo_link.value().prev_txn_;
        auto undo_log = txn_mgr->GetUndoLog(pre_undo_link.value());
        // 检查是否有已提交事务更新它且提交时间大于当前事务读时间
        if(txn_mgr->get_txn_state(pre_txn) == TransactionState::COMMITTED && undo_log.ts_ > txn->get_read_ts()) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
        }
        
        // 如果是已提交且删除的记录，直接跳过
        if(undo_log.is_deleted_){
            LockDataId lock_data_id(fh->GetFd(), rid, LockDataType::RECORD);
            txn_mgr->get_lock_manager()->unlock(txn, lock_data_id);
            return false;
        }
    }
    
    return true;
}

std::unique_ptr<RmRecord> mvcc_get_record(
    const Rid &rid,
    Context *context_, 
    RmFileHandle *fh_,
    TransactionManager *txn_mgr_,
    const std::vector<ColMeta> &cols_
) {
    TRACE_FUNCTION
    auto rec = fh_->get_record(rid, context_);

    auto pre_undo_link = txn_mgr_->GetUndoLink(rid);
    while(pre_undo_link.has_value()){
        auto undo_log = txn_mgr_->GetUndoLog(pre_undo_link.value());
        //如果是自己修改的直接返回
        if(pre_undo_link.value().prev_txn_ == context_->txn_->get_transaction_id()) {
            if(undo_log.is_deleted_){
                rec = nullptr;
            }
            return rec;
        }
        // 如果是已提交事物
        if(undo_log.ts_ <= context_->txn_->get_read_ts()){
            if(undo_log.is_deleted_) {
                rec = nullptr;
            }
            return rec;
        }
        for(size_t i = 0; i < cols_.size(); i++) {
            if(undo_log.modified_fields_[i]){
                if(!undo_log.tuple_[i].raw) {
                    undo_log.tuple_[i].init_raw(cols_[i].len);
                }
                memcpy(rec->data + cols_[i].offset, undo_log.tuple_[i].raw->data, cols_[i].len);
            }
        }
        pre_undo_link = undo_log.prev_version_;
        if(!pre_undo_link->IsValid()) {
            rec = nullptr;
            break;
        }
    }
    return rec;
}

Rid mvcc_insert_record(
    const TabMeta &tab_,
    RmRecord &rec,
    Context *context_,
    RmFileHandle *fh_,
    TransactionManager *txn_mgr_,
    const std::vector<Value> &valus_
) {
    TRACE_FUNCTION
    auto rid = fh_->insert_record(rec.data, context_);
    //加锁
    txn_mgr_->get_lock_manager()->lock_exclusive_on_record(context_->txn_, rid, fh_->GetFd());
    // 插入记录后，创建UndoLog
    UndoLog undo_log;
    undo_log.is_deleted_ = false;
    std::vector<Value> values(valus_.size());
    for (size_t i = 0; i < valus_.size(); ++i) {
        values[i] = valus_[i];
        if (!values[i].raw) {
            values[i].init_raw(tab_.cols[i].len); // 确保每个Value都有原始数据缓冲区
        }
    }
    undo_log.tuple_ = std::move(values);
    //此时commit_ts 应该是还未提交
    undo_log.ts_ = txn_mgr_->get_next_timestamp();
    // 插入时没有前一个版本
    undo_log.prev_version_ = UndoLink{}; 
    undo_log.modified_fields_.resize(valus_.size(), true); // 全部字段都被修改
    auto undo_link = context_->txn_->AppendUndoLog(undo_log);
    txn_mgr_->UpdateUndoLink(rid, undo_link);
    context_->txn_->append_write_record(std::make_unique<WriteRecord>(WType::INSERT_TUPLE, tab_.name, rid, rec));
    context_->log_mgr_->add_insert_log(context_->txn_->get_transaction_id(),rec,rid,tab_.name);
    return rid;
}

void mvcc_delete_record(
    const TabMeta &tab_,
    const Rid &rid,
    Context *context_,
    RmFileHandle *fh_,
    TransactionManager *txn_mgr_
) {
    TRACE_FUNCTION
    auto rec = fh_->get_record(rid, context_);
    UndoLog undo_log;
    undo_log.is_deleted_ = true;
    std::vector<Value> values = convert_record_to_values(rec, tab_.cols);
    undo_log.tuple_ = std::move(values);
    undo_log.modified_fields_.resize(tab_.cols.size(), true); // 全部字段都被修改
    //此时commit_ts 应该是还未提交
    undo_log.ts_ = txn_mgr_->get_next_timestamp();
    auto pre = txn_mgr_->GetUndoLink(rid);
    if(pre.has_value()) {
        undo_log.prev_version_ = pre.value(); // 获取前一个版本的撤销链接
    } else {
        undo_log.prev_version_ = UndoLink{}; // 没有前一个版本
    }

    auto undo_link = context_->txn_->AppendUndoLog(undo_log);
    txn_mgr_->UpdateUndoLink(rid, undo_link);
    context_->txn_->append_write_record(
        std::make_unique<WriteRecord>(WType::DELETE_TUPLE, tab_.name, rid, *rec)
    );
    context_->log_mgr_->add_delete_log(context_->txn_->get_transaction_id(), *rec, rid, tab_.name);
}

void mvcc_update_record(
    const TabMeta &tab_,
    const Rid &rid,
    std::unique_ptr<RmRecord> &new_rec,
    std::unique_ptr<RmRecord> &old_rec,
    Context *context_,
    RmFileHandle *fh_,
    TransactionManager *txn_mgr_,
    std::vector<bool> is_modify
) {
    TRACE_FUNCTION
    fh_->update_record(rid, new_rec->data, context_);
    std::vector<Value> values(tab_.cols.size());
    for (size_t i = 0; i < tab_.cols.size(); ++i) {
        if(is_modify[i] == false) {
            continue; // 如果该字段没有被修改，则跳过
        }
        Value value;
        value.set_col_data(tab_.cols[i].type, old_rec->data + tab_.cols[i].offset, tab_.cols[i].len);
        value.init_raw(tab_.cols[i].len); // 确保每个Value都有原始数据缓冲区
        values[i] = value;
    }
    // 创建UndoLog
    UndoLog undo_log;
    undo_log.is_deleted_ = false; // 更新不是删除操作
    undo_log.tuple_ = values;
    // 此时commit_ts 应该是还未提交
    undo_log.ts_ = txn_mgr_->get_next_timestamp();
    auto pre = txn_mgr_->GetUndoLink(rid);
    if(pre.has_value()) {
        undo_log.prev_version_ = pre.value(); // 获取前一个版本的撤销链接
    } else {
        undo_log.prev_version_ = UndoLink{}; // 没有前一个版本
    }
    undo_log.modified_fields_ = std::move(is_modify); // 使用传入的修改标志

    auto undo_link = context_->txn_->AppendUndoLog(undo_log);
    txn_mgr_->UpdateUndoLink(rid, undo_link);

    context_->txn_->append_write_record(
        std::make_unique<WriteRecord>(WType::UPDATE_TUPLE, tab_.name, rid, *old_rec)
    );
    context_->log_mgr_->add_update_log(context_->txn_->get_transaction_id(), *old_rec, *new_rec, rid, tab_.name);
}

/**
 * @brief 从记录数据中根据列元数据提取值
 *
 * @param record 记录数据
 * @param col 列元数据
 * @return 提取出的 Value
 */
Value GetColumnValue(const RmRecord &record, const ColMeta &col) {
    Value val;
    val.set_col_data(col.type, record.data + col.offset, col.len);
    return val;
}


Value EvaluateExpr(const ExprTerm &term, const RmRecord &record, const std::vector<ColMeta> &cols) {
    TRACE_FUNCTION
    switch (term.term_type) {
        case TermType::VALUE: {
            // 直接返回值
            return term.val;
        }
        case TermType::COLUMN: {
            // 查找列并返回值
            for (const auto &col_meta : cols) {
                if (col_meta.name == term.col.col_name && col_meta.tab_name == term.col.tab_name) {
                    return GetColumnValue(record, col_meta);
                }
            }
        }
        case TermType::EXPR: {
            // 递归计算左右操作数
            Value lhs_val = EvaluateExpr(*term.expr->lhs, record, cols);
            Value rhs_val = EvaluateExpr(*term.expr->rhs, record, cols);
            Value result;

            // 执行算术运算，处理类型转换和除零错误
            bool is_float_op = (lhs_val.type == ColType::TYPE_FLOAT || rhs_val.type == ColType::TYPE_FLOAT);

            if (is_float_op) {
                // 至少有一个操作数是 FLOAT，执行浮点运算
                float lhs_float = (lhs_val.type == ColType::TYPE_INT) ? static_cast<float>(lhs_val.int_val) : lhs_val.float_val;
                float rhs_float = (rhs_val.type == ColType::TYPE_INT) ? static_cast<float>(rhs_val.int_val) : rhs_val.float_val;
                float res_float;

                switch (term.expr->op) {
                    case ArithOp::OP_PLUS:
                        res_float = lhs_float + rhs_float;
                        break;
                    case ArithOp::OP_MINUS:
                        res_float = lhs_float - rhs_float;
                        break;
                    case ArithOp::OP_MULTIPLY:
                        res_float = lhs_float * rhs_float;
                        break;
                    case ArithOp::OP_DIVIDE:
                        // 检查除零
                        if (std::fabs(rhs_float) < std::numeric_limits<float>::epsilon()) {
                            throw RMDBError("Division by zero");
                        }
                        res_float = lhs_float / rhs_float;
                        break;
                    default:
                         throw RMDBError("Unsupported arithmetic operator");
                }
                result.set_float(res_float);
            } else if (lhs_val.type == ColType::TYPE_INT && rhs_val.type == ColType::TYPE_INT) {
                // 两个操作数都是 INT，执行整数运算
                int lhs_int = lhs_val.int_val;
                int rhs_int = rhs_val.int_val;
                int res_int;

                switch (term.expr->op) {
                    case ArithOp::OP_PLUS:
                        res_int = lhs_int + rhs_int;
                        break;
                    case ArithOp::OP_MINUS:
                        res_int = lhs_int - rhs_int;
                        break;
                    case ArithOp::OP_MULTIPLY:
                        res_int = lhs_int * rhs_int;
                        break;
                    case ArithOp::OP_DIVIDE:
                        // 检查除零
                        if (rhs_int == 0) {
                            throw RMDBError("Division by zero");
                        }
                        // 注意：整数除法结果也是整数
                        res_int = lhs_int / rhs_int;
                        break;
                    default:
                         throw RMDBError("Unsupported arithmetic operator");
                }
                 result.set_int(res_int);
            } else {
                // 不支持的操作数类型（例如字符串）
                throw RMDBError("Unsupported operand types for arithmetic operation");
            }
            return result;
        }
        default:
            throw RMDBError("Unknown expression term type");
    }
}
