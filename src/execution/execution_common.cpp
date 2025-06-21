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

std::vector<Value> convert_record_to_values(
    const std::unique_ptr<RmRecord> &record, 
    const std::vector<ColMeta> &cols_
) {
    TRACE_FUNCTION
    std::vector<Value> values;
    values.reserve(cols_.size()); // 预分配空间以提高性能
    for (const auto &col : cols_) {
        Value value;
        value.set_col_data(col.type, record->data + col.offset, col.len);
        value.init_raw(col.len); // 确保每个Value都有原始数据缓冲区
        values.push_back(value);
    }
    return std::move(values);
}

/**
 * @brief 检查事务是否与记录发生冲突, 获取锁
 */
void get_lock_and_check_conflict(
    Transaction *txn,
    TransactionManager *txn_mgr,
    RmFileHandle *fh,
    const Rid &rid
) {
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
    }
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
