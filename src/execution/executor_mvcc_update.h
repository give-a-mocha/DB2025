/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include <memory>
#include <vector>

#include "execution/execution.h"
#include "execution/execution_common.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

extern SmManager sm_manager;
extern TransactionManager txn_manager;

/**
 * @brief 更新执行器，负责实现UPDATE语句的功能
 */
class MvccUpdateExecutor : public AbstractExecutor {
   private:
    TabMeta &tab_;                                                                 // 表的元数据
    std::vector<Condition> conds_;                                                 // 更新条件列表
    RmFileHandle *fh_;                                                             // 表的数据文件句柄
    std::vector<std::tuple<TupleMeta, std::unique_ptr<RmRecord>, Rid>> old_recs_;  // 旧记录列表
    std::string tab_name_;                                                         // 表名
    std::vector<SetClause> set_clauses_;                                           // SET子句列表(新值)

   public:
    MvccUpdateExecutor(const std::string &tab_name, std::vector<SetClause> set_clauses, std::vector<Condition> conds,
                       std::vector<std::tuple<TupleMeta, std::unique_ptr<RmRecord>, Rid>> old_recs, Context *context)
        : tab_(sm_manager.db_.get_table(tab_name)) {
        tab_name_ = tab_name;
        set_clauses_ = std::move(set_clauses);
        fh_ = sm_manager.fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        old_recs_ = std::move(old_recs);
        context_ = context;
    }

    /**
     * @brief 执行批量更新操作
     * @return nullptr，因为UPDATE不产生结果集
     * @throw IncompatibleTypeError 当值的类型与列类型不兼容时
     * @throw RMDBError 当索引更新失败需要回滚时
     */
    std::unique_ptr<RmRecord> Next() override {
        for (auto &rec_tuple : old_recs_) {
            auto &base_meta = std::get<0>(rec_tuple);
            auto &old_rec = std::get<1>(rec_tuple);
            auto &rid_ = std::get<2>(rec_tuple);
            auto link = txn_manager.GetUndoLink(fh_->GetFd(), rid_);

            if (IsWriteWriteConflict(context_->txn_, link)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            }

            // 创建新记录
            auto new_rec = std::make_unique<RmRecord>(old_rec->size, old_rec->data);

            // 处理每个SET子句
            for (const auto &set_clause : set_clauses_) {
                auto col = tab_.get_col(set_clause.lhs.col_name);
                Value value;  // 将 value 的声明提前

                // 根据 rhs_type 获取值
                if (set_clause.rhs_type == SetRhsType::SET_RHS_VALUE) {
                    value = set_clause.rhs_val;  // 直接使用 rhs_val
                } else if (set_clause.rhs_type == SetRhsType::SET_RHS_EXPR) {
                    // 创建一个临时的 ExprTerm 来包装 ArithExpr
                    ExprTerm temp_expr_term(set_clause.rhs_expr);
                    // 计算表达式的值
                    // 注意：这里需要传入当前的旧记录 old_rec 来获取列值
                    value = EvaluateExpr(temp_expr_term, old_rec, tab_.cols);
                } else if (set_clause.rhs_type == SetRhsType::SET_RHS_COL) {
                    // 从旧记录中获取列的值
                    // 找到对应的列元数据
                    const ColMeta *rhs_col_meta = nullptr;
                    for (const auto &meta : tab_.cols) {
                        if (meta.tab_name == set_clause.rhs_col.tab_name && meta.name == set_clause.rhs_col.col_name) {
                            rhs_col_meta = &meta;
                            break;
                        }
                    }
                    if (!rhs_col_meta) {
                        throw RMDBError("RHS column not found in SET clause: " + set_clause.rhs_col.tab_name + "." +
                                        set_clause.rhs_col.col_name);
                    }
                    value = GetColumnValue(old_rec, *rhs_col_meta);

                } else {
                    throw RMDBError("Unsupported SetRhsType");
                }

                // 处理类型转换 (使用计算或获取到的 value.type)
                if (col->type != value.type) {
                    if (col->type == ColType::TYPE_INT && value.type == ColType::TYPE_FLOAT) {
                        value.set(static_cast<int>(value.float_val));
                    } else if (col->type == ColType::TYPE_FLOAT && value.type == ColType::TYPE_INT) {
                        value.set(static_cast<float>(value.int_val));
                    } else {
                        throw IncompatibleTypeError(coltype2str(col->type), coltype2str(value.type));
                    }
                }

                value.raw.reset();         // 确保 raw 数据被重置
                value.init_raw(col->len);  // 确保 raw 数据被初始化

                memcpy(new_rec->data + col->offset, value.raw->data, col->len);
            }

            std::vector<Rid> rids = sm_manager.exist_in_index(tab_, new_rec, context_->txn_);
            Rid insert_rid;
            // 唯一性检查
            TupleMeta base_meta_(0, true);
            for (const auto &rid : rids) {
                INFO("Checking unique constraint for rid: {}", rid);
                auto [tuple_meta, tuple_rec, link] = txn_manager.GetTupleAndUndoLink(fh_, rid);
                base_meta_ = tuple_meta;
                if (IsWriteWriteConflict(context_->txn_, link)) {
                    throw TransactionAbortException(context_->txn_->get_transaction_id(),
                                                    AbortReason::UPGRADE_CONFLICT);
                }
                // 主键冲突
                if (rid != rid_ && tuple_meta.is_deleted_ == false) {
                    txn_manager.abort(context_);
                    throw InternalError("Primary key conflict, duplicate insert");
                }
            }
            TupleMeta new_meta(context_->txn_->get_transaction_id(), false);
            if (!rids.empty()) {
                insert_rid = rids.back();
                if (!txn_manager.AtomicUpdate(tab_name_, fh_, rid_, base_meta, old_rec, insert_rid, base_meta_, nullptr,
                                              new_rec, context_->txn_)) {
                    throw TransactionAbortException(context_->txn_->get_transaction_id(),
                                                    AbortReason::UPGRADE_CONFLICT);
                }
            } else {
                TupleMeta new_meta(context_->txn_->get_transaction_id(), false);
                TupleMeta delete_meta(0, true);
                insert_rid = fh_->GetNewRid();
                if (!txn_manager.AtomicUpdate(tab_name_, fh_, rid_, base_meta, old_rec, insert_rid, delete_meta,
                                              nullptr, new_rec, context_->txn_)) {
                    fh_->delete_record(insert_rid);
                    throw TransactionAbortException(context_->txn_->get_transaction_id(),
                                                    AbortReason::UPGRADE_CONFLICT);
                }
                sm_manager.insert_index_with_tab_meta(tab_, new_rec, insert_rid, context_->txn_);
            }
            // context_->log_mgr_->add_insert_log(context_->txn_->get_transaction_id(), std::move(new_rec), insert_rid,
            // tab_.name);
        }
        return nullptr;
    }

    /**
     * @brief 获取当前记录的RID
     * @return 抽象RID的引用
     */
    Rid &rid() override { return _abstract_rid; }

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    std::string getType() override { return "MvccUpdateExecutor"; }
};
