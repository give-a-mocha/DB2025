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
    std::unique_ptr<AbstractExecutor> prev_;  // 前序执行器
    TabMeta &tab_;                            // 表的元数据
    RmFileHandle *fh_;                        // 表的数据文件句柄
    std::vector<SetClause> set_clauses_;      // SET子句列表(新值)

   public:
    MvccUpdateExecutor(std::unique_ptr<AbstractExecutor> prev, std::string tab_name, std::vector<SetClause> set_clauses,
                       Context *context)
        : tab_(sm_manager.db_.get_table(tab_name)) {
        prev_ = std::move(prev);
        set_clauses_ = std::move(set_clauses);
        fh_ = sm_manager.fhs_.at(tab_name).get();
        context_ = context;
    }

    /**
     * @brief 执行批量更新操作
     * @return nullptr，因为UPDATE不产生结果集
     * @throw IncompatibleTypeError 当值的类型与列类型不兼容时
     * @throw RMDBError 当索引更新失败需要回滚时
     */
    std::unique_ptr<RmRecord> Next() override {
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto &delete_meta = prev_->tuple_meta();
            auto old_rec = prev_->Next();
            auto &rid_ = prev_->rid();
            auto link = txn_manager.GetUndoLink(fh_->GetFd(), rid_);

            // if (!lock_manager.lock_exclusive_on_record(context_->txn_, rid_, fh_->GetFd())) {
            //     txn_manager.abort(context_);
            //     lock_manager.wait_for_lock_release(LockDataId(fh_->GetFd(), rid_));
            //     throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            // }

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
                // 将值设置到新记录中
                value.set_record_data(new_rec->data + col->offset, col->len);
            }

            std::vector<Rid> rids = sm_manager.exist_in_index(tab_, new_rec, context_->txn_);
            Rid insert_rid;
            // 唯一性检查
            TupleMeta insert_old_meta(0, true);
            TupleMeta insert_new_meta(context_->txn_->get_transaction_id(), false);
            for (const auto &rid : rids) {
                auto [tuple_meta, link] = txn_manager.GetTupleMetaAndUndoLink(fh_, rid);
                insert_old_meta = tuple_meta;
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
            if (!rids.empty()) {
                //! 这里使用back在唯一索引下才是对的
                insert_rid = rids.back();

                // if (!lock_manager.lock_exclusive_on_record(context_->txn_, insert_rid, fh_->GetFd())) {
                //     txn_manager.abort(context_);
                //     lock_manager.wait_for_lock_release(LockDataId(fh_->GetFd(), insert_rid));
                //     throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
                // }

                if (!txn_manager.AtomicUpdate(tab_.name, fh_, rid_, delete_meta, old_rec, insert_rid, insert_old_meta,
                                              nullptr, insert_new_meta, new_rec, context_->txn_)) {
                    throw TransactionAbortException(context_->txn_->get_transaction_id(),
                                                    AbortReason::UPGRADE_CONFLICT);
                }
            } else {
                insert_rid = fh_->GetNewRid();
                // if (!lock_manager.lock_exclusive_on_record(context_->txn_, insert_rid, fh_->GetFd())) {
                //     txn_manager.abort(context_);
                //     lock_manager.wait_for_lock_release(LockDataId(fh_->GetFd(), insert_rid));
                //     throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
                // }
                if (!txn_manager.AtomicUpdate(tab_.name, fh_, rid_, delete_meta, old_rec, insert_rid, insert_old_meta,
                                              nullptr, insert_new_meta, new_rec, context_->txn_)) {
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
