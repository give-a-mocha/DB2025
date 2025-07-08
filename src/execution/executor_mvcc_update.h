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

/**
 * @brief 更新执行器，负责实现UPDATE语句的功能
 */
class MvccUpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                         // 表的元数据
    std::vector<Condition> conds_;        // 更新条件列表
    RmFileHandle *fh_;                    // 表的数据文件句柄
    std::vector<Rid> rids_;               // 待更新记录的RID列表
    std::string tab_name_;                // 表名
    std::vector<SetClause> set_clauses_;  // SET子句列表(新值)
    SmManager *sm_manager_;               // 系统管理器指针
    TransactionManager *txn_mgr_;         // 事务管理器指针

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     * @param tab_name 目标表名
     * @param set_clauses SET子句列表
     * @param conds 更新条件列表
     * @param rids 待更新记录的RID列表
     * @param context 执行上下文
     */
    MvccUpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                       std::vector<Condition> conds, std::vector<Rid> rids, Context *context,
                       TransactionManager *txn_mgr) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = std::move(set_clauses);
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;
        txn_mgr_ = txn_mgr;
    }

    /**
     * @brief 执行批量更新操作
     * @return nullptr，因为UPDATE不产生结果集
     * @throw IncompatibleTypeError 当值的类型与列类型不兼容时
     * @throw RMDBError 当索引更新失败需要回滚时
     */
    std::unique_ptr<RmRecord> Next() override {
        TRACE_FUNCTION
        // 加锁间隙
        txn_mgr_->get_lock_manager()->lock_gap(context_->txn_, fh_->GetFd(), conds_);
        for (size_t i = 0; i < rids_.size(); ++i) {
            auto &rid = rids_[i];
            if (!get_lock_and_check_conflict(context_->txn_, txn_mgr_, fh_, rid)) {
                continue;
            }
            // 获取旧记录并创建新记录
            auto old_rec = fh_->get_record(rid, context_);
            auto new_rec = std::make_unique<RmRecord>(old_rec->size, old_rec->data);
            std::vector<bool> is_modify(tab_.cols.size(), false);

            // 处理每个SET子句
            for (const auto &set_clause : set_clauses_) {
                auto col = tab_.get_col(set_clause.lhs.col_name);
                is_modify[col - tab_.cols.begin()] = true;  // 标记列已被修改
                Value value;                                // 将 value 的声明提前

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

                // 确保 value 有 raw 数据，但避免不必要的重置
                // value.raw.reset(); // 移除这行，因为 EvaluateExpr 和 GetColumnValue 会处理

                // 处理类型转换 (使用计算或获取到的 value.type)
                if (col->type != value.type) {
                    if (col->type == ColType::TYPE_INT && value.type == ColType::TYPE_FLOAT) {
                        value.set_int(static_cast<int>(value.float_val));
                    } else if (col->type == ColType::TYPE_FLOAT && value.type == ColType::TYPE_INT) {
                        value.set_float(static_cast<float>(value.int_val));
                    } else if (col->type != value.type) {  // 添加一个检查，防止相同类型也抛出错误
                        throw IncompatibleTypeError(coltype2str(col->type), coltype2str(value.type));
                    }
                }

                value.raw.reset();         // 确保 raw 数据被重置
                value.init_raw(col->len);  // 确保 raw 数据被初始化

                memcpy(new_rec->data + col->offset, value.raw->data, col->len);
            }

            // 获取全局条件
            std::vector<Condition> conds =
                txn_mgr_->get_lock_manager()->get_gap_condition(fh_->GetFd(), context_->txn_);
            if (!conds.empty() && eval_conds(tab_.cols, conds, new_rec)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            }
            
            // update = delete + insert
            txn_mgr_->add_delete_undo_log(context_->txn_, fh_->GetFd(), rid);
            context_->txn_->append_write_record(
                std::make_unique<WriteRecord>(WType::DELETE_TUPLE, tab_.name, rid, *old_rec));
            context_->log_mgr_->add_delete_log(context_->txn_->get_transaction_id(), *old_rec, rid, tab_.name);
            RmRecord Value{};
            Rid insert_rid;
            if (sm_manager_->exist_in_index(tab_, *new_rec, insert_rid, context_->txn_)) {
                get_lock_and_check_conflict(context_->txn_, txn_mgr_, fh_, insert_rid);
                Value = *fh_->get_record(insert_rid, context_);
                fh_->insert_record_force(insert_rid, new_rec->data);
            } else {
                insert_rid = fh_->insert_record(new_rec->data, context_);
                txn_mgr_->get_lock_manager()->lock_exclusive_on_record(context_->txn_, insert_rid, fh_->GetFd());
                sm_manager_->insert_index(tab_name_, *new_rec, insert_rid, context_->txn_);
            }
            
            txn_mgr_->add_insert_undo_log(context_->txn_, fh_->GetFd(), insert_rid, std::move(Value));
            
            context_->txn_->append_write_record(std::make_unique<WriteRecord>(WType::INSERT_TUPLE, tab_.name, insert_rid, *new_rec));
            context_->log_mgr_->add_insert_log(context_->txn_->get_transaction_id(), *new_rec, insert_rid, tab_.name);
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
