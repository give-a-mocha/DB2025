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
#include "execution_common.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

extern SmManager sm_manager;
extern TransactionManager txn_manager;

/**
 * @brief 删除执行器，负责实现DELETE语句的功能
 */
class MvccDeleteExecutor : public AbstractExecutor {
   private:
    TabMeta &tab_;                                                                 // 表的元数据
    std::vector<Condition> conds_;                                                 // 删除条件列表
    RmFileHandle *fh_;                                                             // 表的数据文件句柄
    std::vector<std::tuple<TupleMeta, std::unique_ptr<RmRecord>, Rid>> old_recs_;  // 旧记录列表
    std::string tab_name_;                                                         // 表名

   public:
    MvccDeleteExecutor(const std::string &tab_name, std::vector<Condition> conds,
                       std::vector<std::tuple<TupleMeta, std::unique_ptr<RmRecord>, Rid>> old_recs, Context *context)
        : tab_(sm_manager.db_.get_table(tab_name)) {
        tab_name_ = tab_name;
        fh_ = sm_manager.fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        old_recs_ = std::move(old_recs);
        context_ = context;
    }

    /**
     * @brief 执行批量删除操作
     * @return nullptr，因为DELETE不产生结果集
     */
    std::unique_ptr<RmRecord> Next() override {
        for (auto &rec_tuple : old_recs_) {
            auto &base_meta = std::get<0>(rec_tuple);
            auto &old_rec = std::get<1>(rec_tuple);
            auto &rid = std::get<2>(rec_tuple);
            auto link = txn_manager.GetUndoLink(fh_->GetFd(), rid);

            if (IsWriteWriteConflict(context_->txn_, &txn_manager, link)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            }
            TupleMeta new_meta(context_->txn_->get_transaction_id(), true);
            if (!txn_manager.UpdateTupleAndUndoLink(tab_name_, fh_, rid, base_meta, new_meta, old_rec, nullptr,
                                                    context_->txn_)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            }
            // context_->log_mgr_->add_delete_log(context_->txn_->get_transaction_id(), std::move(old_rec), rid,
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
    std::string getType() override { return "MvccDeleteExecutor"; }
};