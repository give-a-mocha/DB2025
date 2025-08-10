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
extern LockManager lock_manager;
/**
 * @brief 删除执行器，负责实现DELETE语句的功能
 */
class MvccDeleteExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;  // 前序执行器
    TabMeta &tab_;                                                                 // 表的元数据
    RmFileHandle *fh_;                                                             // 表的数据文件句柄

   public:
    MvccDeleteExecutor(std::unique_ptr<AbstractExecutor> prev, std::string tab_name, Context *context)
        : tab_(sm_manager.db_.get_table(tab_name)) {
        prev_ = std::move(prev);
        fh_ = sm_manager.fhs_.at(tab_name).get();
        context_ = context;
    }

    /**
     * @brief 执行批量删除操作
     * @return nullptr，因为DELETE不产生结果集
     */
    std::unique_ptr<RmRecord> Next() override {
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto &base_meta = prev_->tuple_meta();
            auto old_rec = prev_->Next();
            auto &rid = prev_->rid();
            auto link = txn_manager.GetUndoLink(fh_->GetFd(), rid);

            if (!lock_manager.lock_exclusive_on_record(context_->txn_, rid, fh_->GetFd())) {
                txn_manager.abort(context_);
                lock_manager.wait_for_lock_release(LockDataId(fh_->GetFd(), rid));
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            }

            if (IsWriteWriteConflict(context_->txn_, link)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            }
            TupleMeta new_meta(context_->txn_->get_transaction_id(), true);
            if (!txn_manager.UpdateTupleAndUndoLink(tab_.name, fh_, rid, base_meta, new_meta, old_rec, nullptr,
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