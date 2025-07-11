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

/**
 * @brief 删除执行器，负责实现DELETE语句的功能
 */
class MvccDeleteExecutor : public AbstractExecutor {
   private:
    TabMeta& tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // 删除条件列表
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<std::tuple<TupleMeta, std::unique_ptr<RmRecord>, Rid>> old_recs_;   // 旧记录列表
    std::string tab_name_;          // 表名
    SmManager *sm_manager_;         // 系统管理器指针
    TransactionManager *txn_mgr_;   // 事务管理器指针

   public:
    MvccDeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                      std::vector<std::tuple<TupleMeta, std::unique_ptr<RmRecord>, Rid>> old_recs, Context *context, TransactionManager *txn_mgr)
                      :tab_(sm_manager->db_.get_table(tab_name)) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        old_recs_ = std::move(old_recs);
        context_ = context;
        txn_mgr_ = txn_mgr;
    }

    /**
     * @brief 执行批量删除操作
     * @return nullptr，因为DELETE不产生结果集
     */
    std::unique_ptr<RmRecord> Next() override {
        // 添加间隙锁
        txn_mgr_->get_lock_manager()->lock_gap(context_->txn_, fh_->GetFd(), conds_);
        for (auto &rec_tuple : old_recs_) {
            auto &base_meta = std::get<0>(rec_tuple);
            auto &old_rec = std::get<1>(rec_tuple);
            auto &rid = std::get<2>(rec_tuple);
            auto link = txn_mgr_->GetUndoLink(fh_->GetFd(), rid);
            // 先获取写锁
            if (IsWriteWriteConflict(context_->txn_, txn_mgr_, link)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            }
            bool ok = txn_mgr_->get_lock_manager()->lock_exclusive_on_record(context_->txn_, rid, fh_->GetFd());
            if (!ok) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
            if (base_meta.is_deleted_) {
                // 如果元组已经被删除，跳过
                continue;
            }
            TupleMeta delete_meta;
            delete_meta.is_deleted_ = true;  // 设置元组为已删除状态
            fh_->update_tuple_meta(rid, delete_meta);
            if (link.IsValid() && link.prev_txn_ != context_->txn_->get_transaction_id()) {
                txn_mgr_->GenerateNewUndoLog(fh_->GetFd(), rid, old_rec, base_meta, context_->txn_);
                context_->txn_->append_write_record(
                    std::make_unique<WriteRecord>(tab_name_, rid));
            }
            context_->log_mgr_->add_delete_log(context_->txn_->get_transaction_id(), std::move(old_rec), rid, tab_.name);
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