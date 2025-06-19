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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "execution_common.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 删除执行器，负责实现DELETE语句的功能
 */
class MvccDeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // 删除条件列表
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 待删除记录的RID列表
    std::string tab_name_;          // 表名
    SmManager *sm_manager_;         // 系统管理器指针
    TransactionManager *txn_mgr_;   // 事务管理器指针

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     * @param tab_name 目标表名
     * @param conds 删除条件列表
     * @param rids 要删除的记录RID列表
     * @param context 执行上下文
     */
    MvccDeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                    std::vector<Rid> rids, Context *context, TransactionManager *txn_mgr) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;
        txn_mgr_ = txn_mgr;
    }

    /**
     * @brief 执行批量删除操作
     * @return nullptr，因为DELETE不产生结果集
     */
    std::unique_ptr<RmRecord> Next() override {
        for (auto &rid : rids_) {
            if(!get_lock_and_check_conflict(context_->txn_, txn_mgr_, fh_, rid)){
                continue;
            }
            // 添加间隙锁
            txn_mgr_->get_lock_manager()->lock_gap(context_->txn_, fh_->GetFd(), conds_);
            auto rec = fh_->get_record(rid, context_);
            // fh_->delete_record(rid, context_);
            std::vector<Value> values = convert_record_to_values(rec, tab_.cols);
            txn_mgr_->add_delete_undo_log(context_->txn_, rid, std::move(values));
            context_->txn_->append_write_record(
                std::make_unique<WriteRecord>(WType::DELETE_TUPLE, tab_.name, rid, *rec)
            );
            context_->log_mgr_->add_delete_log(context_->txn_->get_transaction_id(), *rec, rid, tab_.name);
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