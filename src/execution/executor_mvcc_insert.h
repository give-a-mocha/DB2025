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

#include "execution/execution_common.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 插入执行器，负责实现INSERT语句的功能
 */
class MvccInsertExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                  // 表的元数据
    std::vector<Value> values_;    // 待插入的值列表
    RmFileHandle *fh_;             // 表的数据文件句柄
    std::string tab_name_;         // 表名
    Rid rid_;                      // 插入记录的位置(插入成功后赋值)
    SmManager *sm_manager_;        // 系统管理器指针
    TransactionManager *txn_mgr_;  // 事务管理器指针

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     * @param tab_name 目标表名
     * @param values 要插入的值列表
     * @param context 执行上下文
     * @throw InvalidValueCountError 当值的数量与表的列数不匹配时
     */
    MvccInsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context,
                       TransactionManager *txn_mgr) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        // 检查插入值的数量是否与表的列数匹配
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
        txn_mgr_ = txn_mgr;
    };

    /**
     * @brief 执行插入操作
     * @return nullptr，因为INSERT不产生结果集
     * @throw IncompatibleTypeError 当值的类型与列类型不兼容时
     * @throw RMDBError 当索引更新失败需要回滚时
     */
    std::unique_ptr<RmRecord> Next() override {
        // 创建记录缓冲区
        std::unique_ptr<RmRecord> rec = std::make_unique<RmRecord>(fh_->get_file_hdr().record_size);

        // 处理每个字段的值
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];

            // 处理类型不匹配的情况
            if (col.type != values_[i].type) {
                if (col.type == ColType::TYPE_INT && values_[i].type == ColType::TYPE_FLOAT) {
                    values_[i].set_int(static_cast<int>(values_[i].float_val));
                } else if (col.type == ColType::TYPE_FLOAT && values_[i].type == ColType::TYPE_INT) {
                    values_[i].set_float(static_cast<float>(values_[i].int_val));
                } else {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(values_[i].type));
                }
            }

            // 复制值到记录中
            values_[i].init_raw(col.len);
            memcpy(rec->data + col.offset, values_[i].raw->data, col.len);
        }

        // 获取全局条件
        std::vector<Condition> conds = txn_mgr_->get_lock_manager()->get_gap_condition(fh_->GetFd(), context_->txn_);

        if (!conds.empty() && eval_conds(tab_.cols, conds, rec)) {
            throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
        }
        rid_ = fh_->insert_record(rec->data, context_);
        txn_mgr_->get_lock_manager()->lock_exclusive_on_record(context_->txn_, rid_, fh_->GetFd());
        // 添加日志要在插入索引之后，因为abort会回滚索引
        if (!mvcc_insert_index(tab_, rec, rid_, context_, txn_mgr_, sm_manager_)) {
            fh_->delete_record(rid_, context_);
            txn_mgr_->abort(context_, context_->log_mgr_);
            throw RMDBError("Failed to insert into index, rolled back record insertion at " + getType());
        }
        txn_mgr_->add_insert_undo_log(context_->txn_, fh_->GetFd(), rid_);
        context_->txn_->append_write_record(std::make_unique<WriteRecord>(WType::INSERT_TUPLE, tab_.name, rid_, *rec));
        context_->log_mgr_->add_insert_log(context_->txn_->get_transaction_id(), *rec, rid_, tab_.name);
        return nullptr;
    }

    /**
     * @brief 获取插入记录的RID
     * @return 插入记录的RID引用
     */
    Rid &rid() override { return rid_; }

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    std::string getType() override { return "MvccInsertExecutor"; }
};