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

extern SmManager sm_manager;
extern TransactionManager txn_manager;

/**
 * @brief 插入执行器，负责实现INSERT语句的功能
 */
class MvccInsertExecutor : public AbstractExecutor {
   private:
    TabMeta &tab_;               // 表的元数据
    std::vector<Value> values_;  // 待插入的值列表
    RmFileHandle *fh_;           // 表的数据文件句柄
    Rid rid_;                    // 插入记录的位置(插入成功后赋值)

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     * @param tab_name 目标表名
     * @param values 要插入的值列表
     * @param context 执行上下文
     * @throw InvalidValueCountError 当值的数量与表的列数不匹配时
     */
    MvccInsertExecutor(std::string tab_name, std::vector<Value> values, Context *context)
        : tab_(sm_manager.db_.get_table(tab_name)) {
        values_ = std::move(values);
        // 检查插入值的数量是否与表的列数匹配
        if (values_.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager.fhs_.at(tab_.name).get();
        context_ = context;
    };

    /**
     * @brief 执行插入操作
     * @return nullptr，因为INSERT不产生结果集
     * @throw IncompatibleTypeError 当值的类型与列类型不兼容时
     * @throw RMDBError 当索引更新失败需要回滚时
     */
    std::unique_ptr<RmRecord> Next() override {
        TRACE_FUNCTION
        // 创建记录缓冲区
        std::unique_ptr<RmRecord> rec = std::make_unique<RmRecord>(fh_->get_file_hdr().record_size);

        // 处理每个字段的值
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];

            // 处理类型不匹配的情况
            if (col.type != values_[i].type) {
                if (col.type == ColType::TYPE_INT && values_[i].type == ColType::TYPE_FLOAT) {
                    values_[i].set(static_cast<int>(values_[i].float_val));
                } else if (col.type == ColType::TYPE_FLOAT && values_[i].type == ColType::TYPE_INT) {
                    values_[i].set(static_cast<float>(values_[i].int_val));
                } else {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(values_[i].type));
                }
            }
            // 设置记录数据
            values_[i].set_record_data(rec->data + col.offset, col.len);
        }
        std::vector<Rid> rids = sm_manager.exist_in_index(tab_, rec, context_->txn_);

        // 唯一性检查
        TupleMeta base_meta_(0, true);
        for (const auto &rid : rids) {
            auto [base_meta, old_rec, link] = txn_manager.GetTupleAndUndoLink(fh_, rid);
            base_meta_ = base_meta;
            if (IsWriteWriteConflict(context_->txn_, link)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            }
            // 主键冲突
            if (base_meta.is_deleted_ == false) {
                txn_manager.abort(context_);
                throw InternalError("Primary key conflict, duplicate insert");
            }
        }
        TupleMeta new_meta(context_->txn_->get_transaction_id(), false);
        if (!rids.empty()) {
            //! 这里使用back在唯一索引下才是对的
            rid_ = rids.back();
            if (!txn_manager.UpdateTupleAndUndoLink(tab_.name, fh_, rid_, base_meta_, new_meta, nullptr, rec,
                                                    context_->txn_)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            }
        } else {
            rid_ = fh_->GetNewRid();
            TupleMeta base_meta(0, true);
            if (!txn_manager.UpdateTupleAndUndoLink(tab_.name, fh_, rid_, base_meta, new_meta, nullptr, rec,
                                                    context_->txn_)) {
                fh_->delete_record(rid_);
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            }
            sm_manager.insert_index_with_tab_meta(tab_, rec, rid_, context_->txn_);
        }
        // context_->log_mgr_->add_insert_log(context_->txn_->get_transaction_id(), std::move(rec), rid_, tab_.name);
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