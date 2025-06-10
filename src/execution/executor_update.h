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

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 更新执行器，负责实现UPDATE语句的功能
 */
class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                         // 表的元数据
    std::vector<Condition> conds_;        // 更新条件列表
    RmFileHandle *fh_;                    // 表的数据文件句柄
    std::vector<Rid> rids_;               // 待更新记录的RID列表
    std::string tab_name_;                // 表名
    std::vector<SetClause> set_clauses_;  // SET子句列表(新值)
    SmManager *sm_manager_;               // 系统管理器指针

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
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = std::move(set_clauses);
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;
    }

    /**
     * @brief 从所有索引中删除记录的索引项
     * @param rec 要删除索引的记录指针
     * @param rid_ 记录的RID
     */
    void delete_index(RmRecord *rec, Rid rid_) {
        // 遍历所有索引
        for (auto &index : tab_.indexes) {
            // 获取索引句柄
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();

            // 构造索引键值
            auto key = std::make_unique<char[]>(index.col_tot_len);
            int offset = 0;
            for (size_t i = 0; i < static_cast<size_t>(index.col_num); ++i) {
                memcpy(key.get() + offset, rec->data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }

            // 从索引中删除条目
            ih->delete_entry(key.get(), context_->txn_);
        }
    }

    /**
     * @brief 重新插入记录的所有索引项(用于回滚)
     * @param rec 要恢复索引的记录指针
     * @param rid_ 记录的RID
     * @throw RMDBError 当索引重插入失败时
     */
    void reinsert_index(RmRecord *rec, Rid rid_) {
        // 重新插入索引（用于回滚）
        for (auto &index : tab_.indexes) {
            auto ix_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols);
            auto ih = sm_manager_->ihs_.at(ix_name).get();
            auto key = std::make_unique<char[]>(index.col_tot_len);
            int offset = 0;
            for (int j = 0; j < index.col_num; ++j) {
                memcpy(key.get() + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }
            auto result = ih->insert_entry(key.get(), rid_, context_->txn_);
            if (result == INVALID_PAGE_ID) {
                throw RMDBError("Failed to reinsert index for " + tab_name_ + " at " + getType());
            }
        }
    }

    /**
     * @brief 为新记录创建所有索引项
     * @param rec 要创建索引的记录指针
     * @param rid_ 记录的RID
     * @return true表示所有索引创建成功，false表示失败
     */
    bool insert_index(RmRecord *rec, Rid rid_) {
        std::vector<std::unique_ptr<char[]>> inserted_keys;  // 记录已插入的键值
        inserted_keys.reserve(tab_.indexes.size());          // 预分配空间以提高性能

        // 遍历所有索引
        for (size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto &index = tab_.indexes[i];
            // 获取索引句柄
            auto ix_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols);
            auto ih = sm_manager_->ihs_.at(ix_name).get();

            // 构造索引键值
            auto key = std::make_unique<char[]>(index.col_tot_len);
            int offset = 0;
            for (int j = 0; j < index.col_num; ++j) {
                memcpy(key.get() + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }

            // 尝试插入新索引
            auto result = ih->insert_entry(key.get(), rid_, context_->txn_);
            if (result == INVALID_PAGE_ID) {
                // 插入失败，回滚已插入的索引
                for (size_t rollback_i = 0; rollback_i < i; ++rollback_i) {
                    auto &rollback_index = tab_.indexes[rollback_i];
                    auto rollback_ix_name =
                        sm_manager_->get_ix_manager()->get_index_name(tab_name_, rollback_index.cols);
                    auto rollback_ih = sm_manager_->ihs_.at(rollback_ix_name).get();
                    rollback_ih->delete_entry(inserted_keys[rollback_i].get(), context_->txn_);
                }
                return false;
            }
            inserted_keys.emplace_back(std::move(key));
        }
        return true;
    }

    /**
     * @brief 执行批量更新操作
     * @return nullptr，因为UPDATE不产生结果集
     * @throw IncompatibleTypeError 当值的类型与列类型不兼容时
     * @throw RMDBError 当索引更新失败需要回滚时
     */
    std::unique_ptr<RmRecord> Next() override {
        std::vector<std::unique_ptr<RmRecord>> old_records;  // 保存旧记录用于回滚
        std::vector<std::unique_ptr<RmRecord>> new_records;  // 保存新记录
        old_records.reserve(rids_.size());
        new_records.reserve(rids_.size());

        // 第一阶段：准备所有新记录
        for (size_t i = 0; i < rids_.size(); ++i) {
            auto &rid = rids_[i];
            // 获取旧记录并创建新记录
            auto old_rec = fh_->get_record(rid, context_);
            auto new_rec = fh_->get_record(rid, context_);

            // 处理每个SET子句
            for (const auto &set_clause : set_clauses_) {
                auto col = tab_.get_col(set_clause.lhs.col_name);
                // 复制值以避免修改原始数据
                auto value = set_clause.rhs;
                value.raw.reset();

                // 处理类型转换
                if (col->type != set_clause.rhs.type) {
                    if (col->type == ColType::TYPE_INT && value.type == ColType::TYPE_FLOAT) {
                        value.set_int(static_cast<int>(value.float_val));
                    } else if (col->type == ColType::TYPE_FLOAT && value.type == ColType::TYPE_INT) {
                        value.set_float(static_cast<float>(value.int_val));
                    } else {
                        throw IncompatibleTypeError(coltype2str(col->type), coltype2str(value.type));
                    }
                }

                // 更新新记录中的值
                value.init_raw(col->len);
                memcpy(new_rec->data + col->offset, value.raw->data, col->len);
            }

            old_records.emplace_back(std::move(old_rec));
            new_records.emplace_back(std::move(new_rec));
        }

        // 第二阶段：删除所有旧索引
        for (size_t i = 0; i < rids_.size(); ++i) {
            delete_index(old_records[i].get(), rids_[i]);
        }

        // 第三阶段：插入所有新索引
        for (size_t i = 0; i < rids_.size(); ++i) {
            if (!insert_index(new_records[i].get(), rids_[i])) {
                // 索引插入失败，执行完整回滚
                // 1. 删除已插入的新索引
                for (size_t k = 0; k < i; ++k) {
                    delete_index(new_records[k].get(), rids_[k]);
                }
                // 2. 恢复所有旧索引
                for (size_t j = 0; j < rids_.size(); ++j) {
                    reinsert_index(old_records[j].get(), rids_[j]);
                }
                throw RMDBError("Failed to insert new index, rolled back all changes at " + getType());
            }
        }

        // 第四阶段：更新所有记录数据
        for (size_t i = 0; i < rids_.size(); ++i) {
            fh_->update_record(rids_[i], new_records[i]->data, context_);
            context_->txn_->append_write_record(
                std::make_unique<WriteRecord>(WType::UPDATE_TUPLE, tab_name_, rids_[i], *old_records[i])
            );
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
    std::string getType() override { return "UpdateExecutor"; }
};
