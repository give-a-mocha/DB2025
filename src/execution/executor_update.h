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
 * @brief 更新执行器，负责执行UPDATE语句
 *
 * 主要功能：
 * 1. 更新指定表中满足条件的记录
 * 2. 维护索引的一致性
 * 3. 保证事务的原子性
 *
 * 实现策略：
 * 1. 四阶段更新过程：
 *    - 准备新记录
 *    - 删除旧索引
 *    - 插入新索引
 *    - 更新记录数据
 * 2. 任何阶段失败都进行完整回滚
 * 3. 支持批量更新多条记录
 */
class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    /**
     * @brief 构造函数
     *
     * 初始化更新执行器的各个组件：
     * 1. 设置系统管理器和执行上下文
     * 2. 获取表的元数据和文件句柄
     * 3. 保存更新条件和目标记录
     *
     * @param sm_manager 系统管理器指针
     * @param tab_name 要更新的表名
     * @param set_clauses 更新的赋值语句
     * @param conds 更新条件
     * @param rids 要更新的记录RID列表
     * @param context 执行上下文
     */
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    /**
     * @brief 从所有索引中删除记录的索引项
     *
     * 遍历表的所有索引，生成索引键并删除对应条目。
     * 这是更新操作的第二阶段。
     *
     * @param rec 要删除索引的记录
     * @param rid_ 记录的RID
     */
    void delete_index(RmRecord *rec, Rid rid_) {
        // 从索引中删除
        for (auto &index : tab_.indexes) {
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            auto key = std::make_unique<char[]>(index.col_tot_len);
            int offset = 0;
            for (size_t i = 0; i < static_cast<size_t>(index.col_num); ++i) {
                memcpy(key.get() + offset, rec->data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            ih->delete_entry(key.get(), context_->txn_);
        }
    }

    // 重新插入索引的辅助方法（用于回滚）
    /**
     * @brief 重新插入记录的所有索引项
     *
     * 用于更新操作失败时的回滚。
     * 遍历所有索引，重新插入之前删除的索引项。
     *
     * @param rec 要重新插入索引的记录
     * @param rid_ 记录的RID
     * @throw RMDBError 如果索引重插入失败
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
     * @brief 为更新后的记录插入新的索引项
     *
     * 这是更新操作的第三阶段。主要步骤：
     * 1. 遍历所有索引并插入新的索引项
     * 2. 如果任何索引插入失败，回滚已插入的索引
     * 3. 维护已插入键的列表用于可能的回滚
     *
     * @param rec 要插入索引的记录
     * @param rid_ 记录的RID
     * @return 所有索引插入成功返回true，否则返回false
     */
    bool insert_index(RmRecord *rec, Rid rid_) {
        std::vector<std::unique_ptr<char[]>> inserted_keys;  // 记录已插入的键值
        inserted_keys.reserve(tab_.indexes.size());          // 预分配空间以提高性能

        // 插入索引
        for (size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto &index = tab_.indexes[i];
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
                // 回滚已插入的索引
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
     *
     * 实现四阶段更新过程，确保事务的原子性：
     * 1. 准备阶段：创建所有新记录
     * 2. 删除阶段：删除所有旧索引
     * 3. 插入阶段：插入所有新索引
     * 4. 更新阶段：更新所有记录数据
     *
     * 错误处理：
     * - 如果任何阶段失败，执行完整的回滚操作
     * - 恢复所有旧索引和记录数据
     *
     * @return 始终返回nullptr，因为UPDATE操作不产生结果集
     * @throw RMDBError 当更新操作失败需要回滚时
     */
    std::unique_ptr<RmRecord> Next() override {
        std::vector<std::unique_ptr<RmRecord>> old_records;  // 保存旧记录用于回滚
        std::vector<std::unique_ptr<RmRecord>> new_records;  // 保存新记录
        old_records.reserve(rids_.size());
        new_records.reserve(rids_.size());

        // 第一阶段：准备所有新记录
        for (size_t i = 0; i < rids_.size(); ++i) {
            auto &rid = rids_[i];
            // 获取旧记录
            auto old_rec = fh_->get_record(rid, context_);
            auto new_rec = fh_->get_record(rid, context_);

            for (const auto &set_clause : set_clauses_) {
                auto col = tab_.get_col(set_clause.lhs.col_name);
                // 一定要拷贝
                auto value = set_clause.rhs;
                value.raw.reset();
                if (col->type != set_clause.rhs.type) {
                    // 类型不匹配，值类型尝试转换为列类型
                    if (col->type == ColType::TYPE_INT && value.type == ColType::TYPE_FLOAT) {
                        value.set_int(static_cast<int>(value.float_val));
                    } else if (col->type == ColType::TYPE_FLOAT && value.type == ColType::TYPE_INT) {
                        value.set_float(static_cast<float>(value.int_val));
                    } else {
                        throw IncompatibleTypeError(coltype2str(col->type), coltype2str(value.type));
                    }
                }
                // 设置新记录的对应列
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
                // 插入新索引失败
                // 1. 回滚 (删除) 所有在此次更新中已成功插入的新索引 (从 0 到 i-1)
                for (size_t k = 0; k < i; ++k) {
                    delete_index(new_records[k].get(), rids_[k]);
                }
                // 2. 恢复 (重新插入) 所有在第二阶段删除的旧索引
                for (size_t j = 0; j < rids_.size(); ++j) {
                    reinsert_index(old_records[j].get(), rids_[j]);
                }
                throw RMDBError("Failed to insert new index, rolled back all changes at " + getType());
            }
        }

        // 第四阶段：更新所有记录
        for (size_t i = 0; i < rids_.size(); ++i) {
            fh_->update_record(rids_[i], new_records[i]->data, context_);
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
