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
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 删除执行器，负责执行DELETE语句
 *
 * 主要功能：
 * 1. 执行表记录的批量删除操作
 * 2. 维护索引的一致性
 * 3. 支持事务处理
 *
 * 实现策略：
 * 1. 根据条件定位要删除的记录
 * 2. 删除索引中对应的条目
 * 3. 删除表中的实际记录
 * 4. 确保删除操作的原子性
 */
class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

   public:
    /**
     * @brief 构造函数
     *
     * 初始化删除执行器：
     * 1. 设置系统管理器和执行上下文
     * 2. 获取表的元数据和文件句柄
     * 3. 保存删除条件和目标记录
     *
     * @param sm_manager 系统管理器指针
     * @param tab_name 要删除数据的表名
     * @param conds 删除条件
     * @param rids 要删除的记录RID列表
     * @param context 执行上下文
     */
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                    std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    /**
     * @brief 从所有索引中删除记录对应的索引项
     *
     * 执行步骤：
     * 1. 遍历表的所有索引
     * 2. 为每个索引生成对应的键值
     * 3. 删除索引中对应的条目
     *
     * @param rec 要删除的记录指针
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

    /**
     * @brief 执行批量删除操作
     *
     * 对每条要删除的记录：
     * 1. 获取记录数据
     * 2. 删除记录对应的所有索引项
     * 3. 从表中删除记录本身
     *
     * @return 始终返回nullptr，因为DELETE操作不产生结果集
     */
    std::unique_ptr<RmRecord> Next() override {
        for (auto &rid : rids_) {
            // 获取要删除的记录
            auto rec = fh_->get_record(rid, context_);

            delete_index(rec.get(), rid);

            // 从表中删除记录
            fh_->delete_record(rid, context_);
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
    std::string getType() override { return "DeleteExecutor"; }
};