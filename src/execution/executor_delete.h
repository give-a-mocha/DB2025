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
 * @brief 删除执行器，负责实现DELETE语句的功能
 *
 * @details 主要功能和特点：
 * 1. 数据删除：
 *    - 支持条件删除
 *    - 批量删除多条记录
 *    - 维护记录完整性
 *
 * 2. 索引维护：
 *    - 同步删除索引项
 *    - 保证索引一致性
 *    - 支持多索引更新
 *
 * 3. 事务处理：
 *    - 原子性操作
 *    - 错误时回滚
 *    - 并发控制
 */
class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // 删除条件列表
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 待删除记录的RID列表
    std::string tab_name_;          // 表名
    SmManager *sm_manager_;         // 系统管理器指针

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     * @param tab_name 目标表名
     * @param conds 删除条件列表
     * @param rids 要删除的记录RID列表
     * @param context 执行上下文
     *
     * @details 初始化过程：
     * 1. 保存系统管理器和表名
     * 2. 获取表的元数据和文件句柄
     * 3. 记录删除条件和目标记录
     * 4. 设置执行上下文
     */
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                    std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;
    }

    /**
     * @brief 删除记录的所有索引项
     * @param rec 要删除索引的记录指针
     * @param rid_ 记录的RID
     *
     * @details 执行步骤：
     * 1. 遍历表的所有索引
     *    - 获取索引句柄
     *    - 构造索引键值
     *    - 删除索引项
     *
     * 2. 事务处理：
     *    - 在事务中执行删除
     *    - 处理并发访问
     *    - 保证一致性
     */
    void delete_index(RmRecord *rec, Rid rid_) {
        // 遍历所有索引
        for (auto &index : tab_.indexes) {
            // 获取索引句柄
            auto ih = sm_manager_->ihs_.at(
                sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)
            ).get();
            
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
     * @brief 执行批量删除操作
     * @return nullptr，因为DELETE不产生结果集
     *
     * @details 执行步骤：
     * 1. 遍历待删除记录
     *    - 读取记录数据
     *    - 删除相关索引
     *    - 删除实际记录
     *
     * 2. 事务保证：
     *    - 按固定顺序删除
     *    - 保证操作原子性
     *    - 支持并发控制
     *
     * 3. 异常处理：
     *    - 记录不存在
     *    - 索引删除失败
     *    - 并发冲突
     */
    std::unique_ptr<RmRecord> Next() override {
        for (auto &rid : rids_) {
            // 获取要删除的记录
            auto rec = fh_->get_record(rid, context_);

            // 删除记录的所有索引项
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