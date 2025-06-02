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
 * @brief 投影执行器，负责从输入记录中选择指定的列
 *
 * 主要功能：
 * 1. 从输入记录中抽取指定的列
 * 2. 重新组织列的布局，调整偏移量
 * 3. 生成新的包含选定列的记录
 *
 * 实现策略：
 * 1. 在构造时计算新记录的布局
 * 2. 记录选中列在原记录中的索引
 * 3. 执行时将选中的列复制到新记录中
 */
class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;  // 投影节点的儿子节点
    std::vector<ColMeta> cols_;               // 需要投影的字段
    size_t len_;                              // 字段总长度
    std::vector<size_t> sel_idxs_;

   public:
    /**
     * @brief 构造函数
     *
     * 初始化投影执行器：
     * 1. 设置输入执行器
     * 2. 计算输出记录的列布局
     * 3. 记录需要选择的列的索引位置
     *
     * @param prev 输入执行器
     * @param sel_cols 需要投影的列
     */
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;
    }

    /**
     * @brief 开始处理第一个元组
     * 调用输入执行器的beginTuple开始扫描
     */
    void beginTuple() override { prev_->beginTuple(); }

    /**
     * @brief 移动到下一个元组
     * 调用输入执行器的nextTuple继续扫描
     */
    void nextTuple() override { prev_->nextTuple(); }

    /**
     * @brief 检查是否完成所有元组的处理
     * @return 如果输入执行器到达末尾则返回true
     */
    bool is_end() const override { return prev_->is_end(); }

    /**
     * @brief 获取投影后的下一条记录
     *
     * 处理步骤：
     * 1. 获取输入执行器的下一条记录
     * 2. 创建新记录并分配空间
     * 3. 将选中的列从输入记录复制到新记录
     *
     * @return 投影后的记录指针，如果输入记录为空则返回nullptr
     */
    std::unique_ptr<RmRecord> Next() override {
        // Todo:
        // !需要自己实现
        auto prev_rec = prev_->Next();
        if (!prev_rec) {
            std::cerr << "Error: Previous record is null at " + getType() << std::endl;
            return nullptr;
        }

        // 创建新的记录，只包含选中的列
        auto proj_rec = std::make_unique<RmRecord>(len_);
        auto &prev_cols = prev_->cols();

        for (size_t i = 0; i < sel_idxs_.size(); ++i) {
            size_t prev_idx = sel_idxs_[i];
            auto &prev_col = prev_cols[prev_idx];
            auto &proj_col = cols_[i];

            memcpy(proj_rec->data + proj_col.offset, prev_rec->data + prev_col.offset, proj_col.len);
        }

        return proj_rec;
    }

    /**
     * @brief 获取投影后记录的长度
     * @return 投影后记录的总字节数
     */
    size_t tupleLen() const override { return len_; }

    /**
     * @brief 获取投影后的列元数据
     * @return 投影列的元数据向量引用
     */
    const std::vector<ColMeta> &cols() const override { return cols_; }

    /**
     * @brief 获取当前记录的RID
     * @return 抽象RID的引用（投影结果没有实际的RID）
     */
    Rid &rid() override { return _abstract_rid; }

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    std::string getType() override { return "ProjectionExecutor"; }
};
