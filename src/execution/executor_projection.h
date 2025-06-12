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
 * @brief 投影执行器，负责实现SELECT语句的列选择功能
 */
class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;  // 前序执行器
    std::vector<ColMeta> cols_;               // 输出列元数据
    size_t len_;                              // 结果记录长度
    std::vector<size_t> sel_idxs_;            // 选中列在原记录中的索引

    bool prev_is_aggr_ = false;

   public:
    /**
     * @brief 构造函数
     *
     * @param prev 输入执行器
     * @param sel_cols 需要投影的列
     */
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
        // 转移前序执行器的所有权到当前执行器
        prev_ = std::move(prev);

        // 检查前序执行器是否为聚合执行器
        // 如果是聚合执行器，需要特殊处理列的映射关系
        if (prev_->getType() == "AggregateExecutor") {
            prev_is_aggr_ = true;
        }

        // 初始化当前偏移量，用于计算投影后每列在记录中的位置
        size_t curr_offset = 0;

        // 获取前序执行器的列元数据，作为投影操作的输入源
        auto &prev_cols = prev_->cols();

        // 遍历需要投影的每一列
        for (auto &sel_col : sel_cols) {
            // 在前序执行器的列中查找当前要投影的列
            auto pos = get_col(prev_cols, sel_col);

            // 记录该列在前序执行器结果中的索引位置
            // 用于后续从前序记录中提取对应列的数据
            sel_idxs_.push_back(pos - prev_cols.begin());

            // 复制列的元数据信息
            auto col = *pos;

            // 重新设置列在投影结果中的偏移量
            // 投影后的列会重新排列，偏移量需要重新计算
            col.offset = curr_offset;

            // 累加偏移量，为下一列的位置做准备
            curr_offset += col.len;

            // 将处理好的列元数据添加到投影执行器的列集合中
            cols_.push_back(col);
        }

        // 设置投影后记录的总长度
        len_ = curr_offset;

        // 特殊处理：如果前序是聚合执行器
        // 直接使用前序执行器的列元数据，因为聚合结果的列结构可能已经发生变化
        if (prev_is_aggr_) {
            cols_ = prev_cols;
        }
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
     * @brief 获取投影后的结果记录
     * @return 投影记录的智能指针，如果输入为空返回nullptr
     */
    std::unique_ptr<RmRecord> Next() override {
        // 获取输入记录
        auto prev_rec = prev_->Next();
        if (!prev_rec) {
            WARN("Previous record is null at {}", getType());
            return nullptr;
        }

        // 创建投影结果记录
        auto proj_rec = std::make_unique<RmRecord>(len_);
        auto &prev_cols = prev_->cols();

        // 复制选中的列数据
        for (size_t i = 0; i < sel_idxs_.size(); ++i) {
            size_t prev_idx = sel_idxs_[i];
            if (prev_is_aggr_) {
                prev_idx = i;
            }
            auto &prev_col = prev_cols[prev_idx];
            auto &proj_col = cols_[i];

            // 将列数据复制到新位置
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
