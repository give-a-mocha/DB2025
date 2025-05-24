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
 * @brief 实现查询执行管道中的投影操作，仅输出指定的列。
 *
 * 构造函数接收一个子执行器和需要投影的列集合，初始化投影列的元数据和偏移。各方法用于控制和获取投影后的元组：`beginTuple()` 和 `nextTuple()` 分别初始化和推进子执行器的元组迭代，`is_end()` 判断是否遍历结束，`Next()` 返回仅包含选中列的新元组（无可用元组时返回 `nullptr`），`tupleLen()` 返回投影后元组的总字节长度，`cols()` 返回投影列的元数据，`rid()` 返回内部记录标识符。
 *
 * @param prev 子执行器，作为投影操作的数据来源。
 * @param sel_cols 需要投影的列集合。
 * @return `Next()` 返回包含投影列的新元组指针，无可用元组时返回 `nullptr`。
 */
class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;  // 投影节点的儿子节点
    std::vector<ColMeta> cols_;               // 需要投影的字段
    size_t len_;                              // 字段总长度
    std::vector<size_t> sel_idxs_;

   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev,
                       const std::vector<TabCol> &sel_cols) {
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

    void beginTuple() override { prev_->beginTuple(); }

    void nextTuple() override { prev_->nextTuple(); }

    bool is_end() const override { return prev_->is_end(); }

    std::unique_ptr<RmRecord> Next() override {
        // Todo:
        // !需要自己实现
        auto prev_rec = prev_->Next();
        if (!prev_rec) return nullptr;

        // 创建新的记录，只包含选中的列
        auto proj_rec = std::make_unique<RmRecord>(len_);
        auto &prev_cols = prev_->cols();

        for (size_t i = 0; i < sel_idxs_.size(); ++i) {
            size_t prev_idx = sel_idxs_[i];
            auto &prev_col = prev_cols[prev_idx];
            auto &proj_col = cols_[i];

            memcpy(proj_rec->data + proj_col.offset,
                   prev_rec->data + prev_col.offset, proj_col.len);
        }

        return proj_rec;
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    Rid &rid() override { return _abstract_rid; }
};
