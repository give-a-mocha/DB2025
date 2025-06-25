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
#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/parallel/sort.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 排序执行器，负责实现ORDER BY的排序功能
 */

class SortExecutor : public AbstractExecutor {
   private:
    struct SortColumn {
        ColMeta col;   // 列元数据
        bool is_desc;  // 是否降序

        SortColumn(const ColMeta& c, bool desc) : col(c), is_desc(desc) {}
    };

    std::unique_ptr<AbstractExecutor> prev_;                // 前序执行器
    std::vector<SortColumn> sort_cols_;                     // 排序列信息
    std::vector<std::unique_ptr<RmRecord>> sorted_tuples_;  // 排序后的元组缓存
    size_t current_index_;                                  // 当前访问的元组索引

   public:
    /**
     * @brief 构造函数
     * @param prev 前序执行器
     * @param sel_col 排序列的表列引用
     * @param is_desc 是否降序排序
     */
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol>& sel_cols,
                 const std::vector<bool>& is_desc) {
        prev_ = std::move(prev);
        // 获取所有排序列的元数据
        for (size_t i = 0; i < sel_cols.size(); i++) {
            auto col_meta = get_col(prev_->cols(), sel_cols[i]);
            sort_cols_.emplace_back(*col_meta, is_desc[i]);
        }
        current_index_ = 0;
    }

    /**
     * @brief 初始化排序过程并找到第一个元组
     */
    void beginTuple() override {
        // 清空缓存
        sorted_tuples_.clear();
        current_index_ = 0;

        // 读取所有元组
        prev_->beginTuple();
        while (!prev_->is_end()) {
            sorted_tuples_.push_back(prev_->Next());
            prev_->nextTuple();
        }

        // 使用std::sort排序
        parallel::sort(
            sorted_tuples_.begin(), sorted_tuples_.end(),
            [this](const std::unique_ptr<RmRecord>& a, const std::unique_ptr<RmRecord>& b) { return this->cmp(a, b); });
    }

    /**
     * @brief 获取下一个排序后的元组
     */
    void nextTuple() override {
        if (!is_end()) {
            current_index_++;
        }
    }

    /**
     * @brief 返回当前排序位置的元组
     * @return 当前元组的智能指针
     */
    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*sorted_tuples_[current_index_]);
    }

    bool is_end() const override { return current_index_ >= sorted_tuples_.size(); }

    /**
     * @brief 获取输出列的元数据
     * @return 列元数据向量的常量引用
     */
    const std::vector<ColMeta>& cols() const override { return prev_->cols(); }

    /**
     * @brief 获取当前记录的RID
     * @return 抽象执行器的RID引用
     */
    Rid& rid() override { return _abstract_rid; }

    /**
     * @brief 比较两个元组在排序列上的大小
     * @param a 第一个元组
     * @param b 第二个元组
     * @return true表示a应该在b之前
     */
    bool cmp(const std::unique_ptr<RmRecord>& a, const std::unique_ptr<RmRecord>& b) const {
        // 处理空值情况
        if (!a || !b) {
            if (!a && !b) return false;  // 两个都是null，认为相等
            return !a;                   // null值始终排在最后
        }

        // 逐列比较
        for (const auto& sort_col : sort_cols_) {
            const char* rec_buf_a = a->data + sort_col.col.offset;
            const char* rec_buf_b = b->data + sort_col.col.offset;

            int result = 0;
            switch (sort_col.col.type) {
                case ColType::TYPE_INT: {
                    int value_a = *reinterpret_cast<const int*>(rec_buf_a);
                    int value_b = *reinterpret_cast<const int*>(rec_buf_b);
                    result = (value_a < value_b) ? -1 : (value_a > value_b ? 1 : 0);
                    break;
                }
                case ColType::TYPE_FLOAT: {
                    float value_a = *reinterpret_cast<const float*>(rec_buf_a);
                    float value_b = *reinterpret_cast<const float*>(rec_buf_b);
                    result = (value_a < value_b) ? -1 : (value_a > value_b ? 1 : 0);
                    break;
                }
                case ColType::TYPE_STRING: {
                    result = strncmp(rec_buf_a, rec_buf_b, static_cast<size_t>(sort_col.col.len));
                    break;
                }
                default:
                    continue;  // 跳过不支持的类型
            }

            if (result != 0) {
                return sort_col.is_desc ? (result > 0) : (result < 0);
            }
        }
        return false;  // 所有列都相等
    }

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    std::string getType() override { return "SortExecutor"; }
};