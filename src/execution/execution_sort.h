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

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 排序执行器，实现查询结果的排序操作
 *
 * 主要功能：
 * 1. 根据指定列对查询结果进行排序
 * 2. 支持升序和降序排序
 * 3. 提供迭代器接口访问排序后的结果
 *
 * 实现策略：
 * - 使用选择排序算法
 * - 维护已使用tuple的索引，避免重复选择
 * - 支持多种数据类型的比较操作
 */
class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;  // 前序执行器
    ColMeta col_;                             // 排序字段的元数据
    size_t tuple_num;                         // 处理的元组数量
    bool is_desc_;                            // 是否为降序排序
    std::vector<size_t> used_tuple;           // 已处理的元组索引
    std::unique_ptr<RmRecord> current_tuple;  // 当前处理的元组

   public:
    /**
     * @brief 构造函数
     * @param prev 前序执行器
     * @param sel_col 用于排序的列
     * @param is_desc 是否为降序排序
     */
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const TabCol& sel_col, bool is_desc) {
        prev_ = std::move(prev);
        auto pos = get_col(prev_->cols(), sel_col);
        col_ = *pos;
        is_desc_ = is_desc;
        tuple_num = 0;
        used_tuple.clear();
        current_tuple = nullptr;
    }

    /**
     * @brief 开始处理元组序列
     *
     * 初始化排序过程，选择第一个最小（或最大）元素
     */
    void beginTuple() override {
        prev_->beginTuple();
        int cnt = 0;
        int now = -1;
        current_tuple = nullptr;
        while (!prev_->is_end()) {
            if (cmp(prev_->Next(), current_tuple)) {
                current_tuple = prev_->Next();
                now = cnt;
            }
            prev_->nextTuple();
            cnt++;
        }
        tuple_num++;
        used_tuple.push_back(now);
    }

    /**
     * @brief 获取下一个元组
     *
     * 在未处理的元组中选择下一个最小（或最大）元素
     */
    void nextTuple() override {
        prev_->beginTuple();
        int cnt = 0;
        int now = -1;
        current_tuple = nullptr;
        while (!prev_->is_end()) {
            if (std::find(used_tuple.begin(), used_tuple.end(), cnt) == used_tuple.end() &&
                cmp(prev_->Next(), current_tuple)) {
                current_tuple = prev_->Next();
                now = cnt;
            }
            prev_->nextTuple();
            cnt++;
        }
        tuple_num++;
        used_tuple.push_back(now);
    }

    std::unique_ptr<RmRecord> Next() override { return std::move(current_tuple); }

    const std::vector<ColMeta>& cols() const override { return prev_->cols(); }

    Rid& rid() override { return _abstract_rid; }

    /**
     * @brief 比较两个元组在排序字段上的值
     * @param a 第一个元组
     * @param b 第二个元组
     * @return 根据排序规则(升序/降序)返回比较结果
     */
    bool cmp(std::unique_ptr<RmRecord> a, std::unique_ptr<RmRecord>& b) {
        if (b == nullptr) {
            return true;
        }

        char* rec_buf_a = a->data + col_.offset;
        char* rec_buf_b = b->data + col_.offset;

        if (col_.type == ColType::TYPE_INT) {
            int value_a = *reinterpret_cast<int*>(rec_buf_a);
            int value_b = *reinterpret_cast<int*>(rec_buf_b);
            if (is_desc_) return value_a > value_b;
            else return value_a < value_b;
        } else if (col_.type == ColType::TYPE_FLOAT) {
            double value_a = *reinterpret_cast<double*>(rec_buf_a);
            double value_b = *reinterpret_cast<double*>(rec_buf_b);
            if (is_desc_) return value_a > value_b;
            else return value_a < value_b;
        } else if (col_.type == ColType::TYPE_STRING) {
            int comparison_result = strncmp(rec_buf_a, rec_buf_b, static_cast<size_t>(col_.len));
            if (is_desc_) {
                return comparison_result > 0;
            } else {
                return comparison_result < 0;
            }
        }
        return false;
    }

    std::string getType() override { return "SortExecutor"; }
};