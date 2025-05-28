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

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;  // 多键排序的数据结构
    size_t tuple_num;
    std::vector<bool> is_desc_;
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;

   public:
    // 单列排序构造函数（向后兼容）
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const TabCol& sel_col, bool is_desc) {
        prev_ = std::move(prev);
        auto pos = get_col(prev_->cols(), sel_col);
        cols_.emplace_back(*pos);
        is_desc_.emplace_back(is_desc);
        tuple_num = 0;
        used_tuple.clear();
        current_tuple = nullptr;
    }
    
    // 多列排序构造函数
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol>& sel_cols, const std::vector<bool>& is_desc) {
        prev_ = std::move(prev);
        for (const auto& sel_col : sel_cols) {
            auto pos = get_col(prev_->cols(), sel_col);
            cols_.emplace_back(*pos);
        }
        is_desc_ = is_desc;
        tuple_num = 0;
        used_tuple.clear();
        current_tuple = nullptr;
    }

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
        used_tuple.emplace_back(now);
    }

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
        used_tuple.emplace_back(now);
    }

    std::unique_ptr<RmRecord> Next() override { return std::move(current_tuple); }

    const std::vector<ColMeta>& cols() const override { return prev_->cols(); }

    Rid& rid() override { return _abstract_rid; }

    bool cmp(std::unique_ptr<RmRecord> a, std::unique_ptr<RmRecord>& b) {
        if (b == nullptr) {
            return true;
        }

        // 多列比较：按照优先级依次比较每一列
        for (size_t i = 0; i < cols_.size(); ++i) {
            const ColMeta& col = cols_[i];
            bool desc = is_desc_[i];
            
            char* rec_buf_a = a->data + col.offset;
            char* rec_buf_b = b->data + col.offset;

            int comparison_result = 0;
            if (col.type == ColType::TYPE_INT) {
                int value_a = *reinterpret_cast<int*>(rec_buf_a);
                int value_b = *reinterpret_cast<int*>(rec_buf_b);
                if (value_a < value_b) comparison_result = -1;
                else if (value_a > value_b) comparison_result = 1;
                else comparison_result = 0;
            } else if (col.type == ColType::TYPE_FLOAT) {
                double value_a = *reinterpret_cast<double*>(rec_buf_a);
                double value_b = *reinterpret_cast<double*>(rec_buf_b);
                if (value_a < value_b) comparison_result = -1;
                else if (value_a > value_b) comparison_result = 1;
                else comparison_result = 0;
            } else if (col.type == ColType::TYPE_STRING) {
                comparison_result = strncmp(rec_buf_a, rec_buf_b, static_cast<size_t>(col.len));
            }
            
            // 如果当前列相等，继续比较下一列
            if (comparison_result == 0) {
                continue;
            }
            
            // 根据排序方向返回结果
            if (desc) {
                return comparison_result > 0;
            } else {
                return comparison_result < 0;
            }
        }
        
        // 所有列都相等
        return false;
    }

    std::string getType() override { return "SortExecutor"; }
};