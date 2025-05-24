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
#include <string_view>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    ColMeta col_;  // 单键排序的数据结构
    size_t tuple_num;
    bool is_desc_;
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev,
                 const TabCol& sel_col,
                 bool is_desc) {
        prev_ = std::move(prev);
        auto pos = get_col(prev_->cols(), sel_col);
        col_ = *pos;
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
        used_tuple.push_back(now);
    }

    void nextTuple() override {
        prev_->beginTuple();
        int cnt = 0;
        int now = -1;
        current_tuple = nullptr;
        while (!prev_->is_end()) {
            if (std::find(used_tuple.begin(), used_tuple.end(), cnt) ==
                    used_tuple.end() &&
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

    std::unique_ptr<RmRecord> Next() override {
        return std::move(current_tuple);
    }

    const std::vector<ColMeta>& cols() const override { return prev_->cols(); }

    Rid& rid() override { return _abstract_rid; }

    bool cmp(std::unique_ptr<RmRecord> a, std::unique_ptr<RmRecord>& b) {
        if (b == nullptr) {
            return true;
        }
        
        char* rec_buf_a = a->data + col_.offset;
        char* rec_buf_b = b->data + col_.offset;
        
        if (col_.type == TYPE_INT) {
            int value_a = *(int*)rec_buf_a;
            int value_b = *(int*)rec_buf_b;
            if (is_desc_) return value_a > value_b;
            else return value_a < value_b;
        } else if (col_.type == TYPE_FLOAT) {
            double value_a = *(double*)rec_buf_a;
            double value_b = *(double*)rec_buf_b;
            if (is_desc_) return value_a > value_b;
            else return value_a < value_b;
        } else if (col_.type == TYPE_STRING) {
            // 使用 string_view 进行更安全的字符串比较
            // 先找到实际的字符串长度（去除尾部空字符）
            size_t a_actual_len = std::min(static_cast<size_t>(col_.len),
                                           std::strlen(rec_buf_a));
            size_t b_actual_len = std::min(static_cast<size_t>(col_.len),
                                           std::strlen(rec_buf_b));

            // 创建 string_view 进行比较，避免不必要的拷贝
            std::string_view value_a(rec_buf_a, a_actual_len);
            std::string_view value_b(rec_buf_b, b_actual_len);

            if (is_desc_) return value_a > value_b;
            else return value_a < value_b;
        }
        return false;
    }
};