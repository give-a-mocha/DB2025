/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
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

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    ColMeta cols_;                              // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    size_t tuple_num;
    bool is_desc_;
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc) {
        prev_ = std::move(prev);
        cols_ = prev_->get_col_offset(sel_cols);
        is_desc_ = is_desc;
        tuple_num = 0;
        used_tuple.clear();
    }

    void beginTuple() override { 
        
    }

    void nextTuple() override {
        
    }

    std::unique_ptr<RmRecord> Next() override { return std::move(current_tuple); }

    const std::vector<ColMeta>& cols() const override { return prev_->cols(); }

    Rid& rid() override { return _abstract_rid; }

    bool cmp(std::unique_ptr<RmRecord> a, std::unique_ptr<RmRecord>& b) {
        if (b == nullptr) {
            return true;
        }

        char* rec_buf_a = a->data + col_.offset;
        char* rec_buf_b = b->data + col_.offset;

        if (col_.type == TYPE_INT) {
            int value_a = *reinterpret_cast<int*>(rec_buf_a);
            int value_b = *reinterpret_cast<int*>(rec_buf_b);
            if (is_desc_) return value_a > value_b;
            else return value_a < value_b;
        } else if (col_.type == TYPE_FLOAT) {
            double value_a = *reinterpret_cast<double*>(rec_buf_a);
            double value_b = *reinterpret_cast<double*>(rec_buf_b);
            if (is_desc_) return value_a > value_b;
            else return value_a < value_b;
        } else if (col_.type == TYPE_STRING) {
            int comparison_result = strncmp(rec_buf_a, rec_buf_b, static_cast<size_t>(col_.len));
            if (is_desc_) {
                return comparison_result > 0;
            } else {
                return comparison_result < 0;
            }
        }
        return false;
    }

    Rid &rid() override { return _abstract_rid; }
};