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
#include <algorithm>
#include <vector>
#include <memory>
#include <string>
#include <cstring>

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    ColMeta cols_;                              // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    size_t tuple_num;
    bool is_desc_;
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;
    std::vector<std::unique_ptr<RmRecord>> sorted_records_;  // 存储排序后的记录
    size_t current_index_;                      // 当前记录索引

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc) {
        prev_ = std::move(prev);
        cols_ = prev_->get_col_offset(sel_cols);
        is_desc_ = is_desc;
        tuple_num = 0;
        used_tuple.clear();
        current_index_ = 0;
    }

    void beginTuple() override {
        // 收集所有记录
        sorted_records_.clear();
        prev_->beginTuple();
        while (!prev_->is_end()) {
            auto record = prev_->Next();
            if (record) {
                sorted_records_.push_back(std::move(record));
            }
            prev_->nextTuple();
        }
        
        tuple_num = sorted_records_.size();
        
        // 根据排序列进行排序
        std::sort(sorted_records_.begin(), sorted_records_.end(),
                  [this](const std::unique_ptr<RmRecord>& a, const std::unique_ptr<RmRecord>& b) {
                      char* a_data = a->data + cols_.offset;
                      char* b_data = b->data + cols_.offset;
                      
                      int cmp = 0;
                      if (cols_.type == TYPE_INT) {
                          cmp = *(int*)a_data - *(int*)b_data;
                      } else if (cols_.type == TYPE_FLOAT) {
                          float diff = *(float*)a_data - *(float*)b_data;
                          cmp = (diff > 0) ? 1 : (diff < 0) ? -1 : 0;
                      } else if (cols_.type == TYPE_STRING) {
                          // 将字符数据转换为字符串进行比较
                          std::string a_str(a_data, cols_.len);
                          std::string b_str(b_data, cols_.len);
                          
                          // 去除尾部的空字符
                          a_str.resize(strlen(a_str.c_str()));
                          b_str.resize(strlen(b_str.c_str()));
                          
                          // 使用字符串比较
                          if (a_str < b_str) {
                              cmp = -1;
                          } else if (a_str > b_str) {
                              cmp = 1;
                          } else {
                              cmp = 0;
                          }
                      }
                      
                      return is_desc_ ? (cmp > 0) : (cmp < 0);
                  });
        
        current_index_ = 0;
    }

    void nextTuple() override {
        if (current_index_ < sorted_records_.size()) {
            current_index_++;
        }
    }

    bool is_end() const override {
        return current_index_ >= sorted_records_.size();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        
        // 创建当前记录的副本
        auto& current = sorted_records_[current_index_];
        auto result = std::make_unique<RmRecord>(current->size);
        memcpy(result->data, current->data, current->size);
        return result;
    }

    size_t tupleLen() const override {
        return prev_->tupleLen();
    }

    const std::vector<ColMeta> &cols() const override {
        return prev_->cols();
    }

    Rid &rid() override { return _abstract_rid; }
};