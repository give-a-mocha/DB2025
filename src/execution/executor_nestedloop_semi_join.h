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
#include "common/BatchArray.hpp"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopSemiJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;   // 左子树执行器
    std::unique_ptr<AbstractExecutor> right_;  // 右子树执行器
    size_t len_;                               // 结果记录长度
    std::vector<ColMeta> cols_;                // 结果集列元数据
    std::vector<ColMeta> tot_cols_;            // 左右表总列元数据
    std::vector<Condition> fed_conds_;         // 连接条件列表
    bool _is_end;                              // 扫描结束标志

    // 批处理相关状态
    std::unique_ptr<BatchRecord> left_batch_;    // 当前左表批次
    std::unique_ptr<BatchRecord> right_batch_;   // 当前右表批次
    std::unique_ptr<BatchRecord> result_batch_;  // 结果批次

    // 处理状态
    size_t left_idx_;       // 当前处理的左表记录索引
    bool left_exhausted_;   // 左表是否已用完
    bool right_exhausted_;  // 右表是否已用完

   public:
    NestedLoopSemiJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                               std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }
        tot_cols_ = left_->cols();
        tot_cols_.insert(tot_cols_.end(), right_cols.begin(), right_cols.end());
        _is_end = false;
        fed_conds_ = std::move(conds);

        // 初始化批处理状态
        left_idx_ = 0;
        left_exhausted_ = false;
        right_exhausted_ = false;
    }

    void beginTuple() override {
        TRACE_FUNCTION
        left_->beginTuple();

        // 初始化批处理状态
        left_idx_ = 0;
        left_exhausted_ = false;

        // 获取第一批左表记录
        fetch_left_batch();

        // 生成第一批结果
        generate_result_batch();
    }

    void nextTuple() override {
        TRACE_FUNCTION
        if (is_end()) return;

        // 生成下一批结果
        generate_result_batch();
    }

    bool is_end() const override { return _is_end; }

    std::unique_ptr<BatchRecord> Next() override {
        TRACE_FUNCTION
        return std::move(result_batch_);
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    Rid &rid() override { return _abstract_rid; }

   private:
    void fetch_left_batch() {
        TRACE_FUNCTION
        if (left_->is_end()) {
            left_exhausted_ = true;
            left_batch_.reset();
            return;
        }
        left_batch_ = left_->Next();
        if (!left_batch_ || left_batch_->empty()) {
            left_exhausted_ = true;
            left_batch_.reset();
        } else {
            left_idx_ = 0;
        }
    }

    void fetch_right_batch() {
        TRACE_FUNCTION
        if (right_->is_end()) {
            right_exhausted_ = true;
            right_batch_.reset();
            return;
        }
        right_batch_ = right_->Next();
        if (!right_batch_ || right_batch_->empty()) {
            right_exhausted_ = true;
            right_batch_.reset();
        }
    }

    void reset_right_scan() {
        TRACE_FUNCTION
        right_->beginTuple();
        right_exhausted_ = false;
        fetch_right_batch();
    }

    void generate_result_batch() {
        TRACE_FUNCTION
        result_batch_ = std::make_unique<BatchRecord>();

        while (!left_exhausted_ && !result_batch_->full()) {
            if (!left_batch_ || left_idx_ >= left_batch_->size()) {
                left_->nextTuple();
                fetch_left_batch();
                if (left_exhausted_) {
                    break;
                }
                continue;
            }

            auto &left_rec = *(left_batch_->begin() + left_idx_);
            bool match_found = false;

            reset_right_scan();

            while (!right_exhausted_) {
                if (right_batch_) {
                    for (size_t i = 0; i < right_batch_->size(); ++i) {
                        auto &right_rec = *(right_batch_->begin() + i);
                        auto joined_rec = std::make_unique<RmRecord>(left_->tupleLen() + right_->tupleLen());
                        memcpy(joined_rec->data, left_rec->data, left_->tupleLen());
                        memcpy(joined_rec->data + left_->tupleLen(), right_rec->data, right_->tupleLen());

                        if (eval_conds(tot_cols_, fed_conds_, joined_rec)) {
                            match_found = true;
                            break;
                        }
                    }
                }
                if (match_found) {
                    break;
                }
                right_->nextTuple();
                fetch_right_batch();
            }

            if (match_found) {
                result_batch_->push_back(std::make_unique<RmRecord>(*left_rec));
            }

            left_idx_++;
        }

        if (left_exhausted_ && (!result_batch_ || result_batch_->empty())) {
            _is_end = true;
        }
    }

    std::string getType() override { return "NestedLoopSemiJoinExecutor"; }
};