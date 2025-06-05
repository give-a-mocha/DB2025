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

class NestedLoopSemiJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;   // 左子树执行器
    std::unique_ptr<AbstractExecutor> right_;  // 右子树执行器
    size_t len_;                               // 连接结果记录长度
    std::vector<ColMeta> cols_;                // 结果集列元数据
    std::vector<ColMeta> tot_cols_;  // 左表列元数据
    std::vector<Condition> fed_conds_;         // 连接条件列表
    bool _is_end;                              // 扫描结束标志

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
    }

    void beginTuple() override {
        left_->beginTuple();
        right_->beginTuple();
        //左表是空或者右表是空直接设置结束标记
        if (left_->is_end() || right_->is_end()) {
            _is_end = true;
            return;
        }
        find_record();
    }

    void nextTuple() override {
        if (is_end()) return;
        left_->nextTuple();
        right_->beginTuple();
        if (left_->is_end()) {
            _is_end = true;
            return;
        }
        find_record();
    }

    bool is_end() const override { return _is_end; }

    std::unique_ptr<RmRecord> Next() override {
        auto rec = left_->Next();
        // 检查空指针，记录为空则返回空指针
        if (!rec) {
            std::cerr << "Error: One of the records is null at " + getType() << std::endl;
            return nullptr;
        }
        return rec;
    }


    size_t tupleLen() const override { return len_; }


    const std::vector<ColMeta> &cols() const override { return cols_; }


    Rid &rid() override { return _abstract_rid; }

   private:
    
    void find_record() {
        while (!is_end()) {
            if (right_->is_end()) {
                left_->nextTuple();
                if (left_->is_end()) {
                    _is_end = true;
                    return;
                }
                right_->beginTuple();
            }

            auto left_rec = left_->Next();
            auto right_rec = right_->Next();
            if (!left_rec || !right_rec) {
                _is_end = true;
                return;
            }

            auto rec = std::make_unique<RmRecord>(left_->tupleLen() + right_->tupleLen());
            memcpy(rec->data, left_rec->data, left_->tupleLen());
            memcpy(rec->data + left_->tupleLen(), right_rec->data, right_->tupleLen());
            if (eval_conds(tot_cols_, fed_conds_, rec.get())) {
                return;
            }
            right_->nextTuple();
        }
        _is_end = true;
    }


    std::string getType() override { return "NestedLoopSemiJoinExecutor"; }
};