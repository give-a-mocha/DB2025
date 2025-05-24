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

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;   // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;  // 右儿子节点（需要join的表）
    size_t len_;                               // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                // join后获得的记录的字段

    std::vector<Condition> fed_conds_;  // join条件
    bool isend;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left,
                           std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);
    }

    void beginTuple() override {
        // Todo:
        // !需要自己实现
        left_->beginTuple();
        right_->beginTuple();
        if (left_->is_end() || right_->is_end()) {
            isend = true;
            return;
        }
        find_record();
    }

    void nextTuple() override {
        // Todo:
        // !需要自己实现
        if (is_end()) return;
        left_->nextTuple();
        if (left_->is_end()) {
            right_->nextTuple();
            left_->beginTuple();
        }
        find_record();
    }

    bool is_end() const override { return isend; }

    std::unique_ptr<RmRecord> Next() override {
        // Todo:
        // !需要自己实现
        auto left_rec = left_->Next();
        auto right_rec = right_->Next();

        if (!left_rec || !right_rec) {
            nextTuple();
        }

        auto join_rec = std::make_unique<RmRecord>(len_);
        memcpy(join_rec->data, left_rec->data, left_->tupleLen());
        memcpy(join_rec->data + left_->tupleLen(), right_rec->data,
               right_->tupleLen());
        return join_rec;
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    Rid &rid() override { return _abstract_rid; }

   private:
    void find_record() {
        while (!is_end()) {
            auto left_rec = left_->Next();
            auto right_rec = right_->Next();
            auto rec = std::make_unique<RmRecord>(len_);
            memcpy(rec->data, left_rec->data, left_->tupleLen());
            memcpy(rec->data + left_->tupleLen(), right_rec->data,
                    right_->tupleLen());
            if(eval_conds(cols_, fed_conds_, rec.get())) {
                return;
            }
            left_ -> nextTuple();
            if(left_->is_end()) {
                right_->nextTuple();
                left_->beginTuple();
            }
        }
        isend = true;
    }
};