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
        if (!left_->is_end()) {
            right_->beginTuple();
            isend = false;
        } else {
            isend = true;
        }
    }

    void nextTuple() override {
        // Todo:
        // !需要自己实现
        if (isend) return;

        right_->nextTuple();
        if (right_->is_end()) {
            left_->nextTuple();
            if (left_->is_end()) {
                isend = true;
                return;
            }
            right_->beginTuple();
        }
    }

    bool is_end() const override { return isend; }

    std::unique_ptr<RmRecord> Next() override {
        // Todo:
        // !需要自己实现
        while (!isend) {
            auto left_rec = left_->Next();
            auto right_rec = right_->Next();

            if (!left_rec || !right_rec) {
                nextTuple();
                continue;
            }

            // 检查连接条件
            if (eval_join_conds(left_rec.get(), right_rec.get())) {
                // 合并左右记录
                auto join_rec = std::make_unique<RmRecord>(len_);
                memcpy(join_rec->data, left_rec->data, left_->tupleLen());
                memcpy(join_rec->data + left_->tupleLen(), right_rec->data,
                       right_->tupleLen());
                return join_rec;
            }

            nextTuple();
        }
        return nullptr;
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    Rid &rid() override { return _abstract_rid; }

   private:
    /**
     * @brief 检查连接条件
     */
    bool eval_join_conds(const RmRecord *left_rec, const RmRecord *right_rec) {
        // Todo:
        // !需要自己实现
        for (const auto &cond : fed_conds_) {
            if (!eval_join_cond(left_rec, right_rec, cond)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief 检查单个连接条件
     */
    bool eval_join_cond(const RmRecord *left_rec, const RmRecord *right_rec,
                        const Condition &cond) {
        // Todo:
        // !需要自己实现
        auto &left_cols = left_->cols();
        auto &right_cols = right_->cols();

        char *lhs_data, *rhs_data;
        ColType lhs_type, rhs_type;

        // 获取左操作数
        auto lhs_col_it = std::find_if(
            left_cols.begin(), left_cols.end(), [&](const ColMeta &col) {
                return col.tab_name == cond.lhs_col.tab_name &&
                       col.name == cond.lhs_col.col_name;
            });

        if (lhs_col_it != left_cols.end()) {
            lhs_data = left_rec->data + lhs_col_it->offset;
            lhs_type = lhs_col_it->type;
        } else {
            auto lhs_col_it_right = std::find_if(
                right_cols.begin(), right_cols.end(), [&](const ColMeta &col) {
                    return col.tab_name == cond.lhs_col.tab_name &&
                           col.name == cond.lhs_col.col_name;
                });
            if (lhs_col_it_right != right_cols.end()) {
                lhs_data = right_rec->data +
                           (lhs_col_it_right->offset - left_->tupleLen());
                lhs_type = lhs_col_it_right->type;
            } else {
                return false;
            }
        }

        // 获取右操作数
        if (cond.is_rhs_val) {
            rhs_data = cond.rhs_val.raw->data;
            rhs_type = cond.rhs_val.type;
        } else {
            auto rhs_col_it = std::find_if(
                left_cols.begin(), left_cols.end(), [&](const ColMeta &col) {
                    return col.tab_name == cond.rhs_col.tab_name &&
                           col.name == cond.rhs_col.col_name;
                });

            if (rhs_col_it != left_cols.end()) {
                rhs_data = left_rec->data + rhs_col_it->offset;
                rhs_type = rhs_col_it->type;
            } else {
                auto rhs_col_it_right = std::find_if(
                    right_cols.begin(), right_cols.end(),
                    [&](const ColMeta &col) {
                        return col.tab_name == cond.rhs_col.tab_name &&
                               col.name == cond.rhs_col.col_name;
                    });
                if (rhs_col_it_right != right_cols.end()) {
                    rhs_data = right_rec->data +
                               (rhs_col_it_right->offset - left_->tupleLen());
                    rhs_type = rhs_col_it_right->type;
                } else {
                    return false;
                }
            }
        }

        // 类型检查
        if (lhs_type != rhs_type) {
            throw IncompatibleTypeError(coltype2str(lhs_type),
                                        coltype2str(rhs_type));
        }

        int cmp;
        if (lhs_type == TYPE_INT) {
            cmp = *(int *)lhs_data - *(int *)rhs_data;
        } else if (lhs_type == TYPE_FLOAT) {
            float diff = *(float *)lhs_data - *(float *)rhs_data;
            cmp = (diff > 0) ? 1 : (diff < 0) ? -1 : 0;
        } else if (lhs_type == TYPE_STRING) {
            cmp = memcmp(lhs_data, rhs_data,
                         std::min(lhs_col_it->len,
                                  static_cast<int>(cond.rhs_val.raw->size)));
        }

        switch (cond.op) {
            case OP_EQ:
                return cmp == 0;
            case OP_NE:
                return cmp != 0;
            case OP_LT:
                return cmp < 0;
            case OP_GT:
                return cmp > 0;
            case OP_LE:
                return cmp <= 0;
            case OP_GE:
                return cmp >= 0;
            default:
                return false;
        }
    }
};