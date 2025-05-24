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

#include "common/common.h"
#include "execution_defs.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor {
   public:
    Rid _abstract_rid;

    Context *context_;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    virtual const std::vector<ColMeta> &cols() const {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    virtual std::string getType() { return "AbstractExecutor"; };

    virtual void beginTuple() {};

    virtual void nextTuple() {};

    virtual bool is_end() const { return true; };

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta(); };

    std::vector<ColMeta>::const_iterator get_col(
        const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(),
                                [&](const ColMeta &col) {
                                    return col.tab_name == target.tab_name &&
                                           col.name == target.col_name;
                                });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

    /**
     * @brief 检查记录是否满足所有条件
     */
    bool eval_conds(const std::vector<ColMeta> &rec_cols,
                    const std::vector<Condition> &conds, const RmRecord *rec) {
        // Todo:
        // !需要自己实现
        for (const auto &cond : conds) {
            if (!eval_cond(rec_cols, cond, rec)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief 检查记录是否满足单个条件
     */
    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond,
                   const RmRecord *rec) {
        // Todo:
        // !需要自己实现
        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        char *lhs_data = rec->data + lhs_col->offset;
        char *rhs_data;
        ColType rhs_type;
        int rhs_len = 0;

        if (cond.is_rhs_val) {
            rhs_data = cond.rhs_val.raw->data;
            rhs_type = cond.rhs_val.type;
            rhs_len = cond.rhs_val.raw->size;
        } else {
            auto rhs_col = get_col(rec_cols, cond.rhs_col);
            rhs_data = rec->data + rhs_col->offset;
            rhs_type = rhs_col->type;
            rhs_len = rhs_col->len;
        }

        // 类型应该一致
        if (lhs_col->type != rhs_type) {
            throw IncompatibleTypeError(coltype2str(lhs_col->type),
                                        coltype2str(rhs_type));
        }

        int cmp;
        if (lhs_col->type == TYPE_INT) {
            cmp = *(int *)lhs_data - *(int *)rhs_data;
        } else if (lhs_col->type == TYPE_FLOAT) {
            float diff = *(float *)lhs_data - *(float *)rhs_data;
            cmp = (diff > 0) ? 1 : (diff < 0) ? -1 : 0;
        } else if (lhs_col->type == TYPE_STRING) {
            // 使用两个字符串长度的最小值进行比较，避免越界访问
            int cmp_len = std::min(lhs_col->len, rhs_len);
            cmp = memcmp(lhs_data, rhs_data, cmp_len);
            // 如果前缀相同但长度不同，较短的字符串小于较长的字符串
            if (cmp == 0 && lhs_col->len != rhs_len) {
                cmp = lhs_col->len - rhs_len;
            }
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
                throw InternalError("eval_cond::Unexpected op type");
        }
    }
};