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
#include <string>
#include <string_view>

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

        // 判断是否为数值类型
    bool is_numeric_type(ColType type) {
        return type == TYPE_INT || type == TYPE_FLOAT;
    }

    Value get_value(ColType p, const char* a) {
        Value res;
        switch (p) {
            case TYPE_INT: {
                int ia = static_cast<int>(*reinterpret_cast<const int *>(a));
                res.set_int(ia);
                break;
            }
            case TYPE_FLOAT: {
                float fa = static_cast<float>(*reinterpret_cast<const float *>(a));
                res.set_float(fa);
                break;
            }
            case TYPE_STRING:
                std::string str = a;
                res.set_str(str);
                break;
        }
        return res;
    }

    void convert(Value& a, Value& b) {
        // 数值类型的转化(int, float)
        // int -> float
        if (a.type == b.type)
            return;
        if (a.type == TYPE_FLOAT) {
            if (b.type == TYPE_INT) {
                b.set_float(static_cast<float>(b.int_val));
                return;
            }
        } else if (a.type == TYPE_INT) {
            if (b.type == TYPE_FLOAT) {
                a.set_float(static_cast<float>(a.int_val));
                return;
            }
        }
        throw InternalError("convert::Unexpected value type");
    }

    /**
     * @brief 检查记录是否满足所有条件
     */
    bool eval_conds(const std::vector<ColMeta> &rec_cols,
                    const std::vector<Condition> &conds, const RmRecord *rec) {
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
        bool is_numeric = is_numeric_type(lhs_col->type) &&
                          is_numeric_type(rhs_type);
        if (lhs_col->type != rhs_type && !is_numeric) {
            throw IncompatibleTypeError(coltype2str(lhs_col->type),
                                        coltype2str(rhs_type));
        }

        int cmp;
        if (is_numeric){
            Value lhs_val = get_value(lhs_col->type, lhs_data);
            Value rhs_val = get_value(rhs_type, rhs_data);
            std::cerr << "lhs_val: " << ((lhs_val.type == TYPE_INT) ? std::to_string(lhs_val.int_val) : std::to_string(lhs_val.float_val))
                      << ", rhs_val: " << ((rhs_val.type == TYPE_INT) ? std::to_string(rhs_val.int_val) : std::to_string(rhs_val.float_val)) << std::endl;
            // 整数比较
            if (lhs_col->type == TYPE_INT && rhs_type == TYPE_INT) {
                cmp = (lhs_val.int_val < rhs_val.int_val) ? -1
                      : (lhs_val.int_val > rhs_val.int_val) ? 1 : 0;
            }
            else{
                // 先转化成浮点数
                convert(lhs_val, rhs_val);
                // 浮点数比较
                cmp = (lhs_val.float_val < rhs_val.float_val) ? -1
                      : (lhs_val.float_val > rhs_val.float_val) ? 1 : 0;
            }
        } else if (lhs_col->type == TYPE_STRING) {
            // 使用 string_view 进行更安全的字符串比较
            // 先找到实际的字符串长度（去除尾部空字符）
            size_t lhs_actual_len = std::min(static_cast<size_t>(lhs_col->len),
                                             std::strlen(lhs_data));
            size_t rhs_actual_len =
                std::min(static_cast<size_t>(rhs_len), std::strlen(rhs_data));

            // 创建 string_view 进行比较，避免不必要的拷贝
            std::string_view lhs_view(lhs_data, lhs_actual_len);
            std::string_view rhs_view(rhs_data, rhs_actual_len);

            std::cerr << "lhs_view: " << lhs_view << ", rhs_view: " << rhs_view << std::endl;
            // 使用 string_view 比较
            if (lhs_view < rhs_view) {
                cmp = -1;
            } else if (lhs_view > rhs_view) {
                cmp = 1;
            } else {
                cmp = 0;
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