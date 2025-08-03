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
#include <numeric>
#include <string>
#include <string_view>

#include "common/TraceStack.hpp"
#include "common/common.h"
#include "execution/execution_common.h"
#include "execution_defs.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 执行器抽象基类，定义查询执行引擎的核心接口
 */
class AbstractExecutor {
   public:
    /**
     * @brief 当前处理的记录ID
     */
    Rid _abstract_rid;

    TupleMeta _abstract_tuple_meta;

    /**
     * @brief 执行上下文
     */
    Context *context_;

    virtual ~AbstractExecutor() = default;

    /**
     * @brief 获取元组长度
     * @return 元组的长度(字节数)
     */
    virtual size_t tupleLen() const { return 0; };

    /**
     * @brief 获取执行器输出的列元数据
     * @return 列元数据的向量引用
     */
    virtual const std::vector<ColMeta> &cols() const {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    virtual std::string getType() { return "AbstractExecutor"; };

    /**
     * @brief 初始化元组遍历

     */
    virtual void beginTuple() {};

    /**
     * @brief 移动到下一个元组

     */
    virtual void nextTuple() {};

    /**
     * @brief 检查遍历是否结束
     * @return true表示遍历结束，false表示还有元组

     */
    virtual bool is_end() const { return true; };

    /**
     * @brief 获取当前元组的记录ID
     * @return 当前记录的RID引用

     */
    virtual Rid &rid() = 0;

    virtual TupleMeta &tuple_meta() { return _abstract_tuple_meta; }

    /**
     * @brief 获取下一个元组
     * @return 下一条记录
     * @throw ExecutionError 当获取失败时
     */
    virtual std::unique_ptr<RmRecord> Next() = 0;

    /**
     * @brief 获取列的偏移和元数据信息
     * @param target 目标列的表列引用
     * @return 列的完整元数据
     * @throw ColumnNotFoundError 当列不存在时
     */
    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta(); };

    /**
     * @brief 在列集合中定位指定列
     * @param rec_cols 记录的列集合
     * @param target 目标列引用
     * @return 列的迭代器位置
     * @throw ColumnNotFoundError 当列不存在时
     */
    static std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target,
                                                        bool cmp_agg_type = false) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name &&
                   ((!cmp_agg_type && col.agg_type == AggregateType::NONE) ||
                    (cmp_agg_type && col.agg_type == target.agg_type));
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name + " at StaticEval");
        }
        return pos;
    }

    /**
     * @brief 判断列类型是否为数值类型
     * @param type 要检查的列类型
     * @return 如果是INT或FLOAT类型返回true，否则返回false
     */
    static bool is_numeric_type(ColType type) { return type == ColType::TYPE_INT || type == ColType::TYPE_FLOAT; }

    /**
     * @brief 评估记录是否满足所有条件
     * @param rec_cols 记录的列元数据集合
     * @param conds 条件表达式列表
     * @param rec 待评估的记录
     * @return true表示满足所有条件
     * @throw ExecutionError 当评估过程出错时
     */
    static bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds,
                           const std::unique_ptr<RmRecord> &rec) {
        for (const auto &cond : conds) {
            if (!eval_cond(rec_cols, cond, rec)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief 评估单个条件表达式
     * @param rec_cols 记录的列元数据
     * @param cond 条件表达式
     * @param rec 待评估的记录
     * @return true表示满足条件
     * @throw IncompatibleTypeError 当类型不兼容时
     * @throw InternalError 当遇到非法操作符时
     */
    static bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond,
                          const std::unique_ptr<RmRecord> &rec) {
        TRACE_FUNCTION
        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        Value lhs_val;
        lhs_val.set_value_data(lhs_col->type, rec->data + lhs_col->offset, lhs_col->len);
        Value rhs_val;

        // 根据 rhs_type 获取右侧操作数信息
        switch (cond.rhs_type) {
            case ConditionRhsType::RHS_VALUE:
                rhs_val = cond.rhs_val;
                break;
            case ConditionRhsType::RHS_COLUMN: {
                auto rhs_col = get_col(rec_cols, cond.rhs_col);
                rhs_val.set_value_data(rhs_col->type, rec->data + rhs_col->offset, rhs_col->len);
                break;
            }
            case ConditionRhsType::RHS_EXPR:
                rhs_val = EvaluateExpr(ExprTerm(cond.rhs_expr), rec, rec_cols);
                break;
            default:
                throw RMDBError("Unsupported ConditionRhsType");
        }
        return Value::compare(lhs_val, rhs_val, cond.op);
    }
};