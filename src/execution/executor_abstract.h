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
    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target,
                                                 bool cmp_agg_type = false) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name &&
                   (!cmp_agg_type || col.agg_type == target.agg_type);
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name + " at " + getType());
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
    bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds,
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
    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const std::unique_ptr<RmRecord> &rec) {
        TRACE_FUNCTION
        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        char *lhs_data = rec->data + lhs_col->offset;
        char *rhs_data;
        ColType rhs_type;
        int rhs_len = 0;
        Value rhs_expr_val;  // 用于存储表达式计算结果

        // 根据 rhs_type 获取右侧操作数信息
        switch (cond.rhs_type) {
            case ConditionRhsType::RHS_VALUE:
                rhs_data = cond.rhs_val.raw->data;
                rhs_type = cond.rhs_val.type;
                rhs_len = cond.rhs_val.raw->size;
                break;
            case ConditionRhsType::RHS_COLUMN: {
                auto rhs_col = get_col(rec_cols, cond.rhs_col);
                rhs_data = rec->data + rhs_col->offset;
                rhs_type = rhs_col->type;
                rhs_len = rhs_col->len;
                break;
            }
            case ConditionRhsType::RHS_EXPR:
                // 计算表达式的值
                // 注意：需要将 ArithExpr 包装在 ExprTerm 中传递
                rhs_expr_val = EvaluateExpr(ExprTerm(cond.rhs_expr), rec, rec_cols);
                // 检查计算结果的 raw 是否有效
                rhs_expr_val.raw.reset();  // 确保 raw 被正确初始化
                rhs_expr_val.init_raw();   // 初始化 raw 缓冲区
                rhs_data = rhs_expr_val.raw->data;
                rhs_type = rhs_expr_val.type;
                rhs_len = rhs_expr_val.raw->size;
                break;
            default:
                throw RMDBError("Unsupported ConditionRhsType");
        }

        // 类型应该一致
        bool is_numeric = is_numeric_type(lhs_col->type) && is_numeric_type(rhs_type);
        if (lhs_col->type != rhs_type && !is_numeric) {
            throw IncompatibleTypeError(coltype2str(lhs_col->type), coltype2str(rhs_type));
        }

        int cmp = 0;
        if (is_numeric) {
            Value lhs_val = Value::get_value(lhs_col->type, lhs_data);
            Value rhs_val = Value::get_value(rhs_type, rhs_data);
            // 整数比较
            if (lhs_col->type == ColType::TYPE_INT && rhs_type == ColType::TYPE_INT) {
                cmp = (lhs_val.int_val < rhs_val.int_val) ? -1 : (lhs_val.int_val > rhs_val.int_val) ? 1 : 0;
            } else {
                // 先转化成浮点数
                Value::convert(lhs_val, rhs_val);
                // 浮点数比较
                cmp = (lhs_val.float_val < rhs_val.float_val) ? -1 : (lhs_val.float_val > rhs_val.float_val) ? 1 : 0;
            }
        } else if (lhs_col->type == ColType::TYPE_STRING) {
            size_t len = std::max(lhs_col->len, rhs_len);
            cmp = strncmp(lhs_data, rhs_data, len);
        }

        switch (cond.op) {
            case CompOp::OP_EQ:
                return cmp == 0;
            case CompOp::OP_NE:
                return cmp != 0;
            case CompOp::OP_LT:
                return cmp < 0;
            case CompOp::OP_GT:
                return cmp > 0;
            case CompOp::OP_LE:
                return cmp <= 0;
            case CompOp::OP_GE:
                return cmp >= 0;
            default:
                throw InternalError("eval_cond::Unexpected op type at " + getType());
        }
    }

    /**
     * @brief 计算指定列的聚合函数值（优化版本）
     * @param rec_cols 记录的列元数据信息，用于查找目标列的类型和偏移量
     * @param rec 参与聚合计算的记录集合
     * @param tab_col 目标列信息（包含表名和列名）
     * @param agg_type 聚合函数类型（COUNT、SUM、MAX、MIN、NONE等）
     * @return 计算得到的聚合值
     */
    [[maybe_unused]] Value get_aggr_value(const std::vector<ColMeta> &rec_cols,
                                          const std::vector<std::unique_ptr<RmRecord>> &rec, const TabCol &tab_col,
                                          AggregateType agg_type) {
        TRACE_FUNCTION
        Value val;         // 存储最终的聚合结果
        ColMeta col_meta;  // 目标列的元数据信息

        // 特殊处理 COUNT(*) 情况
        if (agg_type == AggregateType::COUNT && tab_col.col_name == "*") {
            // 为 COUNT(*) 创建虚拟的列元数据，类型为整数
            col_meta = ColMeta{.tab_name = "", .name = "*", .type = ColType::TYPE_INT, .len = sizeof(int), .offset = 0};
        } else {
            // 从记录列信息中查找指定的目标列
            col_meta = *get_col(rec_cols, tab_col);
        }

        // 根据聚合类型赋值默认值
        switch (agg_type) {
            case AggregateType::NONE:
                // 非聚合函数，赋值第一行的值
                if (!rec.empty()) {
                    for (const auto &col : rec_cols) {
                        if (col.name == tab_col.col_name && col.tab_name == tab_col.tab_name) {
                            val.set_col_data(col.type, rec[0]->data + col.offset, col.len);
                            break;
                        }
                    }
                }
                break;
            case AggregateType::COUNT:
                val.set_int(0);
                break;
            case AggregateType::SUM:
                if (col_meta.type == ColType::TYPE_INT) {
                    val.set_int(0);
                } else if (col_meta.type == ColType::TYPE_FLOAT) {
                    val.set_float(0.0f);
                }
                break;
            case AggregateType::MIN:
                if (col_meta.type == ColType::TYPE_INT) {
                    val.set_int(std::numeric_limits<int>::max());
                } else if (col_meta.type == ColType::TYPE_FLOAT) {
                    val.set_float(std::numeric_limits<float>::max());
                }
                break;
            case AggregateType::MAX:
                if (col_meta.type == ColType::TYPE_INT) {
                    val.set_int(std::numeric_limits<int>::min());
                } else if (col_meta.type == ColType::TYPE_FLOAT) {
                    val.set_float(std::numeric_limits<float>::lowest());
                }
                break;
            case AggregateType::AVG:
                val.set_float(0.0f);
                break;
            default:
                throw InternalError("Unknown aggregate type at " + getType());
        }

        if (rec.empty()) {
            // 如果没有记录，直接返回默认值
            return val;
        }

        // 根据聚合函数类型进行不同的计算
        switch (agg_type) {
            case AggregateType::NONE:
                break;

            case AggregateType::COUNT:
                // COUNT 聚合：返回记录总数
                val.set_int(static_cast<int>(rec.size()));
                break;

            case AggregateType::SUM:
                // SUM 聚合：计算指定列所有值的总和
                switch (col_meta.type) {
                    case ColType::TYPE_INT: {
                        int sum = std::accumulate(rec.begin(), rec.end(), 0, [&col_meta](int acc, const auto &record) {
                            return acc + *reinterpret_cast<const int *>(record->data + col_meta.offset);
                        });
                        val.set_int(sum);
                        break;
                    }
                    case ColType::TYPE_FLOAT: {
                        float sum =
                            std::accumulate(rec.begin(), rec.end(), 0.0f, [&col_meta](float acc, const auto &record) {
                                return acc + *reinterpret_cast<const float *>(record->data + col_meta.offset);
                            });
                        val.set_float(sum);
                        break;
                    }
                    case ColType::TYPE_STRING:
                        throw AggregateError("Aggregate function SUM is not supported for string type column.");
                    default:
                        throw InternalError("Unsupported column type for SUM aggregation");
                }
                break;

            case AggregateType::MAX:
                // MAX 聚合：找出指定列的最大值
                switch (col_meta.type) {
                    case ColType::TYPE_INT: {
                        int max_val = *reinterpret_cast<const int *>(rec[0]->data + col_meta.offset);
                        for (size_t i = 1; i < rec.size(); ++i) {
                            max_val = std::max(max_val, *reinterpret_cast<const int *>(rec[i]->data + col_meta.offset));
                        }
                        val.set_int(max_val);
                        break;
                    }
                    case ColType::TYPE_FLOAT: {
                        float max_val = *reinterpret_cast<const float *>(rec[0]->data + col_meta.offset);
                        for (size_t i = 1; i < rec.size(); ++i) {
                            max_val =
                                std::max(max_val, *reinterpret_cast<const float *>(rec[i]->data + col_meta.offset));
                        }
                        val.set_float(max_val);
                        break;
                    }
                    case ColType::TYPE_STRING:
                        throw AggregateError("Aggregate function MAX is not supported for string type column.");
                    default:
                        throw InternalError("Unsupported column type for MAX aggregation");
                }
                break;

            case AggregateType::MIN:
                // MIN 聚合：找出指定列的最小值
                switch (col_meta.type) {
                    case ColType::TYPE_INT: {
                        int min_val = *reinterpret_cast<const int *>(rec[0]->data + col_meta.offset);
                        for (size_t i = 1; i < rec.size(); ++i) {
                            min_val = std::min(min_val, *reinterpret_cast<const int *>(rec[i]->data + col_meta.offset));
                        }
                        val.set_int(min_val);
                        break;
                    }
                    case ColType::TYPE_FLOAT: {
                        float min_val = *reinterpret_cast<const float *>(rec[0]->data + col_meta.offset);
                        for (size_t i = 1; i < rec.size(); ++i) {
                            min_val =
                                std::min(min_val, *reinterpret_cast<const float *>(rec[i]->data + col_meta.offset));
                        }
                        val.set_float(min_val);
                        break;
                    }
                    case ColType::TYPE_STRING:
                        throw AggregateError("Aggregate function MIN is not supported for string type column.");
                    default:
                        throw InternalError("Unsupported column type for MIN aggregation");
                }
                break;

            case AggregateType::AVG:
                // AVG 聚合：计算指定列所有值的平均值
                switch (col_meta.type) {
                    case ColType::TYPE_INT: {
                        int sum = std::accumulate(rec.begin(), rec.end(), 0, [&col_meta](int acc, const auto &record) {
                            return acc + *reinterpret_cast<const int *>(record->data + col_meta.offset);
                        });
                        val.set_float(static_cast<float>(sum) / static_cast<float>(rec.size()));
                        break;
                    }
                    case ColType::TYPE_FLOAT: {
                        float sum =
                            std::accumulate(rec.begin(), rec.end(), 0.0f, [&col_meta](float acc, const auto &record) {
                                return acc + *reinterpret_cast<const float *>(record->data + col_meta.offset);
                            });
                        val.set_float(sum / static_cast<float>(rec.size()));
                        break;
                    }
                    case ColType::TYPE_STRING:
                        throw AggregateError("Aggregate function AVG is not supported for string type column.");
                    default:
                        throw InternalError("Unsupported column type for AVG aggregation");
                }
                break;

            default:
                throw InternalError("Unknown aggregate type at " + getType());
        }
        return val;  // 返回计算得到的聚合值
    }

    /**
     * @brief 批量计算多个列的聚合函数值
     * @param rec_cols 记录的列元数据信息，用于查找目标列的类型和偏移量
     * @param rec 参与聚合计算的记录集合
     * @param ab_cols 目标列信息列表（包含表名和列名）
     * @param agg_types 聚合函数类型列表（COUNT、SUM、MAX、MIN、NONE等）
     * @return 计算得到的聚合值向量
     */
    std::vector<Value> get_aggr_values(const std::vector<ColMeta> &rec_cols,
                                       const std::vector<std::unique_ptr<RmRecord>> &rec,
                                       const std::vector<TabCol> &tab_cols,
                                       const std::vector<AggregateType> &agg_types) {
        TRACE_FUNCTION

        // 验证输入参数
        if (tab_cols.size() != agg_types.size()) {
            throw InternalError("Column list size does not match aggregate type list size");
        }

        std::vector<Value> vals(tab_cols.size());         // 存储计算得到的聚合值
        std::vector<ColMeta> col_metas(tab_cols.size());  // 存储目标列的元数据信息

        // 获取所有目标列的元数据信息
        for (size_t i = 0; i < tab_cols.size(); ++i) {
            const auto &tab_col = tab_cols[i];
            if (tab_col.col_name == "*" && agg_types[i] == AggregateType::COUNT) {
                // 特殊处理 COUNT(*) 情况
                col_metas[i] =
                    ColMeta{.tab_name = "", .name = "*", .type = ColType::TYPE_INT, .len = sizeof(int), .offset = 0};
            } else {
                // 从记录列信息中查找指定的目标列
                col_metas[i] = *get_col(rec_cols, tab_col);
            }
        }

        for (size_t i = 0; i < tab_cols.size(); ++i) {
            switch (agg_types[i]) {
                case AggregateType::NONE:
                    // 非聚合函数，赋值第一行的值
                    if (!rec.empty()) {
                        for (const auto &col : rec_cols) {
                            if (col.name == tab_cols[i].col_name && col.tab_name == tab_cols[i].tab_name) {
                                vals[i].set_col_data(col.type, rec[0]->data + col.offset, col.len);
                                break;
                            }
                        }
                    }
                    break;
                case AggregateType::COUNT:
                    vals[i].set_int(0);
                    break;
                case AggregateType::SUM:
                    if (col_metas[i].type == ColType::TYPE_INT) {
                        vals[i].set_int(0);
                    } else if (col_metas[i].type == ColType::TYPE_FLOAT) {
                        vals[i].set_float(0.0f);
                    }
                    break;
                case AggregateType::MIN:
                    if (col_metas[i].type == ColType::TYPE_INT) {
                        vals[i].set_int(std::numeric_limits<int>::max());
                    } else if (col_metas[i].type == ColType::TYPE_FLOAT) {
                        vals[i].set_float(std::numeric_limits<float>::max());
                    }
                    break;
                case AggregateType::MAX:
                    if (col_metas[i].type == ColType::TYPE_INT) {
                        vals[i].set_int(std::numeric_limits<int>::min());
                    } else if (col_metas[i].type == ColType::TYPE_FLOAT) {
                        vals[i].set_float(std::numeric_limits<float>::lowest());
                    }
                    break;
                case AggregateType::AVG:
                    vals[i].set_float(0.0f);
                    break;
                default:
                    break;
            }
        }

        if (rec.empty()) {
            // 如果没有记录，直接返回默认值
            return vals;
        }

        // 计算每个目标列的聚合值, 遍历records为外层循环以优化大表性能
        for (const auto &record : rec) {
            for (size_t i = 0; i < tab_cols.size(); i++) {
                switch (agg_types[i]) {
                    case AggregateType::NONE:
                        // 非聚合函数，已经在上面处理
                        break;
                    case AggregateType::COUNT:
                        // COUNT 聚合：返回记录总数
                        vals[i].set_int(static_cast<int>(rec.size()));
                        break;
                    case AggregateType::SUM:
                        // SUM 聚合：计算指定列所有值的总和
                        switch (col_metas[i].type) {
                            case ColType::TYPE_INT: {
                                int sum = vals[i].int_val +
                                          *reinterpret_cast<const int *>(record->data + col_metas[i].offset);
                                vals[i].set_int(sum);
                                break;
                            }
                            case ColType::TYPE_FLOAT: {
                                float sum = vals[i].float_val +
                                            *reinterpret_cast<const float *>(record->data + col_metas[i].offset);
                                vals[i].set_float(sum);
                                break;
                            }
                            case ColType::TYPE_STRING:
                                throw AggregateError("Aggregate function SUM is not supported for string type column.");
                        }
                        break;
                    case AggregateType::AVG:
                        switch (col_metas[i].type) {
                            case ColType::TYPE_INT: {
                                int sum = vals[i].int_val +
                                          *reinterpret_cast<const int *>(record->data + col_metas[i].offset);
                                vals[i].set_int(sum);
                                break;
                            }
                            case ColType::TYPE_FLOAT: {
                                float sum = vals[i].float_val +
                                            *reinterpret_cast<const float *>(record->data + col_metas[i].offset);
                                vals[i].set_float(sum);
                                break;
                            }
                            case ColType::TYPE_STRING:
                                throw AggregateError("Aggregate function AVG is not supported for string type column.");
                        }
                        break;
                    case AggregateType::MIN:
                        // MIN 聚合：找出指定列的最小值
                        switch (col_metas[i].type) {
                            case ColType::TYPE_INT: {
                                int min_val =
                                    std::min(vals[i].int_val,
                                             *reinterpret_cast<const int *>(record->data + col_metas[i].offset));
                                vals[i].set_int(min_val);
                                break;
                            }
                            case ColType::TYPE_FLOAT: {
                                float min_val =
                                    std::min(vals[i].float_val,
                                             *reinterpret_cast<const float *>(record->data + col_metas[i].offset));
                                vals[i].set_float(min_val);
                                break;
                            }
                            case ColType::TYPE_STRING:
                                throw AggregateError("Aggregate function MIN is not supported for string type column.");
                        }
                        break;
                    case AggregateType::MAX:
                        // MAX 聚合：找出指定列的最大值
                        switch (col_metas[i].type) {
                            case ColType::TYPE_INT: {
                                int max_val =
                                    std::max(vals[i].int_val,
                                             *reinterpret_cast<const int *>(record->data + col_metas[i].offset));
                                vals[i].set_int(max_val);
                                break;
                            }
                            case ColType::TYPE_FLOAT: {
                                float max_val =
                                    std::max(vals[i].float_val,
                                             *reinterpret_cast<const float *>(record->data + col_metas[i].offset));
                                vals[i].set_float(max_val);
                                break;
                            }
                            case ColType::TYPE_STRING:
                                throw AggregateError("Aggregate function MAX is not supported for string type column.");
                        }
                        break;
                    default:
                        throw InternalError("Unknown aggregate type at " + getType());
                }
            }
        }
        // 计算 AVG 聚合的最终值
        for (size_t i = 0; i < tab_cols.size(); ++i) {
            if (agg_types[i] == AggregateType::AVG) {
                if (col_metas[i].type == ColType::TYPE_INT) {
                    vals[i].set_float(static_cast<float>(vals[i].int_val) / static_cast<float>(rec.size()));
                } else if (col_metas[i].type == ColType::TYPE_FLOAT) {
                    vals[i].set_float(vals[i].float_val / static_cast<float>(rec.size()));
                }
            }
        }
        return vals;  // 返回计算得到的聚合值向量
    }
};