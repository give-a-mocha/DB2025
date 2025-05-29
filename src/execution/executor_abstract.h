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

/**
 * @brief 执行器抽象基类
 *
 * AbstractExecutor是RMDB执行引擎的核心接口，实现了迭代器模型（Iterator Model）。
 * 所有具体的执行器都继承自这个基类，通过统一的接口提供数据访问能力。
 *
 * 迭代器模型的特点：
 * - 流水线处理：上层算子可以在下层算子产生数据时立即开始处理
 * - 内存效率：不需要缓存所有中间结果，按需生成数据
 * - 可组合性：不同算子可以灵活组合形成复杂的查询计划
 *
 * 执行器生命周期：
 * 1. 创建执行器对象
 * 2. 调用beginTuple()初始化并定位到第一条记录
 * 3. 循环调用nextTuple()和Next()获取后续记录
 * 4. 通过is_end()检查是否已处理完所有记录
 */
class AbstractExecutor {
   public:
    Rid _abstract_rid;             ///< 抽象记录标识符，某些执行器可能不使用
    Context *context_;             ///< 执行上下文，包含事务、缓冲区等信息

    virtual ~AbstractExecutor() = default;

    /**
     * @brief 获取每条记录的长度（字节数）
     * @return 记录长度，用于内存分配和数据拷贝
     */
    virtual size_t tupleLen() const { return 0; };

    /**
     * @brief 获取输出记录的列元数据信息
     * @return 列元数据向量，包含列名、类型、偏移量等信息
     */
    virtual const std::vector<ColMeta> &cols() const {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    /**
     * @brief 获取执行器类型标识
     * @return 执行器类型字符串，用于调试和错误诊断
     */
    virtual std::string getType() { return "AbstractExecutor"; };

    /**
     * @brief 初始化迭代器并定位到第一条满足条件的记录
     *
     * 这个方法执行必要的初始化工作，如打开文件、建立索引扫描等，
     * 并将内部状态设置为指向第一条有效记录。
     */
    virtual void beginTuple() {};

    /**
     * @brief 移动到下一条满足条件的记录
     *
     * 这个方法推进内部迭代器状态，跳过不满足条件的记录，
     * 直到找到下一条有效记录或到达末尾。
     */
    virtual void nextTuple() {};

    /**
     * @brief 检查是否已到达迭代器末尾
     * @return true表示没有更多记录，false表示还有记录可读
     */
    virtual bool is_end() const { return true; };

    /**
     * @brief 获取当前记录的RID（记录标识符）
     * @return 当前记录的RID，主要用于UPDATE和DELETE操作
     */
    virtual Rid &rid() = 0;

    /**
     * @brief 获取当前记录的数据
     * @return 指向当前记录数据的智能指针，如果无记录则返回nullptr
     */
    virtual std::unique_ptr<RmRecord> Next() = 0;

    /**
     * @brief 根据列标识获取列元数据（可选实现）
     * @param target 目标列标识
     * @return 对应的列元数据
     */
    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta(); };

    /**
     * @brief 在列元数据向量中查找指定的列
     *
     * 这是一个工具方法，用于在记录的列元数据中定位特定的列。
     * 查找依据是表名和列名的完全匹配。
     *
     * @param rec_cols 记录的列元数据向量
     * @param target 目标列标识，包含表名和列名
     * @return 指向匹配列元数据的迭代器
     * @throws ColumnNotFoundError 如果未找到指定列
     */
    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target, bool need_table_name = true) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return (col.tab_name == target.tab_name && col.name == target.col_name) || 
                   (!need_table_name && col.name == target.col_name);
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

    /**
     * @brief 判断是否为数值类型
     *
     * 数值类型包括整型和浮点型，这些类型之间可以进行自动转换和比较。
     * 此方法主要用于条件评估中的类型兼容性检查。
     *
     * @param type 列数据类型
     * @return true表示是数值类型，false表示非数值类型
     */
    bool is_numeric_type(ColType type) { return type == ColType::TYPE_INT || type == ColType::TYPE_FLOAT; }

    /**
     * @brief 从原始数据中提取Value对象
     *
     * 将存储在记录中的原始字节数据转换为Value对象，便于进行比较和运算。
     *
     * @param p 数据类型
     * @param a 指向原始数据的指针
     * @return 转换后的Value对象
     * @throws InternalError 对于不支持的字符串类型
     *
     * @note 字符串类型需要特殊处理，当前版本不支持在此函数中处理
     */
    Value get_value(ColType p, const char *a) {
        Value res;
        switch (p) {
            case ColType::TYPE_INT: {
                int ia = static_cast<int>(*reinterpret_cast<const int *>(a));
                res.set_int(ia);
                break;
            }

            case ColType::TYPE_FLOAT: {
                float fa = static_cast<float>(*reinterpret_cast<const float *>(a));
                res.set_float(fa);
                break;
            }

            case ColType::TYPE_STRING: {
                // 需要手动处理string类型的获取
                throw InternalError("get_value::Unexpected string value type at " + getType());
            }
        }
        return res;
    }

    /**
     * @brief 数值类型自动转换
     *
     * 当两个数值进行比较时，如果类型不同（一个是int，一个是float），
     * 则将int类型转换为float类型以进行统一比较。
     *
     * @param a 第一个数值（可能被转换）
     * @param b 第二个数值（可能被转换）
     *
     * @note 转换规则：int → float，保证比较的精度
     */
    void convert(Value &a, Value &b) {
        // 数值类型的转化(int, float)
        // int -> float
        if (a.type == b.type) return;
        if (b.type == ColType::TYPE_INT) {
            b.set_float(static_cast<float>(b.int_val));
            return;
        } else {
            a.set_float(static_cast<float>(a.int_val));
            return;
        }
    }

    /**
     * @brief 检查记录是否满足所有条件
     *
     * 这是条件评估的入口函数，用于检查一条记录是否同时满足所有给定的条件。
     * 条件之间是AND关系，只有当所有条件都满足时才返回true。
     *
     * @param rec_cols 记录的列元数据，用于定位列数据
     * @param conds 条件列表，每个条件包含操作符和操作数
     * @param rec 待检查的记录数据
     * @return true表示记录满足所有条件，false表示至少有一个条件不满足
     *
     * @note 采用短路求值：一旦发现不满足的条件立即返回false
     */
    bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds, const RmRecord *rec) {
        for (const auto &cond : conds) {
            if (!eval_cond(rec_cols, cond, rec)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief 检查记录是否满足单个条件
     *
     * 评估单个WHERE条件，支持各种比较操作符（=, <>, <, >, <=, >=）。
     * 能够处理列与常量的比较，以及列与列之间的比较。
     *
     * 处理流程：
     * 1. 提取左操作数（必须是列）
     * 2. 提取右操作数（可以是常量或列）
     * 3. 进行类型兼容性检查
     * 4. 根据数据类型选择比较方法
     * 5. 应用比较操作符得出结果
     *
     * @param rec_cols 记录的列元数据
     * @param cond 单个条件，包含左操作数、操作符、右操作数
     * @param rec 待检查的记录数据
     * @return true表示条件满足，false表示条件不满足
     *
     * @throws IncompatibleTypeError 当操作数类型不兼容时
     * @throws InternalError 当遇到未知操作符时
     */
    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const RmRecord *rec) {
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
        bool is_numeric = is_numeric_type(lhs_col->type) && is_numeric_type(rhs_type);
        if (lhs_col->type != rhs_type && !is_numeric) {
            throw IncompatibleTypeError(coltype2str(lhs_col->type), coltype2str(rhs_type));
        }

        int cmp;
        if (is_numeric) {
            Value lhs_val = get_value(lhs_col->type, lhs_data);
            Value rhs_val = get_value(rhs_type, rhs_data);
            // 整数比较
            if (lhs_col->type == ColType::TYPE_INT && rhs_type == ColType::TYPE_INT) {
                cmp = (lhs_val.int_val < rhs_val.int_val) ? -1 : (lhs_val.int_val > rhs_val.int_val) ? 1 : 0;
            } else {
                // 先转化成浮点数
                convert(lhs_val, rhs_val);
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

    Value get_aggr_value(
        const std::vector<ColMeta>& rec_cols,
        const std::vector<std::unique_ptr<RmRecord>>& rec,
        const TabCol& tab_col, AggregateType agg_type) {
        Value val;
        ColMeta col_meta;
        if (agg_type == AggregateType::AGG_COUNT && tab_col.col_name == "*") {
            col_meta = ColMeta{.tab_name = "",
                               .name = "*",
                               .type = ColType::TYPE_INT,
                               .len = sizeof(int),
                               .offset = 0};
        } else {
            col_meta = *get_col(rec_cols, tab_col);
        }
        if (agg_type == AggregateType::NONE) {
            for (auto& col_meta : rec_cols) {
                if (col_meta.name == tab_col.col_name) {
                    if (col_meta.type == ColType::TYPE_INT) {
                        val.set_int(*(int*)(rec[0]->data + col_meta.offset));
                    } else if (col_meta.type == ColType::TYPE_FLOAT) {
                        val.set_float(
                            *(float*)(rec[0]->data + col_meta.offset));
                    } else {
                        val.set_str(std::string(rec[0]->data + col_meta.offset,
                                                col_meta.len));
                    }
                    break;
                }
            }
        } else if (agg_type == AggregateType::AGG_COUNT) {
            val.set_int(rec.size());
        } else if (agg_type == AggregateType::AGG_SUM) {
            if (col_meta.type == ColType::TYPE_INT) {
                int sum = 0;
                for (const auto& record : rec) {
                    sum += *(int*)(record->data + col_meta.offset);
                }
                val.set_int(sum);
            } else if (col_meta.type == ColType::TYPE_FLOAT) {
                float sum = 0;
                for (const auto& record : rec) {
                    sum += *(float*)(record->data + col_meta.offset);
                }
                val.set_float(sum);
            }
        } else if (agg_type == AggregateType::AGG_MAX) {
            if (col_meta.type == ColType::TYPE_INT) {
                int max = std::numeric_limits<int>::min();
                for (const auto& record : rec) {
                    max =
                        std::max(max, *(int*)(record->data + col_meta.offset));
                }
                val.set_int(max);
            } else if (col_meta.type == ColType::TYPE_FLOAT) {
                float max = std::numeric_limits<float>::lowest();
                for (const auto& record : rec) {
                    max = std::max(max,
                                   *(float*)(record->data + col_meta.offset));
                }
                val.set_float(max);
            } else if (col_meta.type == ColType::TYPE_STRING) {
                std::string max = "";
                for (const auto& record : rec) {
                    std::string str(record->data + col_meta.offset,
                                    col_meta.len);
                    max = std::max(max, str);
                }
                val.set_str(max);
            }
        } else if (agg_type == AggregateType::AGG_MIN) {
            if (col_meta.type == ColType::TYPE_INT) {
                int min = std::numeric_limits<int>::max();
                for (const auto& record : rec) {
                    min =
                        std::min(min, *(int*)(record->data + col_meta.offset));
                }
                val.set_int(min);
            } else if (col_meta.type == ColType::TYPE_FLOAT) {
                float min = std::numeric_limits<float>::max();
                for (const auto& record : rec) {
                    min = std::min(min,
                                   *(float*)(record->data + col_meta.offset));
                }
                val.set_float(min);
            } else if (col_meta.type == ColType::TYPE_STRING) {
                std::string min = std::string(255, 255);
                for (const auto& record : rec) {
                    std::string str(record->data + col_meta.offset,
                                    col_meta.len);
                    min = std::min(min, str);
                }
                val.set_str(min);
            }
        }
        return val;
    }
    bool compare(Value lhs, Value rhs, CompOp op) {
        bool is_numeric = is_numeric_type(lhs.type) && is_numeric_type(rhs.type);
        if (lhs.type != rhs.type && !is_numeric) {
            throw IncompatibleTypeError(coltype2str(lhs.type), coltype2str(rhs.type));
        }
        int cmp;
        if (is_numeric) {
            // 整数比较
            if (lhs.type == ColType::TYPE_INT && rhs.type == ColType::TYPE_INT) {
                cmp = (lhs.int_val < rhs.int_val) ? -1 : (lhs.int_val > rhs.int_val) ? 1 : 0;
            } else {
                // 先转化成浮点数
                convert(lhs, rhs);
                // 浮点数比较
                cmp = (lhs.float_val < rhs.float_val) ? -1 : (lhs.float_val > rhs.float_val) ? 1 : 0;
            }
        } else if (lhs.type == ColType::TYPE_STRING) {
            size_t len = std::max(lhs.str_val.size(), rhs.str_val.size());
            cmp = strncmp(lhs.str_val.c_str(), rhs.str_val.c_str(), len);
        }
        switch (op) {
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
                throw InternalError("compare::Unexpected op type at " + getType());
        }
    }
};