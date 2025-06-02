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
 * @brief 执行器抽象基类，定义查询执行器的通用接口
 *
 * 该类提供了执行器的基本功能和接口定义，包括：
 * - 元组遍历和访问
 * - 记录比较和条件评估
 * - 类型转换和数据访问
 *
 * 所有具体的执行器类(如SeqScan、IndexScan等)都继承自该类
 */
class AbstractExecutor {
   public:
    Rid _abstract_rid;

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
     * @brief 开始遍历元组
     * 初始化执行器状态，准备开始扫描元组
     */
    virtual void beginTuple() {};

    /**
     * @brief 移动到下一个元组
     * 更新执行器状态以访问下一个元组
     */
    virtual void nextTuple() {};

    /**
     * @brief 检查是否到达元组序列末尾
     * @return 如果没有更多元组返回true，否则返回false
     */
    virtual bool is_end() const { return true; };

    /**
     * @brief 获取当前元组的RID
     * @return 当前元组的RID引用
     */
    virtual Rid &rid() = 0;

    /**
     * @brief 获取下一个记录
     * @return 下一条记录的智能指针
     */
    virtual std::unique_ptr<RmRecord> Next() = 0;

    /**
     * @brief 获取指定列的元数据
     * @param target 目标列的表列引用
     * @return 列的元数据
     */
    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta(); };

    /**
     * @brief 在列集合中查找指定列
     *
     * 根据表名和列名在给定的列集合中查找匹配的列
     * 如果找不到指定的列，抛出ColumnNotFoundError异常
     *
     * @param rec_cols 要搜索的列集合
     * @param target 目标列的表列引用
     * @return 找到的列的迭代器
     * @throw ColumnNotFoundError 如果列不存在
     */
    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

    // 判断是否为数值类型
    /**
     * @brief 判断列类型是否为数值类型
     * @param type 要检查的列类型
     * @return 如果是INT或FLOAT类型返回true，否则返回false
     */
    bool is_numeric_type(ColType type) { return type == ColType::TYPE_INT || type == ColType::TYPE_FLOAT; }

    /**
     * @brief 从原始数据中提取值
     *
     * 根据列类型从内存中读取数据并转换为Value对象
     * 支持INT和FLOAT类型，不支持直接获取STRING类型
     *
     * @param p 值的类型
     * @param a 指向原始数据的指针
     * @return 转换后的Value对象
     * @throw InternalError 当尝试获取STRING类型时
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
     * @brief 在两个Value对象间进行类型转换
     *
     * 处理数值类型之间的转换：
     * - 如果类型相同，不进行转换
     * - INT转换为FLOAT时，将整数转换为对应的浮点数
     *
     * @param a 第一个Value对象(会被修改)
     * @param b 第二个Value对象(会被修改)
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
     * 遍历所有条件，检查记录是否满足每一个条件。
     * 所有条件都满足时返回true，任一条件不满足时返回false。
     *
     * @param rec_cols 记录的列元数据
     * @param conds 需要检查的条件列表
     * @param rec 要检查的记录
     * @return 如果记录满足所有条件返回true，否则返回false
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
     * 对指定的条件进行评估：
     * 1. 获取条件左右两边的值
     * 2. 进行类型检查和必要的类型转换
     * 3. 根据比较运算符进行值比较
     *
     * @param rec_cols 记录的列元数据
     * @param cond 要检查的条件
     * @param rec 要检查的记录
     * @return 如果记录满足条件返回true，否则返回false
     * @throw IncompatibleTypeError 当比较的类型不兼容时
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
};