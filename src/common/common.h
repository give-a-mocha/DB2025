/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "defs.h"
#include "record/rm_defs.h"

/**
 * @brief 表列引用结构体，用于标识一个特定表中的列
 *
 * 该结构体存储表名、表别名和列名信息，用于在SQL查询处理中
 * 唯一标识和引用一个具体的列。
 */
struct TabCol {
    std::string tab_name;   // 表名
    std::string tab_alias;  // 表别名
    std::string col_name;   // 列名
    std::string col_alias;  // 列别名

    AggregateType agg_type{AggregateType::AGGREGATE_NONE};  // 聚合类型

    /**
     * @brief 默认构造函数，创建一个空的表列引用
     */
    TabCol() = default;

    /**
     * @brief 构造函数，根据表名、列名和可选的表别名创建表列引用
     *
     * @param tab_name_ 表名
     * @param col_name_ 列名
     * @param tab_alias_ 表别名(可选)
     */
    TabCol(std::string tab_name_, std::string col_name_, std::string tab_alias_ = "")
        : tab_name(std::move(tab_name_)), tab_alias(std::move(tab_alias_)), col_name(std::move(col_name_)) {}

    /**
     * @brief 小于运算符重载，用于在容器中排序
     *
     * @param x 左侧操作数
     * @param y 右侧操作数
     * @return 如果x小于y则返回true
     */
    friend bool operator<(const TabCol &x, const TabCol &y) {
        return std::tie(x.tab_name, x.col_name, x.tab_alias) < std::tie(y.tab_name, y.col_name, y.tab_alias);
    }

    /**
     * @brief 相等运算符重载，用于比较两个表列引用是否相同
     *
     * @param x 左侧操作数
     * @param y 右侧操作数
     * @return 如果x等于y则返回true
     */
    friend bool operator==(const TabCol &x, const TabCol &y) {
        return x.tab_name == y.tab_name && x.col_name == y.col_name && x.tab_alias == y.tab_alias;
    }

    /**
     * @brief 获取表示引用的表名，优先使用别名
     *
     * @return 表名或表别名(如果存在)
     */
    std::string get_tab_name() const { return tab_alias.empty() ? tab_name : tab_alias; }

    /**
     * @brief 获取表列引用的字符串表示
     *
     * @return 格式为"表名.列名"的字符串
     */
    std::string to_string() const { return get_tab_name() + "." + col_name; }
};

/**
 * @brief 表引用结构体，存储表名和可选的表别名
 *
 * 在SQL查询中用于跟踪表的引用方式，尤其是在FROM子句和JOIN操作中。
 */
struct TabRef {
    std::string name;   // 表名
    std::string alias;  // 表别名

    /**
     * @brief 构造函数，根据表名和可选的别名创建表引用
     *
     * @param name_ 表名
     * @param alias_ 表别名(可选)
     */
    TabRef(std::string name_, std::string alias_ = "") : name(std::move(name_)), alias(std::move(alias_)) {}

    /**
     * @brief 获取用于引用该表的名称，优先使用别名
     *
     * @return 表名或表别名(如果存在)
     */
    std::string get_name() const { return alias.empty() ? name : alias; }
};

/**
 * @brief 值对象结构体，用于存储不同类型的数据值
 *
 * 该结构体支持存储整型、浮点型和字符串类型的值，并提供原始数据缓冲区管理。
 * 用于SQL查询中的值表示，如WHERE子句中的常量值或INSERT语句中的插入值。
 */
struct Value {
    ColType type;  // 值的类型
    union {
        int int_val;      // 整数值
        float float_val;  // 浮点数值
    };
    std::string str_val;  // 字符串值

    std::shared_ptr<RmRecord> raw;  // 原始记录缓冲区

    /**
     * @brief 设置整数值
     *
     * @param int_val_ 要设置的整数值
     */
    void set_int(int int_val_) {
        type = ColType::TYPE_INT;
        int_val = int_val_;
    }

    /**
     * @brief 设置浮点数值
     *
     * @param float_val_ 要设置的浮点数值
     */
    void set_float(float float_val_) {
        type = ColType::TYPE_FLOAT;
        float_val = float_val_;
    }

    /**
     * @brief 设置字符串值
     *
     * @param str_val_ 要设置的字符串值
     */
    void set_str(std::string str_val_) {
        type = ColType::TYPE_STRING;
        str_val = std::move(str_val_);
    }

    /**
     * @brief 初始化原始数据缓冲区
     *
     * 根据当前值的类型，将值转换为原始字节格式存储在缓冲区中。
     * 对于字符串类型，会检查长度是否超出限制。
     *
     * @param len 缓冲区长度(字节)
     * @throw StringOverflowError 字符串长度超过指定长度时抛出
     */
    void init_raw(int len) {
        assert(raw == nullptr);
        raw = std::make_shared<RmRecord>(len);
        if (type == ColType::TYPE_INT) {
            assert(len == sizeof(int));
            *(int *)(raw->data) = int_val;  // 将整数值写入缓冲区
        } else if (type == ColType::TYPE_FLOAT) {
            assert(len == sizeof(float));
            *(float *)(raw->data) = float_val;  // 将浮点数值写入缓冲区
        } else if (type == ColType::TYPE_STRING) {
            if (len < (int)str_val.size()) {
                throw StringOverflowError();  // 字符串长度溢出异常
            }
            memset(raw->data, 0, len);                           // 初始化缓冲区为0
            memcpy(raw->data, str_val.c_str(), str_val.size());  // 复制字符串到缓冲区
        }
    }

    void init_raw(){
        assert(raw == nullptr);
        if (type == ColType::TYPE_INT) {
            raw = std::make_shared<RmRecord>(sizeof(int));
            *(int *)(raw->data) = int_val;  // 将整数值写入缓冲区
        } else if (type == ColType::TYPE_FLOAT) {
            raw = std::make_shared<RmRecord>(sizeof(float));
            *(float *)(raw->data) = float_val;  // 将浮点数值写入缓冲区
        } else if (type == ColType::TYPE_STRING) {
            raw = std::make_shared<RmRecord>(str_val.size() + 1);
            memset(raw->data, 0, str_val.size() + 1);  // 初始化缓冲区为0
            memcpy(raw->data, str_val.c_str(), str_val.size());  // 复制字符串到缓冲区
        }
    }
};

/**
 * @brief 比较操作符枚举，定义WHERE条件和JOIN条件中可用的比较操作
 */
enum class CompOp {
    OP_EQ,  // 等于
    OP_NE,  // 不等于
    OP_LT,  // 小于
    OP_GT,  // 大于
    OP_LE,  // 小于等于
    OP_GE   // 大于等于
};

/**
 * @brief JOIN连接类型枚举，支持不同的SQL JOIN操作
 */
enum class JoinType {
    INNER_JOIN,  // 内连接
    LEFT_JOIN,   // 左外连接
    RIGHT_JOIN,  // 右外连接
    FULL_JOIN,   // 全外连接
    CROSS_JOIN   // 交叉连接(笛卡尔积)
};

/**
 * @brief 条件表达式结构体，表示WHERE子句或JOIN条件中的一个条件
 *
 * 支持两种形式的条件：列与值的比较(如col = 10)和列与列的比较(如t1.col = t2.col)
 */
struct Condition {
    TabCol lhs_col;   // 左侧列引用
    CompOp op;        // 比较操作符
    bool is_rhs_val;  // 右侧是否为值(而非列)
    TabCol rhs_col;   // 右侧列引用(当is_rhs_val为false时有效)
    Value rhs_val;    // 右侧值(当is_rhs_val为true时有效)

    /**
     * @brief 将条件转换为字符串表示形式
     *
     * 用于调试和日志输出，格式为"左列 操作符 右侧值/右列"
     *
     * @return 条件的字符串表示
     */
    std::string to_string() const {
        // 将比较操作符转换为字符串
        auto compOp2String = [&](CompOp op) -> std::string {
            switch (op) {
                case CompOp::OP_EQ:
                    return "=";
                case CompOp::OP_NE:
                    return "!=";
                case CompOp::OP_GT:
                    return ">";
                case CompOp::OP_GE:
                    return ">=";
                case CompOp::OP_LT:
                    return "<";
                case CompOp::OP_LE:
                    return "<=";
                default:
                    throw InternalError("Unknown comparison operator");
            }
        };

        // 构造条件字符串
        std::string res = lhs_col.to_string();  // 左侧列名
        res += compOp2String(op);               // 添加操作符

        if (is_rhs_val) {
            // 如果右侧是值，根据类型添加相应格式的值
            if (rhs_val.type == ColType::TYPE_INT) {
                res += std::to_string(rhs_val.int_val);  // 整数值
            } else if (rhs_val.type == ColType::TYPE_FLOAT) {
                res += std::to_string(rhs_val.float_val);  // 浮点数值
            } else if (rhs_val.type == ColType::TYPE_STRING) {
                res += "'" + std::string(rhs_val.str_val) + "'";  // 带引号的字符串值
            } else {
                throw InternalError("Unknown value type in condition");  // 未知类型值
            }
        } else {
            // 如果右侧是列，添加列名
            res += rhs_col.to_string();
        }
        return res;
    }
};

struct JoinNode {
    std::string tab_name;
    std::vector<Condition> join_conds;  // JOIN条件
    JoinType join_type;                 // JOIN类型

    JoinNode(std::string name_, std::vector<Condition> join_conds_, JoinType join_type_)
        : tab_name(name_), join_conds(std::move(join_conds_)), join_type(join_type_) {}
};

struct SetClause {
    TabCol lhs;
    Value rhs;
};

inline CompOp swap_op(CompOp op) {
    switch (op) {
        case CompOp::OP_EQ:
            return CompOp::OP_EQ;
        case CompOp::OP_NE:
            return CompOp::OP_NE;
        case CompOp::OP_LT:
            return CompOp::OP_GT;
        case CompOp::OP_GT:
            return CompOp::OP_LT;
        case CompOp::OP_LE:
            return CompOp::OP_GE;
        case CompOp::OP_GE:
            return CompOp::OP_LE;
        default:
            throw RMDBError("Unknown comparison operator");
    }
}
