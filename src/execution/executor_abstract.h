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

#include "common/common.h"
#include "execution_defs.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 执行器抽象基类，定义查询执行引擎的核心接口
 *
 * 执行器架构设计：
 * 1. 火山模型接口
 *    - next(): 获取下一个元组
 *    - beginTuple(): 初始化扫描
 *    - is_end(): 检查是否结束
 *
 * 2. 数据访问接口
 *    - 元组遍历和定位
 *    - 列值提取和类型转换
 *    - 条件评估和过滤
 *
 * 3. 资源管理
 *    - 内存分配和释放
 *    - 缓冲区管理
 *    - 异常处理机制
 *
 * 4. 扩展设计
 *    - 支持流水线执行
 *    - 允许向量化处理
 *    - 便于添加新算子
 *
 * @note 具体执行器实现：
 * - SeqScan: 顺序扫描
 * - IndexScan: 索引扫描
 * - NestedLoop: 嵌套循环连接
 * - HashJoin: 哈希连接
 * - Sort: 排序
 * - Projection: 投影
 */
class AbstractExecutor {
   public:
    /**
     * @brief 当前处理的记录ID
     * @note 用于定位和访问具体记录
     */
    Rid _abstract_rid;

    /**
     * @brief 执行上下文
     * @note 包含：
     * - 事务信息
     * - 系统配置
     * - 执行状态
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
     * @note 子类实现必须：
     * 1. 重置内部状态
     * 2. 初始化扫描位置
     * 3. 准备第一个元组
     */
    virtual void beginTuple() {};

    /**
     * @brief 移动到下一个元组
     * @note 子类实现必须：
     * 1. 更新当前位置
     * 2. 维护内部状态
     * 3. 处理边界情况
     */
    virtual void nextTuple() {};

    /**
     * @brief 检查遍历是否结束
     * @return true表示遍历结束，false表示还有元组
     * @note 子类实现必须：
     * 1. 正确判断边界
     * 2. 考虑过滤条件
     * 3. 处理异常情况
     */
    virtual bool is_end() const { return true; };

    /**
     * @brief 获取当前元组的记录ID
     * @return 当前记录的RID引用
     * @note 子类实现必须：
     * 1. 维护有效的RID
     * 2. 支持随机访问
     * 3. 在遍历过程中更新
     */
    virtual Rid &rid() = 0;

    /**
     * @brief 获取下一个元组
     * @return 下一条记录
     * @throw ExecutionError 当获取失败时
     *
     * @note 火山模型的核心接口，子类实现必须：
     * 1. 返回一个有效记录
     * 2. 正确处理终止条件
     * 3. 维护内部迭代状态
     * 4. 处理所有错误情况
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
                                                        bool has_table = true) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return (!has_table || col.tab_name == target.tab_name) && col.name == target.col_name;
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
     * @brief 评估记录是否满足所有条件
     * @param rec_cols 记录的列元数据集合
     * @param conds 条件表达式列表
     * @param rec 待评估的记录
     * @return true表示满足所有条件
     * @throw ExecutionError 当评估过程出错时
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
     * @brief 评估单个条件表达式
     * @param rec_cols 记录的列元数据
     * @param cond 条件表达式
     * @param rec 待评估的记录
     * @return true表示满足条件
     * @throw IncompatibleTypeError 当类型不兼容时
     * @throw InternalError 当遇到非法操作符时
     *
     * @details 评估过程：
     * @note 支持的优化：
     * - 数值比较的快速路径
     * - 字符串比较的长度优化
     * - 类型转换的缓存机制
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

    /**
     * @brief 比较两个值的通用实现
     * @param lhs 左操作数
     * @param rhs 右操作数
     * @param op 比较操作类型
     * @return 比较结果
     * @throw IncompatibleTypeError 类型不兼容时
     * @note 优化考虑：
     * - 常见类型快速路径
     * - 大小写敏感性
     * - 特殊值处理
     */
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

    /**
     * @brief 计算指定列的聚合函数值
     * @param rec_cols 记录的列元数据信息，用于查找目标列的类型和偏移量
     * @param rec 参与聚合计算的记录集合
     * @param tab_col 目标列信息（包含表名和列名）
     * @param agg_type 聚合函数类型（COUNT、SUM、MAX、MIN、NONE等）
     * @return 计算得到的聚合值
     */
    static Value get_aggr_value(const std::vector<ColMeta> &rec_cols, const std::vector<std::unique_ptr<RmRecord>> &rec,
                                const TabCol &tab_col, AggregateType agg_type) {
        Value val;         // 存储最终的聚合结果
        ColMeta col_meta;  // 目标列的元数据信息

        // 特殊处理 COUNT(*) 情况
        if (agg_type == AggregateType::COUNT && tab_col.col_name == "*") {
            // 为 COUNT(*) 创建虚拟的列元数据，类型为整数
            col_meta = ColMeta{.tab_name = "", .name = "*", .type = ColType::TYPE_INT, .len = sizeof(int), .offset = 0};
        } else {
            // 从记录列信息中查找指定的目标列，不比较表名
            col_meta = *get_col(rec_cols, tab_col, false);
        }
        // 根据聚合函数类型进行不同的计算
        if (agg_type == AggregateType::NONE) {
            // 非聚合情况：直接返回第一条记录中该列的值
            for (auto &col_meta : rec_cols) {
                if (col_meta.name == tab_col.col_name) {
                    val.set_col_data(col_meta.type, rec[0]->data + col_meta.offset, col_meta.len);
                    break;
                }
            }
        } else if (agg_type == AggregateType::COUNT) {
            // COUNT 聚合：返回记录总数
            val.set_int(rec.size());
        } else if (agg_type == AggregateType::SUM) {
            // SUM 聚合：计算指定列所有值的总和
            if (col_meta.type == ColType::TYPE_INT) {
                // 整数求和
                int sum = std::accumulate(rec.begin(), rec.end(), 0, [&col_meta](int acc, const auto &record) {
                    return acc + *(int *)(record->data + col_meta.offset);
                });
                val.set_int(sum);
            } else if (col_meta.type == ColType::TYPE_FLOAT) {
                // 浮点数求和
                double sum = std::accumulate(rec.begin(), rec.end(), 0.0, [&col_meta](double acc, const auto &record) {
                    return acc + *(int *)(record->data + col_meta.offset);
                });
                val.set_float(sum);
            } else if (col_meta.type == ColType::TYPE_STRING) {
                throw AggregateError("Aggregate function SUM is not supported for string type column.");
            }
        } else if (agg_type == AggregateType::MAX) {
            // MAX 聚合：找出指定列的最大值
            if (col_meta.type == ColType::TYPE_INT) {
                // 整数最大值计算
                int max = std::numeric_limits<int>::min();  // 初始化为整数最小值
                for (const auto &record : rec) {
                    max = std::max(max, *(int *)(record->data + col_meta.offset));
                }
                val.set_int(max);
            } else if (col_meta.type == ColType::TYPE_FLOAT) {
                // 浮点数最大值计算
                double max = std::numeric_limits<double>::lowest();  // 初始化为浮点数最小值
                for (const auto &record : rec) {
                    max = std::max(max, *(double *)(record->data + col_meta.offset));
                }
                val.set_float(max);
            } else if (col_meta.type == ColType::TYPE_STRING) {
                // 字符串最大值计算（按字典序）
                std::string_view max = "";  // 初始化为空字符串（字典序最小）
                for (const auto &record : rec) {
                    std::string_view str(record->data + col_meta.offset, col_meta.len);
                    max = std::max(max, str);  // 字典序比较
                }
                val.set_str(std::string(max));
            }
        } else if (agg_type == AggregateType::MIN) {
            // MIN 聚合：找出指定列的最小值
            if (col_meta.type == ColType::TYPE_INT) {
                // 整数最小值计算
                int min = std::numeric_limits<int>::max();  // 初始化为整数最大值
                for (const auto &record : rec) {
                    min = std::min(min, *(int *)(record->data + col_meta.offset));
                }
                val.set_int(min);
            } else if (col_meta.type == ColType::TYPE_FLOAT) {
                // 浮点数最小值计算
                double min = std::numeric_limits<double>::max();  // 初始化为浮点数最大值
                for (const auto &record : rec) {
                    min = std::min(min, *(double *)(record->data + col_meta.offset));
                }
                val.set_float(min);
            } else if (col_meta.type == ColType::TYPE_STRING) {
                std::string_view min(rec.front()->data + col_meta.offset, col_meta.len);
                for (size_t i = 1; i < rec.size(); i++) {
                    const auto &record = rec[i];
                    std::string_view str(record->data + col_meta.offset, col_meta.len);
                    min = std::min(min, str);
                }
                val.set_str(std::string(min));
            } else if (agg_type == AggregateType::AVG) {
                // SUM 聚合：计算指定列所有值的总和
                if (col_meta.type == ColType::TYPE_INT) {
                    // 整数求和
                    int sum = std::accumulate(rec.begin(), rec.end(), 0, [&col_meta](int acc, const auto &record) {
                        return acc + *(int *)(record->data + col_meta.offset);
                    });
                    val.set_float(static_cast<double>(sum) / static_cast<double>(rec.size()));
                } else if (col_meta.type == ColType::TYPE_FLOAT) {
                    // 浮点数求和
                    double sum =
                        std::accumulate(rec.begin(), rec.end(), 0.0, [&col_meta](double acc, const auto &record) {
                            return acc + *(int *)(record->data + col_meta.offset);
                        });
                    val.set_float(static_cast<double>(sum) / static_cast<double>(rec.size()));
                } else if (col_meta.type == ColType::TYPE_STRING) {
                    throw AggregateError("Aggregate function AVG is not supported for string type column.");
                }
            }
        }
        return val;  // 返回计算得到的聚合值
    }
};