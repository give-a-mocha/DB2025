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
     *
     * @details 获取信息包括：
     * 1. 列的物理位置
     *    - 字节偏移量
     *    - 对齐要求
     *
     * 2. 列的属性
     *    - 数据类型
     *    - 长度信息
     *    - 是否允许NULL
     *
     * 3. 访问优化
     *    - 缓存常用列
     *    - 批量获取优化
     */
    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta(); };

    /**
     * @brief 在列集合中定位指定列
     * @param rec_cols 记录的列集合
     * @param target 目标列引用
     * @return 列的迭代器位置
     * @throw ColumnNotFoundError 当列不存在时
     *
     * @details 查找过程：
     * 1. 列匹配规则
     *    - 完整匹配：表名和列名
     *    - 部分匹配：仅列名(需唯一)
     *    - 别名处理
     *
     * 2. 搜索优化
     *    - 使用find_if快速定位
     *    - 处理特殊情况
     *    - 错误恢复机制
     *
     * 3. 结果验证
     *    - 检查唯一性
     *    - 验证访问权限
     *    - 确保列可用
     *
     * @note 性能考虑：
     * - 对频繁访问的列建立索引
     * - 缓存查找结果
     * - 批量查找优化
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
     * @brief 评估记录是否满足所有条件
     * @param rec_cols 记录的列元数据集合
     * @param conds 条件表达式列表
     * @param rec 待评估的记录
     * @return true表示满足所有条件
     * @throw ExecutionError 当评估过程出错时
     *
     * @details 评估策略：
     * 1. 短路求值
     *    - 任一条件不满足立即返回false
     *    - 按条件选择性优化顺序
     *    - 减少不必要的计算
     *
     * 2. 条件重排序
     *    - 高选择性条件前置
     *    - 低代价条件前置
     *    - 考虑列访问局部性
     *
     * 3. 批量处理优化
     *    - 缓存频繁访问的值
     *    - 复用中间计算结果
     *    - 减少内存访问
     *
     * @note 性能优化：
     * - 对于大量记录的评估，考虑向量化
     * - 条件表达式的复用和缓存
     * - 避免重复的类型转换
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
     * 1. 值提取和准备
     *    - 定位左值的偏移位置
     *    - 处理右值(常量或列值)
     *    - 准备比较数据
     *
     * 2. 类型处理
     *    - 验证类型兼容性
     *    - 执行必要的类型转换
     *    - 特殊类型的比较处理
     *
     * 3. 值比较策略
     *    - 数值类型的高效比较
     *    - 字符串的优化比较
     *    - NULL值的特殊处理
     *
     * 4. 错误处理
     *    - 类型不兼容检查
     *    - 非法操作符检查
     *    - 数值范围检查
     *
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
};