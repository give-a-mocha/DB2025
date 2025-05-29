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
#include "parser/ast.h"

#include "parser/parser.h"

/**
 * @brief 查询执行计划节点类型枚举
 *
 * 定义了RMDB系统中所有支持的计划节点类型，包括：
 * - DDL操作：数据定义语言操作
 * - DML操作：数据操作语言操作
 * - 执行算子：具体的查询执行操作符
 * - 事务控制：事务管理相关操作
 * - 系统操作：帮助、显示等辅助操作
 */
enum class PlanTag{
    T_Invalid = 1,           // 无效标签
    
    // 系统操作
    T_Help,                  // 显示帮助信息
    T_ShowTable,             // 显示所有表
    T_ShowIndex,             // 显示索引信息
    T_DescTable,             // 描述表结构
    T_SetKnob,               // 设置系统参数
    
    // DML操作（数据操作语言）
    T_Insert,                // 插入数据
    T_Update,                // 更新数据
    T_Delete,                // 删除数据
    T_select,                // 查询数据
    
    // DDL操作（数据定义语言）
    T_CreateTable,           // 创建表
    T_DropTable,             // 删除表
    T_CreateIndex,           // 创建索引
    T_DropIndex,             // 删除索引
    
    // 事务控制
    T_Transaction_begin,     // 开始事务
    T_Transaction_commit,    // 提交事务
    T_Transaction_abort,     // 中止事务
    T_Transaction_rollback,  // 回滚事务
    
    // 执行算子
    T_SeqScan,               // 顺序扫描
    T_IndexScan,             // 索引扫描
    T_NestLoop,              // 嵌套循环连接
    T_SortMerge,             // 排序合并连接
    T_Sort,                  // 排序操作
    T_Projection,            // 投影操作
    T_Aggregate,             // 聚合操作
    T_Group                  // 分组操作
};

/**
 * @brief 查询执行计划基类
 *
 * 所有查询执行计划节点的抽象基类。每个计划节点代表查询执行过程中的一个操作，
 * 如表扫描、连接、排序等。计划节点组织成树形结构，形成完整的查询执行计划。
 *
 * 设计特点：
 * - 使用虚析构函数支持多态
 * - 通过tag字段标识具体的计划类型
 * - 支持通过dynamic_pointer_cast进行类型转换
 */
class Plan{
public:
    PlanTag tag;             ///< 计划节点类型标识
    virtual ~Plan() = default;
};

/**
 * @brief 表扫描计划节点
 *
 * 表示对单个表的扫描操作，支持两种扫描方式：
 * - 顺序扫描（T_SeqScan）：逐行扫描整个表
 * - 索引扫描（T_IndexScan）：基于索引进行高效扫描
 *
 * 扫描计划包含了扫描过程中需要的所有信息，包括表元数据、
 * 过滤条件、索引信息等。这些信息与对应的ScanExecutor保持一致。
 */
class ScanPlan : public Plan{
public:
    std::string tab_name_;                     ///< 要扫描的表名
    std::vector<ColMeta> cols_;                ///< 表的列元数据信息
    std::vector<Condition> conds_;             ///< 扫描时应用的过滤条件
    size_t len_;                               ///< 每条记录的总长度（字节）
    std::vector<Condition> fed_conds_;         ///< 提供给执行器的条件（与conds_相同）
    std::vector<std::string> index_col_names_; ///< 用于索引扫描的索引列名列表
    
    ~ScanPlan(){}
    
    /**
     * @brief 构造扫描计划
     *
     * @param tag 计划类型（T_SeqScan或T_IndexScan）
     * @param sm_manager 系统管理器，用于获取表元数据
     * @param tab_name 表名
     * @param conds 扫描条件
     * @param index_col_names 索引列名（仅用于索引扫描）
     */
    ScanPlan(PlanTag tag, SmManager *sm_manager, std::string tab_name,
             std::vector<Condition> conds, std::vector<std::string> index_col_names){
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        
        // 从系统管理器获取表元数据
        TabMeta &tab = sm_manager->db_.get_table(tab_name_);
        cols_ = tab.cols;
        
        // 计算记录长度：最后一列的偏移量 + 最后一列的长度
        len_ = cols_.back().offset + cols_.back().len;
        
        fed_conds_ = conds_;
        index_col_names_ = index_col_names;
    }
};

/**
 * @brief 连接计划节点
 *
 * 表示两个关系（表或其他计划节点的结果）之间的连接操作。
 * 支持多种连接算法：
 * - 嵌套循环连接（T_NestLoop）：适用于小表连接或缺少索引的情况
 * - 排序合并连接（T_SortMerge）：适用于大表的等值连接
 *
 * 连接计划采用二叉树结构，左右子节点可以是表扫描计划或其他连接计划，
 * 从而支持多表连接的复杂查询。
 */
class JoinPlan : public Plan{
public:
    std::shared_ptr<Plan> left_;       ///< 左子计划节点
    std::shared_ptr<Plan> right_;      ///< 右子计划节点
    std::vector<Condition> conds_;     ///< 连接条件列表，支持多个连接谓词
    JoinType type;                     ///< 连接类型（当前仅支持内连接）

    ~JoinPlan(){}
    
    /**
     * @brief 构造连接计划
     *
     * @param tag 连接算法类型（T_NestLoop或T_SortMerge）
     * @param left 左子计划
     * @param right 右子计划
     * @param conds 连接条件，通常为等值条件如 table1.id = table2.foreign_id
     *
     * @note 当前实现默认使用内连接（INNER_JOIN），未来可扩展支持外连接
     */
    JoinPlan(PlanTag tag, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds){
        Plan::tag = tag;
        left_ = std::move(left);
        right_ = std::move(right);
        conds_ = std::move(conds);
        type = INNER_JOIN;  // 目前仅支持内连接，未来可扩展
    }
};

/**
 * @brief 投影计划节点
 *
 * 投影操作用于从输入记录中选择指定的列，实现SQL中的SELECT子句。
 * 投影可以：
 * - 选择部分列：SELECT name, age FROM ...
 * - 重新排列列的顺序
 * - 应用别名（在未来版本中支持）
 *
 * 投影通常位于查询计划树的顶层，作为最后一步处理。
 * 通过投影下推优化，也可以在计划树的其他位置应用投影以减少数据传输量。
 */
class ProjectionPlan : public Plan{
public:
    std::shared_ptr<Plan> subplan_;    ///< 子计划，提供需要投影的数据
    std::vector<TabCol> sel_cols_;     ///< 需要选择的列列表，按输出顺序排列
    
    ~ProjectionPlan(){}
    
    /**
     * @brief 构造投影计划
     *
     * @param tag 计划类型（应为T_Projection）
     * @param subplan 产生数据的子计划
     * @param sel_cols 要投影的列，格式为 {table_name, column_name}
     */
    ProjectionPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols){
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        sel_cols_ = std::move(sel_cols);
    }
};

/**
 * @brief 排序计划节点
 *
 * 实现SQL中的ORDER BY子句，对查询结果按指定列进行排序。
 * 支持：
 * - 多列排序：ORDER BY col1 ASC, col2 DESC
 * - 升序和降序排序
 * - 混合排序方向
 *
 * 排序算法可以是内存排序（对于小数据集）或外部排序（对于大数据集）。
 * 排序通常在查询计划的后期执行，但也可能为了支持排序合并连接而提前排序。
 */
class SortPlan : public Plan{
public:
    std::shared_ptr<Plan> subplan_;    ///< 提供待排序数据的子计划
    std::vector<TabCol> sel_col_;      ///< 排序列列表，按优先级顺序排列
    std::vector<bool> is_desc_;        ///< 每列的排序方向：true为降序，false为升序
    
    ~SortPlan(){}
    
    /**
     * @brief 构造排序计划
     *
     * @param tag 计划类型（应为T_Sort）
     * @param subplan 产生待排序数据的子计划
     * @param sel_col 排序列，按排序优先级排列
     * @param is_desc 每列的排序方向，与sel_col一一对应
     *
     * @pre sel_col.size() == is_desc.size()
     */
    SortPlan(PlanTag tag, std::shared_ptr<Plan> subplan,
             std::vector<TabCol> sel_col, std::vector<bool> is_desc){
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        assert(sel_col.size() == is_desc.size());  // 确保列数和方向数匹配
        sel_col_ = std::move(sel_col);
        is_desc_ = std::move(is_desc);
    }
};

/**
 * @brief 数据操作语言（DML）计划节点
 *
 * 处理所有的数据操作语句，包括：
 * - INSERT: 插入新记录，使用values_字段
 * - UPDATE: 更新现有记录，使用set_clauses_和conds_字段
 * - DELETE: 删除记录，使用conds_字段和subplan_
 * - SELECT: 查询记录，使用subplan_进行复杂查询处理
 *
 * DML计划是数据库操作的核心，负责实际的数据修改和查询执行。
 * 对于复杂的DML操作，可能包含子计划来定位或处理目标数据。
 */
class DMLPlan : public Plan{
public:
    std::shared_ptr<Plan> subplan_;        ///< 子计划，用于SELECT、DELETE、UPDATE的数据定位
    std::string tab_name_;                 ///< 目标表名
    std::vector<Value> values_;            ///< INSERT操作的插入值列表
    std::vector<Condition> conds_;         ///< WHERE条件，用于UPDATE和DELETE
    std::vector<SetClause> set_clauses_;   ///< UPDATE操作的SET子句列表
    
    ~DMLPlan(){}
    
    /**
     * @brief 构造DML计划
     *
     * @param tag DML操作类型（T_Insert, T_Update, T_Delete, T_select）
     * @param subplan 子计划，INSERT时为空，其他操作用于数据定位
     * @param tab_name 目标表名
     * @param values INSERT值列表，其他操作为空
     * @param conds WHERE条件，INSERT时为空
     * @param set_clauses UPDATE的SET子句，其他操作为空
     *
     * @note 不同的DML操作使用不同的参数组合：
     *       - INSERT: 使用tab_name_和values_
     *       - UPDATE: 使用subplan_, tab_name_, conds_和set_clauses_
     *       - DELETE: 使用subplan_, tab_name_和conds_
     *       - SELECT: 主要使用subplan_
     */
    DMLPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::string tab_name,
            std::vector<Value> values, std::vector<Condition> conds,
            std::vector<SetClause> set_clauses){
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        tab_name_ = std::move(tab_name);
        values_ = std::move(values);
        conds_ = std::move(conds);
        set_clauses_ = std::move(set_clauses);
    }
};

/**
 * @brief 数据定义语言（DDL）计划节点
 *
 * 处理所有的数据定义语句，包括：
 * - CREATE TABLE: 创建新表，使用tab_name_和cols_字段
 * - DROP TABLE: 删除表，使用tab_name_字段
 * - CREATE INDEX: 创建索引，使用tab_name_和tab_col_names_字段
 * - DROP INDEX: 删除索引，使用tab_name_和tab_col_names_字段
 *
 * DDL操作直接修改数据库的元数据结构，是数据库模式管理的核心组件。
 * 这些操作通常具有原子性，要么完全成功要么完全失败。
 */
class DDLPlan : public Plan{
public:
    std::string tab_name_;                 ///< 目标表名
    std::vector<std::string> tab_col_names_; ///< 列名列表，用于索引操作
    std::vector<ColDef> cols_;             ///< 列定义列表，用于CREATE TABLE
    
    ~DDLPlan(){}
    
    /**
     * @brief 构造DDL计划
     *
     * @param tag DDL操作类型（T_CreateTable, T_DropTable, T_CreateIndex, T_DropIndex）
     * @param tab_name 目标表名
     * @param col_names 列名列表，CREATE/DROP INDEX时使用
     * @param cols 列定义，CREATE TABLE时使用
     *
     * @note 不同的DDL操作使用不同的参数组合：
     *       - CREATE TABLE: 使用tab_name_和cols_
     *       - DROP TABLE: 仅使用tab_name_
     *       - CREATE INDEX: 使用tab_name_和tab_col_names_
     *       - DROP INDEX: 使用tab_name_和tab_col_names_
     */
    DDLPlan(PlanTag tag, std::string tab_name, std::vector<std::string> col_names, std::vector<ColDef> cols){
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
        cols_ = std::move(cols);
        tab_col_names_ = std::move(col_names);
    }
};

/**
 * @brief 其他操作计划节点
 *
 * 处理不属于DDL或DML的其他数据库操作，包括：
 * - HELP: 显示帮助信息
 * - SHOW TABLES: 显示所有表
 * - DESC TABLE: 描述表结构
 * - BEGIN: 开始事务
 * - COMMIT: 提交事务
 * - ABORT/ROLLBACK: 中止/回滚事务
 *
 * 这些操作主要用于数据库管理、信息查询和事务控制。
 */
class OtherPlan : public Plan{
public:
    std::string tab_name_;     ///< 表名，用于SHOW/DESC操作，事务操作时为空
    
    ~OtherPlan(){}
    
    /**
     * @brief 构造其他操作计划
     *
     * @param tag 操作类型（T_Help, T_ShowTable, T_DescTable, T_Transaction_*）
     * @param tab_name 表名，SHOW/DESC操作时使用，其他操作传空字符串
     */
    OtherPlan(PlanTag tag, std::string tab_name){
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
    }
};

/**
 * @brief 聚合计划节点
 *
 * 实现SQL中的聚合函数操作，如：
 * - COUNT(): 计数
 * - SUM(): 求和
 * - AVG(): 平均值
 * - MAX(): 最大值
 * - MIN(): 最小值
 *
 * 聚合操作可以应用于整个结果集（如SELECT COUNT(*) FROM table）
 * 或与GROUP BY结合使用（如SELECT dept, COUNT(*) FROM emp GROUP BY dept）。
 * 聚合计划通常在GROUP BY计划之后执行。
 */
class AggregatePlan : public Plan{
public:
    std::shared_ptr<Plan> subplan_;    ///< 提供待聚合数据的子计划
    std::vector<TabCol> sel_cols_;     ///< 选择的列，包含聚合函数信息
    
    /**
     * @brief 构造聚合计划
     *
     * @param tag 计划类型（应为T_Aggregate）
     * @param subplan 产生待聚合数据的子计划
     * @param sel_cols 包含聚合函数的选择列
     *
     * @note 聚合类型信息当前存储在sel_cols的TabCol.aggregate字段中
     */
    AggregatePlan(PlanTag tag, std::shared_ptr<Plan> subplan,
                    std::vector<TabCol> sel_cols){
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        sel_cols_ = std::move(sel_cols);
    }
    ~AggregatePlan(){}
};

/**
 * @brief 分组计划节点
 *
 * 实现SQL中的GROUP BY子句，将具有相同分组列值的记录聚集在一起。
 * 分组操作是聚合查询的基础，支持：
 * - 单列分组：GROUP BY department
 * - 多列分组：GROUP BY department, position
 * - HAVING条件：对分组后的结果进行过滤
 *
 * 分组计划通常与聚合计划配合使用，先分组再在每个组内进行聚合计算。
 */
class GroupPlan : public Plan{
public:
    std::shared_ptr<Plan> subplan_;        ///< 提供待分组数据的子计划
    std::vector<TabCol> sel_cols_;         ///< 选择的列（包括分组列和聚合列）
    std::vector<TabCol> group_cols_;       ///< 分组列列表
    std::vector<Condition> having_conds_;  ///< HAVING条件，对分组结果进行过滤
    
    /**
     * @brief 构造分组计划
     *
     * @param tag 计划类型（应为T_Group）
     * @param subplan 产生待分组数据的子计划
     * @param sel_cols 选择的列，包括分组列和聚合列
     * @param group_cols 分组依据的列
     * @param having_conds HAVING子句的过滤条件
     *
     * @note GROUP BY和HAVING的执行顺序：
     *       1. 按group_cols进行分组
     *       2. 在每组内计算聚合函数
     *       3. 应用having_conds过滤分组结果
     */
    GroupPlan(PlanTag tag, std::shared_ptr<Plan> subplan,
              std::vector<TabCol> sel_cols, std::vector<TabCol> group_cols,
              std::vector<Condition> having_conds){
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        sel_cols_ = std::move(sel_cols);
        group_cols_ = std::move(group_cols);
        having_conds_ = std::move(having_conds);
    }
    ~GroupPlan(){}
};

/**
 * @brief 系统参数设置计划节点
 *
 * 处理SET命令，用于动态调整数据库系统的运行参数。
 * 支持的参数类型包括：
 * - 连接算法开关：启用/禁用特定的连接算法
 * - 优化器选项：控制查询优化策略
 * - 调试选项：控制日志输出和调试信息
 *
 * 参数设置通常在会话级别生效，不会持久化到磁盘。
 */
class SetKnobPlan : public Plan{
public:
    ast::SetKnobType set_knob_type_;   ///< 要设置的参数类型
    bool bool_value_;                  ///< 参数值（当前仅支持布尔类型）
    
    /**
     * @brief 构造参数设置计划
     *
     * @param knob_type 参数类型，定义在ast::SetKnobType中
     * @param bool_value 布尔值参数
     */
    SetKnobPlan(ast::SetKnobType knob_type, bool bool_value) {
        Plan::tag = PlanTag::T_SetKnob;
        set_knob_type_ = knob_type;
        bool_value_ = bool_value;
    }
};

/**
 * @brief 查询计划构建信息类
 *
 * 存储查询计划构建过程中的中间信息，用于在计划构建的不同阶段之间传递数据。
 * 主要用于SELECT语句的复杂查询处理，包含了从语法分析到计划生成的各个环节的信息。
 *
 * 这个类充当了查询优化器内部的数据容器，帮助协调各个优化阶段的工作。
 */
class plannerInfo{
public:
    std::shared_ptr<ast::SelectStmt> parse;                 ///< 解析后的SELECT语句AST
    std::vector<Condition> where_conds;                     ///< WHERE子句条件列表
    std::vector<TabCol> sel_cols;                           ///< SELECT子句的列列表
    std::shared_ptr<Plan> plan;                             ///< 生成的查询计划
    std::vector<std::shared_ptr<Plan>> table_scan_executors; ///< 表扫描执行器列表
    std::vector<SetClause> set_clauses;                     ///< UPDATE语句的SET子句（如果适用）
    
    /**
     * @brief 构造计划构建信息对象
     *
     * @param parse_ SELECT语句的AST节点
     */
    plannerInfo(std::shared_ptr<ast::SelectStmt> parse_):parse(std::move(parse_)){}
};
