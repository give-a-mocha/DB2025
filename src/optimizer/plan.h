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

enum class PlanTag {
    T_Invalid = 1,
    T_Help,
    T_ShowTable,
    T_ShowIndex,
    T_DescTable,
    T_CreateTable,
    T_DropTable,
    T_CreateIndex,
    T_DropIndex,
    T_SetKnob,
    T_Insert,
    T_Update,
    T_Delete,
    T_select,
    T_explain,
    T_Transaction_begin,
    T_Transaction_commit,
    T_Transaction_abort,
    T_Transaction_rollback,
    T_SeqScan,
    T_IndexScan,
    T_NestLoop,
    T_SortMerge,  // sort merge join
    T_Sort,
    T_Projection,
    T_Group,
    T_Aggregate,
};

// 查询执行计划
class Plan {
   public:
    PlanTag tag;
    Plan() = default;
    Plan(PlanTag tag_) : tag(tag_) {}
    virtual ~Plan() = default;
};

/**
 * @brief 表扫描计划类
 *
 * ScanPlan 类表示一个数据表的扫描计划，是优化器生成的执行计划树的叶节点。
 * 该类支持两种扫描方式：顺序扫描(SeqScan)和索引扫描(IndexScan)，
 * 通过传入的 PlanTag 区分具体扫描类型。
 */
class ScanPlan : public Plan {
   public:
    std::string tab_name_;                      ///< 要扫描的表名
    std::vector<ColMeta> cols_;                 ///< 表中所有列的元数据
    std::vector<Condition> conds_;              ///< 过滤条件列表
    size_t len_;                                ///< 表中一条记录的长度(字节)
    std::vector<Condition> fed_conds_;          ///< 传递给执行器的过滤条件
    std::vector<std::string> index_col_names_;  ///< 索引扫描时使用的列名列表

    /**
     * @brief 析构函数
     */
    ~ScanPlan() = default;

    /**
     * @brief 构造函数
     *
     * @param tag 计划类型标签，区分顺序扫描(T_SeqScan)或索引扫描(T_IndexScan)
     * @param sm_manager 存储管理器指针，用于获取表的元数据
     * @param tab_name 要扫描的表名
     * @param conds 过滤条件列表
     * @param index_col_names 索引扫描使用的列名列表(索引扫描时有效)
     *
     * 构造函数初始化表的元数据信息，设置过滤条件，并计算记录长度。
     * 对于索引扫描，还需指定用于扫描的索引列名。
     */
    ScanPlan(PlanTag tag, SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
             std::vector<std::string> index_col_names)
        : Plan(tag),
          tab_name_(std::move(tab_name)),
          cols_(sm_manager->db_.get_table(tab_name_).cols),
          conds_(std::move(conds)),
          len_(cols_.back().offset + cols_.back().len),
          fed_conds_(conds_),  // Keep copy as in original logic
          index_col_names_(std::move(index_col_names)) {}
};

class JoinPlan : public Plan {
   public:
    // 左节点
    std::shared_ptr<Plan> left_;
    // 右节点
    std::shared_ptr<Plan> right_;
    // 连接条件
    std::vector<Condition> conds_;
    // future TODO: 后续可以支持的连接类型
    JoinType type;

    ~JoinPlan() {}
    JoinPlan(PlanTag tag, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds) {
        Plan::tag = tag;
        left_ = std::move(left);
        right_ = std::move(right);
        conds_ = std::move(conds);
        type = JoinType::INNER_JOIN;
    }
    JoinPlan(PlanTag tag, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds,
             JoinType type) {
        Plan::tag = tag;
        left_ = std::move(left);
        right_ = std::move(right);
        conds_ = std::move(conds);
        this->type = type;
    }
};

/**
 * @brief 投影计划类
 *
 * ProjectionPlan 类表示SQL查询中的投影操作计划，负责从子计划产生的结果中
 * 选择指定的列输出给用户。在查询执行树中通常位于顶层，用于控制最终返回的结果列。
 */
class ProjectionPlan : public Plan {
   public:
    std::shared_ptr<Plan> subplan_;
    std::vector<TabCol> sel_cols_;
    bool isStar_ = false;  // 是否是*投影
    ~ProjectionPlan() = default;
    ProjectionPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols, bool isStar = false)
        : Plan(tag), subplan_(std::move(subplan)), sel_cols_(std::move(sel_cols)), isStar_(isStar) {}
};

class AggregatePlan : public Plan {
   public:
    std::shared_ptr<Plan> subplan_;
    std::vector<TabCol> sel_cols_;
    std::vector<AggregateType> agg_types_;
    AggregatePlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols,
                  std::vector<AggregateType> agg_types)
        : Plan(tag), subplan_(std::move(subplan)), sel_cols_(std::move(sel_cols)), agg_types_(std::move(agg_types)) {}
    ~AggregatePlan() = default;
};

class GroupPlan : public Plan {
   public:
    std::shared_ptr<Plan> subplan_;
    std::vector<TabCol> sel_cols_;
    std::vector<TabCol> group_cols_;
    std::vector<Condition> having_conds_;

    GroupPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols, std::vector<TabCol> group_cols,
              std::vector<Condition> having_conds)
        : Plan(tag),
          subplan_(std::move(subplan)),
          sel_cols_(std::move(sel_cols)),
          group_cols_(std::move(group_cols)),
          having_conds_(std::move(having_conds)) {}
    ~GroupPlan() = default;
};

/**
 * @brief 排序计划节点类
 *
 * 实现ORDER BY子句的排序功能:
 * - 支持升序和降序排序
 * - 基于指定列进行排序
 * - 作为其他操作的输入提供有序数据
 */
class SortPlan : public Plan {
   public:
    std::shared_ptr<Plan> subplan_;  ///< 子计划
    std::vector<TabCol> sel_cols_;   ///< 用于排序的列
    std::vector<bool> is_desc_;      ///< 是否为降序排序

    ~SortPlan() = default;

    /**
     * @brief 构造排序计划节点
     * @param tag 计划类型(T_Sort)
     * @param subplan 子计划
     * @param sel_cols 排序列
     * @param is_desc 是否降序
     */
    SortPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols, std::vector<bool> is_desc)
        : Plan(tag), subplan_(std::move(subplan)), sel_cols_(std::move(sel_cols)), is_desc_(std::move(is_desc)) {}
};

/**
 * @brief DML操作计划节点类
 *
 * 处理数据操作语言(DML)语句，包括：
 * - INSERT: 插入数据
 * - DELETE: 删除数据
 * - UPDATE: 更新数据
 * - SELECT: 查询数据
 */
class DMLPlan : public Plan {
   public:
    std::shared_ptr<Plan> subplan_;       ///< 子计划(用于SELECT或复杂查询)
    std::string tab_name_;                ///< 目标表名
    std::vector<Value> values_;           ///< 插入的值列表(INSERT使用)
    std::vector<Condition> conds_;        ///< 条件列表(WHERE子句)
    std::vector<SetClause> set_clauses_;  ///< 更新子句列表(UPDATE使用)

    ~DMLPlan() {}

    /**
     * @brief 构造DML计划节点
     * @param tag 计划类型(T_Insert/T_Delete/T_Update/T_select)
     * @param subplan 子计划
     * @param tab_name 目标表名
     * @param values 插入值列表
     * @param conds 条件列表
     * @param set_clauses 更新子句列表
     */
    DMLPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::string tab_name, std::vector<Value> values,
            std::vector<Condition> conds, std::vector<SetClause> set_clauses)
        : Plan(tag),
          subplan_(std::move(subplan)),
          tab_name_(std::move(tab_name)),
          values_(std::move(values)),
          conds_(std::move(conds)),
          set_clauses_(std::move(set_clauses)) {}
};

/**
 * @brief DDL操作计划节点类
 *
 * 处理数据定义语言(DDL)语句，包括：
 * - CREATE TABLE: 创建表
 * - DROP TABLE: 删除表
 * - CREATE INDEX: 创建索引
 * - DROP INDEX: 删除索引
 */
class DDLPlan : public Plan {
   public:
    std::string tab_name_;                    ///< 目标表名
    std::vector<std::string> tab_col_names_;  ///< 列名列表(用于索引操作)
    std::vector<ColDef> cols_;                ///< 列定义列表(用于建表)

    ~DDLPlan() {}

    /**
     * @brief 构造DDL计划节点
     * @param tag 计划类型(T_CreateTable/T_DropTable/T_CreateIndex/T_DropIndex)
     * @param tab_name 目标表名
     * @param col_names 列名列表
     * @param cols 列定义列表
     */
    DDLPlan(PlanTag tag, std::string tab_name, std::vector<std::string> col_names, std::vector<ColDef> cols)
        : Plan(tag), tab_name_(std::move(tab_name)), tab_col_names_(std::move(col_names)), cols_(std::move(cols)) {}
};

/**
 * @brief 其他操作计划节点类
 *
 * 处理辅助性质的数据库操作，包括：
 * - HELP: 显示帮助信息
 * - SHOW TABLES: 显示所有表
 * - DESC TABLE: 显示表结构
 * - BEGIN/COMMIT/ABORT/ROLLBACK: 事务控制
 */
class OtherPlan : public Plan {
   public:
    std::string tab_name_;  ///< 目标表名(用于DESC TABLE等操作)

    ~OtherPlan() {}

    /**
     * @brief 构造其他操作计划节点
     * @param tag 计划类型(T_Help/T_ShowTable等)
     * @param tab_name 目标表名
     */
    OtherPlan(PlanTag tag, std::string tab_name) : Plan(tag), tab_name_(std::move(tab_name)) {}
};

/**
 * @brief 系统参数设置计划节点类
 *
 * 用于设置数据库系统的运行参数，如：
 * - 开启/关闭某些优化特性
 * - 调整系统运行模式
 * - 配置执行选项
 */
class SetKnobPlan : public Plan {
   public:
    ast::SetKnobType set_knob_type_;  ///< 要设置的参数类型
    bool bool_value_;                 ///< 参数值(布尔类型)

    /**
     * @brief 构造参数设置计划节点
     * @param knob_type 参数类型
     * @param bool_value 参数值
     */
    SetKnobPlan(ast::SetKnobType knob_type, bool bool_value)
        : Plan(PlanTag::T_SetKnob), set_knob_type_(knob_type), bool_value_(bool_value) {}
};

/**
 * @brief 查询计划生成器辅助信息类
 *
 * 在生成查询执行计划过程中存储和传递必要的信息：
 * - 解析后的SELECT语句
 * - WHERE条件
 * - 投影列
 * - 扫描计划
 * - 更新子句等
 */
class plannerInfo {
   public:
    std::shared_ptr<ast::SelectStmt> parse;                   ///< 解析后的SELECT语句
    std::vector<Condition> where_conds;                       ///< WHERE条件列表
    std::vector<TabCol> sel_cols;                             ///< 投影列列表
    std::shared_ptr<Plan> plan;                               ///< 生成的执行计划
    std::vector<std::shared_ptr<Plan>> table_scan_executors;  ///< 表扫描执行器列表
    std::vector<SetClause> set_clauses;                       ///< 更新子句列表

    /**
     * @brief 构造计划生成器信息对象
     * @param parse_ 解析后的SELECT语句
     */
    plannerInfo(std::shared_ptr<ast::SelectStmt> parse_) : parse(std::move(parse_)) {}
};
