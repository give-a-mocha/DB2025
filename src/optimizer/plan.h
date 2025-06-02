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

enum class PlanTag{
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
    T_SortMerge,    // sort merge join
    T_Sort,
    T_Projection
};

// 查询执行计划
class Plan{
public:
    PlanTag tag;
    virtual ~Plan() = default;
};

/**
 * @brief 表扫描计划类
 * 
 * ScanPlan 类表示一个数据表的扫描计划，是优化器生成的执行计划树的叶节点。
 * 该类支持两种扫描方式：顺序扫描(SeqScan)和索引扫描(IndexScan)，
 * 通过传入的 PlanTag 区分具体扫描类型。
 */
class ScanPlan : public Plan{
public:
    std::string tab_name_;                     ///< 要扫描的表名
    std::vector<ColMeta> cols_;                ///< 表中所有列的元数据
    std::vector<Condition> conds_;             ///< 过滤条件列表
    size_t len_;                               ///< 表中一条记录的长度(字节)
    std::vector<Condition> fed_conds_;         ///< 传递给执行器的过滤条件
    std::vector<std::string> index_col_names_; ///< 索引扫描时使用的列名列表
    
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
    ScanPlan(PlanTag tag, SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names){
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager->db_.get_table(tab_name_);
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;
        fed_conds_ = conds_;
        index_col_names_ = index_col_names;
    }
};

class JoinPlan : public Plan{
public:
    // 左节点
    std::shared_ptr<Plan> left_;
    // 右节点
    std::shared_ptr<Plan> right_;
    // 连接条件
    std::vector<Condition> conds_;
    // 连接的表名
    std::vector<std::string> tables_;
    // future TODO: 后续可以支持的连接类型
    JoinType type;

    ~JoinPlan(){}
    JoinPlan(PlanTag tag, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds){
        Plan::tag = tag;
        left_ = std::move(left);
        right_ = std::move(right);
        conds_ = std::move(conds);
        type = JoinType::INNER_JOIN;
    }
    JoinPlan(PlanTag tag, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds, JoinType type){
        Plan::tag = tag;
        left_ = std::move(left);
        right_ = std::move(right);
        conds_ = std::move(conds);
        this->type = type;
    }
    JoinPlan(PlanTag tag, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds, std::vector<std::string> tables){
        Plan::tag = tag;
        left_ = std::move(left);
        right_ = std::move(right);
        conds_ = std::move(conds);
        tables_ = std::move(tables);
        type = JoinType::INNER_JOIN;
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
    bool isStar_ = false; // 是否是*投影
    ~ProjectionPlan(){}
    ProjectionPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols, bool isStar = false){
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        sel_cols_ = std::move(sel_cols);
        isStar_ = isStar;
    }
};

class SortPlan : public Plan{
public:
    std::shared_ptr<Plan> subplan_;
    TabCol sel_col_;
    bool is_desc_;
    ~SortPlan(){}
    SortPlan(PlanTag tag, std::shared_ptr<Plan> subplan, TabCol sel_col, bool is_desc){
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        sel_col_ = sel_col;
        is_desc_ = is_desc;
    }
};

// dml语句，包括insert; delete; update; select语句　
class DMLPlan : public Plan{
public:
    std::shared_ptr<Plan> subplan_;
    std::string tab_name_;
    std::vector<Value> values_;
    std::vector<Condition> conds_;
    std::vector<SetClause> set_clauses_;
    ~DMLPlan(){}
    DMLPlan(PlanTag tag, std::shared_ptr<Plan> subplan,std::string tab_name,
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

// ddl语句, 包括create/drop table; create/drop index;
class DDLPlan : public Plan{
public:
    std::string tab_name_;
    std::vector<std::string> tab_col_names_;
    std::vector<ColDef> cols_;
    ~DDLPlan(){}
    DDLPlan(PlanTag tag, std::string tab_name, std::vector<std::string> col_names, std::vector<ColDef> cols){
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
        cols_ = std::move(cols);
        tab_col_names_ = std::move(col_names);
    }
};

// help; show tables; desc tables; begin; abort; commit; rollback语句对应的plan
class OtherPlan : public Plan{
public:
    std::string tab_name_;
    ~OtherPlan(){}
    OtherPlan(PlanTag tag, std::string tab_name){
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);            
    }
};

// Set Knob Plan
class SetKnobPlan : public Plan{
public:
    ast::SetKnobType set_knob_type_;
    bool bool_value_;
    SetKnobPlan(ast::SetKnobType knob_type, bool bool_value) {
        Plan::tag = PlanTag::T_SetKnob;
        set_knob_type_ = knob_type;
        bool_value_ = bool_value;
    }
};

class plannerInfo{
public:
    std::shared_ptr<ast::SelectStmt> parse;
    std::vector<Condition> where_conds;
    std::vector<TabCol> sel_cols;
    std::shared_ptr<Plan> plan;
    std::vector<std::shared_ptr<Plan>> table_scan_executors;
    std::vector<SetClause> set_clauses;
    plannerInfo(std::shared_ptr<ast::SelectStmt> parse_):parse(std::move(parse_)){}

};
