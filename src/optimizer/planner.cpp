/**
 * @file planner.cpp
 * @author RMDB Development Team
 * @brief 查询计划生成器的具体实现
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * 该文件实现了查询优化器的核心功能：
 * 1. 索引选择：确定最优的索引访问路径
 * 2. 连接顺序优化：使用贪心算法选择最佳连接顺序
 * 3. 条件下推：将过滤条件尽早应用以减少中间结果
 * 4. 投影优化：及时进行投影以减少数据传输
 */

#include "planner.h"

#include <climits>
#include <memory>
#include <unordered_set>

#include "common/TraceStack.hpp"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "index/ix.h"
#include "record_printer.h"

extern SmManager sm_manager;

// 实现最左匹配原则的索引匹配规则
/**
 * @brief 根据查询条件选择最优索引
 * @param tab_name 表名
 * @param curr_conds 当前的查询条件
 * @param index_col_names 输出参数，存储选中的索引列名
 * @return 是否找到可用索引
 */
bool Planner::get_index_cols(std::string tab_name, std::vector<Condition> curr_conds,
                             std::vector<std::string> &index_col_names) {
    TRACE_FUNCTION
    index_col_names.clear();
    TabMeta &tab = sm_manager.db_.get_table(tab_name);
    if (tab.indexes.empty()) {
        return false;
    }

    // 从条件中提取所有涉及该表的列
    std::unordered_set<std::string> cols;
    for (auto &cond : curr_conds) {
        if (cond.rhs_type == ConditionRhsType::RHS_VALUE && cond.lhs_col.tab_name.compare(tab_name) == 0) {
            // 支持等值条件和范围条件
            if (cond.op == CompOp::OP_EQ || cond.op == CompOp::OP_GT || cond.op == CompOp::OP_GE ||
                cond.op == CompOp::OP_LT || cond.op == CompOp::OP_LE) {
                cols.insert(cond.lhs_col.col_name);
            }
        }
    }

    if (cols.empty()) return false;

    // 寻找最匹配的索引（支持最左匹配原则）
    IndexMeta *ans = nullptr;
    int mx = 0;
    for (auto &index : tab.indexes) {
        int cnt = 0;

        // 检查从索引的第一列开始，连续能匹配多少列
        for (int i = 0; i < index.col_num; ++i) {
            if (cols.count(index.cols[i].name) > 0) {
                cnt++;
            } else {
                break;  // 最左匹配原则：如果某一列不匹配，则停止
            }
        }

        if (cnt > mx) {
            mx = cnt;
            ans = &index;
        }
    }
    if (ans != nullptr && mx > 0) {
        // 找到最匹配的索引，返回其列名
        for (int i = 0; i < ans->col_num; ++i) {
            index_col_names.push_back(ans->cols[i].name);
        }
        return true;
    }
    return false;
}

/**
 * @brief 表算子条件谓词生成
 * 从查询语句的WHERE子句中提取出可以应用于单个表扫描操作的条件（也称为谓词）。
 * @param conds 条件
 * @param tab_names 表名
 * @return std::vector<Condition>
 */
std::unordered_map<std::string_view, std::vector<Condition>> pop_conds(std::vector<Condition> &conds,
                                                                       const std::vector<std::string> &tab_names) {
    TRACE_FUNCTION
    std::unordered_map<std::string_view, std::vector<Condition>> cond_map;
    for (const auto &tab_name : tab_names) {
        cond_map[tab_name] = {};
    }
    std::vector<Condition> remaining_conds;
    for (auto &it : conds) {
        if (it.rhs_type == ConditionRhsType::RHS_VALUE || it.rhs_type == ConditionRhsType::RHS_EXPR ||
            it.lhs_col.tab_name.compare(it.rhs_col.tab_name) == 0) {
            cond_map[it.lhs_col.tab_name].emplace_back(std::move(it));
        } else {
            remaining_conds.emplace_back(std::move(it));
        }
    }
    conds = std::move(remaining_conds);
    return cond_map;
}
/**
 * @brief 将条件下推到查询计划树中
 * @param cond 要下推的条件指针
 * @param plan 当前计划节点
 * @return 条件处理状态码
 */
int push_conds(Condition *cond, std::shared_ptr<Plan> plan) {
    TRACE_FUNCTION
    if (plan->tag == PlanTag::T_SeqScan || plan->tag == PlanTag::T_IndexScan) {
        auto x = std::static_pointer_cast<ScanPlan>(plan);
        if (x->tab_name_.compare(cond->lhs_col.tab_name) == 0) {
            return 1;
        } else if (x->tab_name_.compare(cond->rhs_col.tab_name) == 0) {
            return 2;
        } else {
            return 0;
        }
    } else if (plan->tag == PlanTag::T_NestLoop || plan->tag == PlanTag::T_SortMerge) {
        auto x = std::static_pointer_cast<JoinPlan>(plan);
        int left_res = push_conds(cond, x->left_);
        // 条件已经下推到左子节点
        if (left_res == 3) {
            return 3;
        }
        int right_res = push_conds(cond, x->right_);
        // 条件已经下推到右子节点
        if (right_res == 3) {
            return 3;
        }
        // 左子节点或右子节点有一个没有匹配到条件的列
        if (left_res == 0 || right_res == 0) {
            return left_res + right_res;
        }
        // 左子节点匹配到条件的右边
        if (left_res == 2) {
            std::swap(cond->lhs_col, cond->rhs_col);
            cond->op = swap_op(cond->op);
        }
        x->conds_.emplace_back(std::move(*cond));
        return 3;
    }
    return 0;
}

std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query, Context *context) {
    TRACE_FUNCTION
    // 使用优化的连接顺序生成计划
    std::shared_ptr<Plan> plan = make_one_rel_optimized(query);

    // plan = generate_group_plan(query, std::move(plan));
    // plan = generate_aggregate_plan(query, std::move(plan));
    plan = generate_group_and_aggregate_plan(query, std::move(plan));
    // 处理orderby
    plan = generate_sort_plan(query, std::move(plan));
    // 处理limit
    if (query->limit != std::make_pair(0, std::numeric_limits<int>::max())) {
        plan = std::make_shared<LimitPlan>(std::move(plan), query->limit.first, query->limit.second);
    }
    return plan;
}

/**
 * @brief 生成排序执行计划
 * @param query 待排序的查询
 * @param plan 原始执行计划
 * @return 添加排序操作后的执行计划
 */
std::shared_ptr<Plan> Planner::generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan) {
    TRACE_FUNCTION
    if (query->order_bys.empty()) {
        // 如果没有ORDER BY子句，则不需要排序
        return plan;
    }
    if (query->group_cols.empty() && query->cols.front().agg_type != AggregateType::NONE) {
        // 如果没有GROUP BY且查询列中有聚合值，则结果只包含一列，无需排序
        return plan;
    }
    std::vector<TabCol> sel_cols;
    std::vector<bool> is_desc_list;

    // 处理所有排序列
    for (const auto &orderby_info : query->order_bys) {
        sel_cols.push_back(orderby_info.col);
        is_desc_list.push_back(orderby_info.dir == ast::OrderByDir::OrderBy_DESC);
    }

    return std::make_shared<SortPlan>(PlanTag::T_Sort, std::move(plan), sel_cols, is_desc_list);
}

std::shared_ptr<Plan> Planner::generate_aggregate_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan) {
    TRACE_FUNCTION
    auto query_cols = query->cols;
    for (const auto &group_col : query->group_cols) {
        if (std::find_if(query_cols.begin(), query_cols.end(), [&](const TabCol &col) {
                return std::tie(col.tab_name, col.col_name, col.agg_type) ==
                       std::tie(group_col.tab_name, group_col.col_name, group_col.agg_type);
            }) == query_cols.end()) {
            // 如果分组列不在查询列中，则添加到查询列中
            query_cols.emplace_back(group_col);
        }
    }
    if (all_of(query_cols.begin(), query_cols.end(),
               [](const TabCol &col) { return col.agg_type == AggregateType::NONE; })) {
        // 如果没有聚合函数，则不需要生成聚合计划
        return plan;
    }
    return std::make_shared<AggregatePlan>(PlanTag::T_Aggregate, std::move(plan), query_cols);
}

// GROUPANDAGGREGATE PLAN
std::shared_ptr<Plan> Planner::generate_group_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan) {
    TRACE_FUNCTION
    if (query->group_cols.empty()) {
        return plan;
    }
    return std::make_shared<GroupPlan>(PlanTag::T_Group, std::move(plan), query->cols, query->group_cols,
                                       query->having_conds);
}

std::shared_ptr<Plan> Planner::generate_group_and_aggregate_plan(std::shared_ptr<Query> query,
                                                                 std::shared_ptr<Plan> plan) {
    TRACE_FUNCTION
    std::vector<TabCol> agg_cols;
    for (const auto &col : query->cols) {
        if (col.agg_type != AggregateType::NONE) {
            agg_cols.push_back(col);
        }
    }

    for (const auto &cond : query->having_conds) {
        if (cond.lhs_col.agg_type != AggregateType::NONE) {
            if (std::find(agg_cols.begin(), agg_cols.end(), cond.lhs_col) == agg_cols.end()) {
                agg_cols.push_back(cond.lhs_col);
            }
        }
        if (cond.rhs_type == ConditionRhsType::RHS_COLUMN && cond.rhs_col.agg_type != AggregateType::NONE) {
            if (std::find(agg_cols.begin(), agg_cols.end(), cond.rhs_col) == agg_cols.end()) {
                agg_cols.push_back(cond.rhs_col);
            }
        }
    }

    if (query->group_cols.empty() && agg_cols.empty()) {
        return plan;
    }
    return std::make_shared<GroupAggregatePlan>(PlanTag::T_GroupAggregate, std::move(plan),
                                                std::move(query->group_cols), std::move(agg_cols), query->having_conds);
}

/**
 * @brief select plan 生成
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 */
std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query, Context *context) {
    TRACE_FUNCTION
    // 逻辑优化
    // query = logical_optimization(std::move(query), context);

    // 物理优化
    auto sel_cols = query->cols;
    // joinPlan Or scanPlan Or sortPlan
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);
    bool is_star = std::static_pointer_cast<ast::SelectStmt>(query->parse)->cols.empty();
    bool is_count_star = false;
    for (const auto &col : sel_cols) {
        if (col.agg_type == AggregateType::COUNT && col.col_name == "*") {
            // 如果是count(*)，则不做投影下推
            is_count_star = true;
            break;
        }
    }
    // 如果是 * 或者 count(*)，则不需要投影下推
    if (is_star || is_count_star) {
        return std::make_shared<ProjectionPlan>(PlanTag::T_Projection, std::move(plannerRoot), sel_cols, is_star);
    }
    // 保证投影列不重复
    std::vector<TabCol> project_cols;
    for (auto &col : sel_cols) {
        if (std::find_if(project_cols.begin(), project_cols.end(), [&](const TabCol &x) -> bool {
                return x.tab_name == col.tab_name && x.col_name == col.col_name;
            }) == project_cols.end()) {
            project_cols.emplace_back(col);
        }
    }
    plannerRoot = build_projection_plan_just_scan(std::move(plannerRoot), project_cols);
    if (plannerRoot->tag == PlanTag::T_Projection) {
        auto x = std::static_pointer_cast<ProjectionPlan>(plannerRoot);
        plannerRoot = std::make_shared<ProjectionPlan>(PlanTag::T_Projection, std::move(x->subplan_),
                                                       std::move(sel_cols), is_star);
    } else {
        plannerRoot = std::make_shared<ProjectionPlan>(PlanTag::T_Projection, std::move(plannerRoot),
                                                       std::move(sel_cols), is_star);
    }
    return plannerRoot;
}

/**
 * @brief 生成DDL和DML语句的查询执行计划
 * @param query 查询对象
 * @param context 执行上下文
 * @return 生成的执行计划
 */
std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query, Context *context) {
    TRACE_FUNCTION
    std::shared_ptr<Plan> plannerRoot;
    switch (query->parse->getType()) {
        case ast::AstType::CreateTable: {
            auto x = std::static_pointer_cast<ast::CreateTable>(query->parse);
            // create table;
            std::vector<ColDef> col_defs;
            for (auto &field : x->fields) {
                if (field->getType() == ast::AstType::ColDef) {
                    auto sv_col_def = std::static_pointer_cast<ast::ColDef>(field);
                    ColType col_type = interp_sv_type(sv_col_def->type_len->type);
                    int col_len;

                    // 根据类型设置正确的长度
                    switch (col_type) {
                        case ColType::TYPE_INT:
                            col_len = sizeof(int);
                            break;
                        case ColType::TYPE_FLOAT:
                            col_len = sizeof(float);
                            break;
                        case ColType::TYPE_STRING:
                            col_len = sv_col_def->type_len->len;
                            break;
                        default:
                            throw InternalError("Unknown column type");
                    }

                    ColDef col_def = {.name = sv_col_def->col_name, .type = col_type, .len = col_len};
                    col_defs.push_back(col_def);
                } else {
                    throw InternalError("Planner Unexpected field type");
                }
            }
            plannerRoot =
                std::make_shared<DDLPlan>(PlanTag::T_CreateTable, x->tab_name, std::vector<std::string>(), col_defs);
            break;
        }
        case ast::AstType::DropTable: {
            auto x = std::static_pointer_cast<ast::DropTable>(query->parse);
            // drop table;
            plannerRoot = std::make_shared<DDLPlan>(PlanTag::T_DropTable, x->tab_name, std::vector<std::string>(),
                                                    std::vector<ColDef>());
            break;
        }
        case ast::AstType::CreateIndex: {
            auto x = std::static_pointer_cast<ast::CreateIndex>(query->parse);
            // create index;
            plannerRoot =
                std::make_shared<DDLPlan>(PlanTag::T_CreateIndex, x->tab_name, x->col_names, std::vector<ColDef>());
            break;
        }
        case ast::AstType::DropIndex: {
            auto x = std::static_pointer_cast<ast::DropIndex>(query->parse);
            // drop index
            plannerRoot =
                std::make_shared<DDLPlan>(PlanTag::T_DropIndex, x->tab_name, x->col_names, std::vector<ColDef>());
            break;
        }
        case ast::AstType::InsertStmt: {
            auto x = std::static_pointer_cast<ast::InsertStmt>(query->parse);
            // insert;
            plannerRoot = std::make_shared<DMLPlan>(PlanTag::T_Insert, std::shared_ptr<Plan>(), x->tab_name,
                                                    query->values, std::vector<Condition>(), std::vector<SetClause>());
            break;
        }
        case ast::AstType::DeleteStmt: {
            auto x = std::static_pointer_cast<ast::DeleteStmt>(query->parse);
            // delete;
            // 生成表扫描方式
            std::shared_ptr<Plan> table_scan_executors;
            // 只有一张表，不需要进行物理优化了
            // int index_no = get_indexNo(x->tab_name, query->conds);
            std::vector<std::string> index_col_names;
            bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);

            if (index_exist == false) {  // 该表没有索引
                index_col_names.clear();
                table_scan_executors =
                    std::make_shared<ScanPlan>(PlanTag::T_SeqScan, x->tab_name, query->conds, index_col_names);
            } else {  // 存在索引
                table_scan_executors =
                    std::make_shared<ScanPlan>(PlanTag::T_IndexScan, x->tab_name, query->conds, index_col_names);
            }

            plannerRoot = std::make_shared<DMLPlan>(PlanTag::T_Delete, table_scan_executors, x->tab_name,
                                                    std::vector<Value>(), query->conds, std::vector<SetClause>());
            break;
        }
        case ast::AstType::UpdateStmt: {
            auto x = std::static_pointer_cast<ast::UpdateStmt>(query->parse);
            // update;
            // 生成表扫描方式
            std::shared_ptr<Plan> table_scan_executors;
            // 只有一张表，不需要进行物理优化了
            // int index_no = get_indexNo(x->tab_name, query->conds);
            std::vector<std::string> index_col_names;
            bool index_exist = get_index_cols(x->tab_name->tab_name, query->conds, index_col_names);

            if (index_exist == false) {  // 该表没有索引
                index_col_names.clear();
                table_scan_executors = std::make_shared<ScanPlan>(PlanTag::T_SeqScan, x->tab_name->tab_name,
                                                                  query->conds, index_col_names);
            } else {  // 存在索引
                table_scan_executors = std::make_shared<ScanPlan>(PlanTag::T_IndexScan, x->tab_name->tab_name,
                                                                  query->conds, index_col_names);
            }
            plannerRoot = std::make_shared<DMLPlan>(PlanTag::T_Update, table_scan_executors, x->tab_name->tab_name,
                                                    std::vector<Value>(), query->conds, query->set_clauses);
            break;
        }
        case ast::AstType::ExplainStmt: {
            auto x = std::static_pointer_cast<ast::ExplainStmt>(query->parse);
            // explain
            std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
            plannerRoot = std::make_shared<DMLPlan>(PlanTag::T_explain, projection, std::string(), std::vector<Value>(),
                                                    std::vector<Condition>(), std::vector<SetClause>());
            break;
        }
        case ast::AstType::SelectStmt: {
            // 生成select语句的查询执行计划
            std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
            plannerRoot = std::make_shared<DMLPlan>(PlanTag::T_select, projection, std::string(), std::vector<Value>(),
                                                    std::vector<Condition>(), std::vector<SetClause>());
            break;
        }
        default:
            throw InternalError("Unexpected AST root");
    }
    return plannerRoot;
}

/**
 * @brief 获取表的基数（记录数量）
 */
size_t Planner::get_table_cardinality(const std::string &tab_name) {
    TRACE_FUNCTION
    // 通过文件句柄获取表的记录数量
    auto it = sm_manager.fhs_.find(tab_name);
    size_t res = 0;
    if (it != sm_manager.fhs_.end()) {
        res = it->second->get_file_hdr().record_num;
    }
    return res;
}
/**
 * @brief 获取表的列数
 * @param tab_name 表名
 * @return 表的列数，如果表不存在返回0
 *
 * @details 统计过程：
 * 1. 获取表元数据
 * 2. 检查列信息是否存在
 * 3. 返回列数量
 */
int Planner::get_table_col_num(const std::string &tab_name) {
    TRACE_FUNCTION
    // 获取表的列数
    auto it = sm_manager.db_.get_table(tab_name);
    if (it.cols.empty()) {
        return 0;  // 如果表不存在或没有列，返回0
    }
    return it.cols.size();
}

/**
 * @brief 使用贪心算法优化多表连接顺序
 */
/**
 * @brief 使用贪心算法优化的查询计划生成
 * @param query 查询对象
 * @return 优化后的查询执行计划
 */
// Scan / Join
std::shared_ptr<Plan> Planner::make_one_rel_optimized(std::shared_ptr<Query> query) {
    TRACE_FUNCTION
    std::vector<std::string> tables = std::move(query->tables);
    query->tables.clear();

    // 将JOIN连接条件添加到conds中
    for (auto &join_node : query->jointree) {
        for (auto &cond : join_node.join_conds) {
            query->conds.emplace_back(std::move(cond));
        }
    }
    query->jointree.clear();

    // 谓词下推
    std::vector<std::pair<std::shared_ptr<Plan>, size_t>> table_plans_with_cardinality(tables.size());

    std::unordered_map<std::string_view, std::vector<Condition>> table_conds = pop_conds(query->conds, tables);
    for (size_t i = 0; i < tables.size(); i++) {
        auto curr_conds = std::move(table_conds[tables[i]]);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(tables[i], curr_conds, index_col_names);
        size_t cardinality = get_table_cardinality(tables[i]);
        std::shared_ptr<Plan> scan_plan;
        if (index_exist == false) {  // 该表没有索引
            index_col_names.clear();
            scan_plan =
                std::make_shared<ScanPlan>(PlanTag::T_SeqScan, std::move(tables[i]), curr_conds, index_col_names);
        } else {  // 存在索引
            scan_plan =
                std::make_shared<ScanPlan>(PlanTag::T_IndexScan, std::move(tables[i]), curr_conds, index_col_names);
        }
        table_plans_with_cardinality[i] = {std::move(scan_plan), cardinality};
    }

    // 获取连接条件
    std::list<Condition> join_conditions;
    for (auto &cond : query->conds) {
        join_conditions.emplace_back(std::move(cond));
    }
    query->conds.clear();

    // 对表扫描计划按基数排序
    std::sort(table_plans_with_cardinality.begin(), table_plans_with_cardinality.end(),
              [](const auto &a, const auto &b) { return a.second < b.second; });

    std::list<std::shared_ptr<Plan>> tables_plans;
    for (auto &pair : table_plans_with_cardinality) {
        tables_plans.emplace_back(std::move(pair.first));
    }
    table_plans_with_cardinality.clear();

    // 构建左深树连接计划
    return build_left_deep_join_tree(tables_plans, join_conditions);
}

/**
 * @brief 构建左深树连接计划
 * 这里使用贪心算法，选择基数最小的表开始连接
 */
std::shared_ptr<Plan> Planner::build_left_deep_join_tree(std::list<std::shared_ptr<Plan>> &table_plans,
                                                         std::list<Condition> &join_conditions) {
    // 开始构建左深树
    std::shared_ptr<Plan> result = nullptr;
    std::unordered_set<std::string> joined_tables;

    // 获取第一个表作为初始结果
    std::shared_ptr<ScanPlan> first_scan = std::static_pointer_cast<ScanPlan>(table_plans.front());
    joined_tables.insert(first_scan->tab_name_);
    result = std::move(first_scan);
    table_plans.pop_front();

    // 继续连接剩余的表
    while (!table_plans.empty()) {
        bool flag = false;
        result = add_join(result, joined_tables, table_plans, join_conditions, flag);
        // 如果没有找到可连接的表，强制连接剩余的第一个未使用表
        if (!flag) {
            std::shared_ptr<ScanPlan> current_scan = std::static_pointer_cast<ScanPlan>(*table_plans.begin());
            std::string current_table = current_scan->tab_name_;
            joined_tables.insert(current_scan->tab_name_);
            result = join_tables(std::move(result), current_table, std::move(current_scan), joined_tables,
                                 join_conditions, JoinType::INNER_JOIN);
            table_plans.pop_front();
        }
    }

    // 处理剩余的连接条件（理论来说不会有,但是保留处理）
    if (!join_conditions.empty()) {
        // 将剩余条件下推到结果计划中
        for (auto &cond : join_conditions) {
            push_conds(&cond, result);
        }
        join_conditions.clear();
    }

    return result;
}

std::shared_ptr<Plan> Planner::add_join(std::shared_ptr<Plan> result, std::unordered_set<std::string> &joined_tables,
                                        std::list<std::shared_ptr<Plan>> &table_plans,
                                        std::list<Condition> &join_conditions, bool &flag) {
    // 进入这个函数一定是非空，但是保留检查
    if (table_plans.empty()) {
        return result;
    }
    flag = false;
    auto it = table_plans.begin();
    while (it != table_plans.end()) {
        std::shared_ptr<ScanPlan> current_scan = std::static_pointer_cast<ScanPlan>(*it);
        std::string current_table = current_scan->tab_name_;

        // 检查当前表是否已经连接（这种情况理论上不应该发生，但保留检查）
        if (joined_tables.count(current_table) > 0) {
            flag = true;
            it = table_plans.erase(it);
            break;
        }
        // 检查当前表是否可以连接
        bool can_join = false;
        for (const auto &cond : join_conditions) {
            if ((cond.lhs_col.tab_name == current_table && joined_tables.count(cond.rhs_col.tab_name) > 0) ||
                (cond.rhs_col.tab_name == current_table && joined_tables.count(cond.lhs_col.tab_name) > 0)) {
                can_join = true;
                break;
            }
        }
        if (can_join) {
            // 如果可以连接，执行连接
            result = join_tables(std::move(result), current_table, std::move(*it), joined_tables, join_conditions,
                                 JoinType::INNER_JOIN);
            joined_tables.insert(current_table);
            it = table_plans.erase(it);
            flag = true;
            break;
        } else {
            it++;
        }
    }
    return result;
}

// 复杂度O(N)
std::shared_ptr<Plan> Planner::join_tables(std::shared_ptr<Plan> result, const std::string &current_table,
                                           std::shared_ptr<Plan> current_scan,
                                           std::unordered_set<std::string> &joined_tables,
                                           std::list<Condition> &join_conditions, JoinType join_type) {
    std::vector<Condition> applicable_conds;
    auto it = join_conditions.begin();
    while (it != join_conditions.end()) {
        bool left_in_joined = joined_tables.count(it->lhs_col.tab_name) > 0;
        bool right_in_current = it->rhs_col.tab_name == current_table;
        bool right_in_joined = joined_tables.count(it->rhs_col.tab_name) > 0;
        bool left_in_current = it->lhs_col.tab_name == current_table;
        if ((left_in_joined && right_in_current) || (right_in_joined && left_in_current)) {
            // 确保条件的左边是已连接的表，右边是当前表
            // if (right_in_joined && left_in_current) {
            //     std::swap(it->lhs_col, it->rhs_col);
            //     it->op = swap_op(it->op);
            // }
            applicable_conds.emplace_back(std::move(*it));
            it = join_conditions.erase(it);
        } else {
            ++it;
        }
    }
    // 创建连接计划
    if (enable_nestedloop_join && enable_sortmerge_join) {
        result = std::make_shared<JoinPlan>(PlanTag::T_NestLoop, std::move(result), std::move(current_scan),
                                            std::move(applicable_conds), join_type);
    } else if (enable_nestedloop_join) {
        result = std::make_shared<JoinPlan>(PlanTag::T_NestLoop, std::move(result), std::move(current_scan),
                                            std::move(applicable_conds), join_type);
    } else if (enable_sortmerge_join) {
        result = std::make_shared<JoinPlan>(PlanTag::T_SortMerge, std::move(result), std::move(current_scan),
                                            std::move(applicable_conds), join_type);
    } else {
        throw RMDBError("No join executor selected!");
    }
    return result;
}

/**
 * @brief 构建投影计划
 * @param plan 输入的执行计划
 * @param need_cols 需要投影的列
 * @param all_cols 所有涉及的列
 * @return 优化后的执行计划
 */
std::shared_ptr<Plan> Planner::build_projection_plan_just_scan(std::shared_ptr<Plan> plan,
                                                               std::vector<TabCol> &need_cols) {
    size_t siz = need_cols.size();
    if (plan->tag == PlanTag::T_NestLoop || plan->tag == PlanTag::T_SortMerge) {
        auto x = std::static_pointer_cast<JoinPlan>(plan);
        for (auto &cond : x->conds_) {
            // 检查是否已经存在
            if (std::find_if(need_cols.begin(), need_cols.end(), [&](const TabCol &x) -> bool {
                    return x.tab_name == cond.lhs_col.tab_name && x.col_name == cond.lhs_col.col_name;
                }) == need_cols.end()) {
                need_cols.emplace_back(cond.lhs_col);
            }
            if (std::find_if(need_cols.begin(), need_cols.end(), [&](const TabCol &x) -> bool {
                    return x.tab_name == cond.rhs_col.tab_name && x.col_name == cond.rhs_col.col_name;
                }) == need_cols.end()) {
                need_cols.emplace_back(cond.rhs_col);
            }
        }
        x->left_ = build_projection_plan_just_scan(std::move(x->left_), need_cols);
        x->right_ = build_projection_plan_just_scan(std::move(x->right_), need_cols);
        while (need_cols.size() > siz) {
            need_cols.pop_back();
        }
        return x;
    } else if (plan->tag == PlanTag::T_SeqScan || plan->tag == PlanTag::T_IndexScan) {
        auto x = std::static_pointer_cast<ScanPlan>(plan);
        for (auto &cond : x->conds_) {
            if (std::find_if(need_cols.begin(), need_cols.end(), [&](const TabCol &x) -> bool {
                    return x.tab_name == cond.lhs_col.tab_name && x.col_name == cond.lhs_col.col_name;
                }) == need_cols.end()) {
                need_cols.emplace_back(cond.lhs_col);
            }
            if (std::find_if(need_cols.begin(), need_cols.end(), [&](const TabCol &x) -> bool {
                    return x.tab_name == cond.rhs_col.tab_name && x.col_name == cond.rhs_col.col_name;
                }) == need_cols.end()) {
                need_cols.emplace_back(cond.rhs_col);
            }
        }
        while (need_cols.size() > siz) {
            need_cols.pop_back();
        }
        std::vector<TabCol> project_cols;
        for (auto &col : need_cols) {
            if (col.tab_name.compare(x->tab_name_) == 0) {
                project_cols.push_back(col);
                project_cols.back().agg_type = AggregateType::NONE;
            }
        }
        if (project_cols.size() != static_cast<size_t>(get_table_col_num(x->tab_name_))) {
            return std::make_shared<ProjectionPlan>(PlanTag::T_Projection, std::move(plan), project_cols);
        }
        return x;
    } else if (plan->tag == PlanTag::T_Sort) {
        auto x = std::static_pointer_cast<SortPlan>(plan);
        // 添加所有排序列到需要的列中
        for (const auto &sort_col : x->sel_cols_) {
            if (std::find_if(need_cols.begin(), need_cols.end(), [&](const TabCol &x) -> bool {
                    return x.tab_name == sort_col.tab_name && x.col_name == sort_col.col_name;
                }) == need_cols.end()) {
                need_cols.emplace_back(sort_col);
            }
        }
        x->subplan_ = build_projection_plan_just_scan(std::move(x->subplan_), need_cols);
        while (need_cols.size() > siz) {
            need_cols.pop_back();
        }
        return x;
    } else if (plan->tag == PlanTag::T_Group) {
        auto x = std::static_pointer_cast<GroupPlan>(plan);
        for (const auto &col : x->group_cols_) {
            if (std::find_if(need_cols.begin(), need_cols.end(), [&](const TabCol &x) -> bool {
                    return x.tab_name == col.tab_name && x.col_name == col.col_name;
                }) == need_cols.end()) {
                need_cols.emplace_back(col);
            }
        }
        x->subplan_ = build_projection_plan_just_scan(std::move(x->subplan_), need_cols);
        while (need_cols.size() > siz) {
            need_cols.pop_back();
        }
        return x;
    } else if (plan->tag == PlanTag::T_Aggregate) {
        auto x = std::static_pointer_cast<AggregatePlan>(plan);
        x->subplan_ = build_projection_plan_just_scan(std::move(x->subplan_), need_cols);
        return x;
    } else if (plan->tag == PlanTag::T_GroupAggregate) {
        auto x = std::static_pointer_cast<GroupAggregatePlan>(plan);
        for (const auto &col : x->group_cols_) {
            if (std::find_if(need_cols.begin(), need_cols.end(), [&](const TabCol &x) -> bool {
                    return x.tab_name == col.tab_name && x.col_name == col.col_name;
                }) == need_cols.end()) {
                need_cols.emplace_back(col);
            }
        }

        bool agg_star = false;

        for (const auto &col : x->agg_cols_) {
            if (col.col_name == "*" && col.agg_type == AggregateType::COUNT) {
                agg_star = true;
                break;
            }
            if (std::find_if(need_cols.begin(), need_cols.end(), [&](const TabCol &x) -> bool {
                    return x.tab_name == col.tab_name && x.col_name == col.col_name;
                }) == need_cols.end()) {
                need_cols.emplace_back(col);
            }
        }
        if (!agg_star) {
            x->subplan_ = build_projection_plan_just_scan(std::move(x->subplan_), need_cols);
        }
        while (need_cols.size() > siz) {
            need_cols.pop_back();
        }
        return x;
    } else if (plan->tag == PlanTag::T_Limit) {
        auto x = std::static_pointer_cast<LimitPlan>(plan);
        x->subplan_ = build_projection_plan_just_scan(std::move(x->subplan_), need_cols);
        return x;
    } else {
        throw InternalError("Unexpected plan type in projection optimization");
    }
}