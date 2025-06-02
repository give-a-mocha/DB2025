/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "planner.h"

#include <memory>
#include <set>
#include <unordered_set>

#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "index/ix.h"
#include "record_printer.h"

// 实现最左匹配原则的索引匹配规则
bool Planner::get_index_cols(std::string tab_name, std::vector<Condition> curr_conds,
                             std::vector<std::string> &index_col_names) {
    // 初始代码，完全一致匹配原则
    // index_col_names.clear();
    // for(auto& cond: curr_conds) {
    //     if(cond.is_rhs_val && cond.op == OP_EQ && cond.lhs_col.tab_name.compare(tab_name) == 0)
    //         index_col_names.push_back(cond.lhs_col.col_name);
    // }
    // TabMeta& tab = sm_manager_->db_.get_table(tab_name);
    // if(tab.is_index(index_col_names)) return true;
    // return false;

    index_col_names.clear();
    TabMeta &tab = sm_manager_->db_.get_table(tab_name);
    if (tab.indexes.empty()) {
        return false;
    }

    // 从条件中提取所有涉及该表的列
    std::unordered_set<std::string> cols;
    for (auto &cond : curr_conds) {
        if (cond.is_rhs_val && cond.lhs_col.tab_name.compare(tab_name) == 0) {
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
std::vector<Condition> pop_conds(std::vector<Condition> &conds, std::string tab_names) {
    // auto has_tab = [&](const std::string &tab_name) {
    //     return std::find(tab_names.begin(), tab_names.end(), tab_name) != tab_names.end();
    // };
    std::vector<Condition> solved_conds;
    std::vector<Condition> temp;
    for (auto &it : conds) {
        if (it.lhs_col.tab_name.compare(tab_names) == 0 &&
            (it.is_rhs_val || it.lhs_col.tab_name.compare(it.rhs_col.tab_name) == 0)) {
            solved_conds.emplace_back(std::move(it));
        } else {
            temp.emplace_back(std::move(it));
        }
    }
    conds = std::move(temp);
    return solved_conds;
}

/**
 * @brief 递归地将条件推送到查询计划树的下方。
 *
 * 此函数尝试将给定的条件与能够评估该条件的最低层级的计划节点关联起来。
 * - 对于 ScanPlan（扫描计划节点）：检查条件是否适用于正在扫描的表。
 * - 对于 JoinPlan（连接计划节点）：尝试将条件推向其左子节点或右子节点。
 *   如果条件是一个连接谓词，并且适用于此 JoinPlan 正在连接的表，则该条件
 *   会被添加到此 JoinPlan 自身的条件列表中。条件可能会被规范化
 *   （例如，交换左右操作数以形成类似 LeftTable.col = RightTable.col 的标准形式）。
 *
 * 函数使用特定的返回代码来指示条件如何与当前计划节点及其子树相关联：
 * - 0：条件不涉及当前计划或其子计划中的任何表。
 * - 1：条件的左操作数（LHS）列所属的表与此计划节点（或其子节点）相关的表匹配。
 * - 2：条件的右操作数（RHS）列所属的表与此计划节点（或其子节点）相关的表匹配。
 * - 3：条件已被完全处理并下推/关联到此计划节点或其后代节点之一。
 *      如果关联到 JoinPlan，则 `cond` 指向的 Condition 对象可能会被移走 (moved from)。
 *
 * @param cond 指向要下推的 Condition 对象的指针。
 *             注意：如果条件应用于 JoinPlan，则 `cond` 指向的对象将被移走。
 * @param plan 查询计划树中的当前计划节点（例如 ScanPlan、JoinPlan）。
 * @return 一个整数代码（0、1、2 或 3），指示下推尝试的结果。
 */
int push_conds(Condition *cond, std::shared_ptr<Plan> plan) {
    if (auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
        if (x->tab_name_.compare(cond->lhs_col.tab_name) == 0) {
            return 1;
        } else if (x->tab_name_.compare(cond->rhs_col.tab_name) == 0) {
            return 2;
        } else {
            return 0;
        }
    } else if (auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
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

/**
 * @brief 从一系列计划节点中查找并“弹出”指定表的扫描计划。
 *
 * @param scantbl 一个整型指针，指向一个数组，用于标记哪些扫描计划已被选中。
 *                数组的大小应与 `plans` 向量的大小相同。
 * @param table 要查找的表的名称。
 * @param joined_tables 一个字符串向量的引用，用于记录已连接的表的名称。
 *                      如果找到匹配的扫描计划，该表的名称会被添加到此向量中。
 * @param plans 一个包含共享指针的向量，指向多个候选的计划节点。
 * @return 如果找到匹配的 `ScanPlan`，则返回其共享指针；否则返回 `nullptr`。
 */
std::shared_ptr<Plan> pop_scan(std::vector<int> &scantbl, std::string table, std::vector<std::string> &joined_tables,
                               std::vector<std::shared_ptr<Plan>> plans) {
    for (size_t i = 0; i < plans.size(); i++) {
        auto x = std::dynamic_pointer_cast<ScanPlan>(plans[i]);
        if (x->tab_name_.compare(table) == 0) {
            scantbl[i] = 1;
            joined_tables.emplace_back(x->tab_name_);
            return plans[i];
        }
    }
    return nullptr;
}

std::shared_ptr<Query> Planner::logical_optimization(std::shared_ptr<Query> query, Context *context) {
    if (query == nullptr) return nullptr;

    // 1. 条件下推优化：将单表条件尽早执行
    // 这里已经在make_one_rel中通过pop_conds实现了条件下推

    // 2. 谓词简化：移除恒真条件，标记恒假条件
    // 不存在两侧都为常数的情况
    auto it = query->conds.begin();
    while (it != query->conds.end()) {
        if (!it->is_rhs_val && it->lhs_col == it->rhs_col) {
            if (it->op == CompOp::OP_EQ || it->op == CompOp::OP_LE || it->op == CompOp::OP_GE) {
                // 恒真条件，可以直接移除
                it = query->conds.erase(it);
            } else if (it->op == CompOp::OP_NE) {
                // 如果出现恒假条件，语句无结果
                // TODO
                // return std::make_shared<Query>();
                break;
            } else {
                ++it;
            }
        } else {
            ++it;
        }
    }

    // 3. 冗余表消除：删除不需要的表（没有在结果或条件中使用的表）
    if (query->tables.size() > 1) {
        std::set<std::string> used_tables;

        // 从结果列中收集表名
        for (const auto &col : query->cols) {
            if (!col.tab_name.empty()) {
                used_tables.insert(col.tab_name);
            }
        }

        // 从条件中收集表名
        for (const auto &cond : query->conds) {
            if (!cond.lhs_col.tab_name.empty()) {
                used_tables.insert(cond.lhs_col.tab_name);
            }
            if (!cond.is_rhs_val && !cond.rhs_col.tab_name.empty()) {
                used_tables.insert(cond.rhs_col.tab_name);
            }
        }

        std::remove_if(query->tables.begin(), query->tables.end(),
                       [&used_tables](const std::string &table) { return used_tables.count(table); });
    }

    // TODO
    // 4. 连接重排序：尝试根据表大小进行重排
    // 这需要统计信息，当前系统可能不支持
    // 可以使用一个简单启发式规则：将小表放在外侧
    // if (query->tables.size() > 1 && sm_manager_->db_.is_open()) {
    //     std::vector<std::pair<std::string, int>> table_sizes;

    //     for (const auto& table_name : query->tables) {
    //         // 获取表的大小估计（可以是表的页数或记录数）
    //         int size_estimate = 0;
    //         try {
    //             auto& table_info = sm_manager_->db_.get_table(table_name);
    //             // 使用页数作为大小估计
    //             size_estimate = table_info.file_hdr->num_pages;
    //         } catch (...) {
    //             // 如果无法获取表信息，给一个默认值
    //             size_estimate = 100;
    //         }
    //         table_sizes.emplace_back(table_name, size_estimate);
    //     }

    //     // 根据表大小排序（从小到大）
    //     std::sort(table_sizes.begin(), table_sizes.end(),
    //             [](const auto& a, const auto& b) { return a.second < b.second; });

    //     // 重构tables数组
    //     query->tables.clear();
    //     for (const auto& [table_name, _] : table_sizes) {
    //         query->tables.push_back(table_name);
    //     }
    // }

    return query;
}

std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query, Context *context) {
    // 使用优化的连接顺序生成计划
    std::shared_ptr<Plan> plan = make_one_rel_optimized(query);

    // 处理orderby
    plan = generate_sort_plan(query, std::move(plan));

    return plan;
}

/**
 * @brief 为单关系查询（或多关系查询的单个表部分）创建扫描计划。
 * @param query 查询对象。
 * @return 生成的扫描计划或基础连接计划。
 */
std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query) {
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    std::vector<std::string> tables = query->tables;
    // Scan table , 生成表算子列表tab_nodes
    std::vector<std::shared_ptr<Plan>> table_scan_executors(tables.size());
    for (size_t i = 0; i < tables.size(); i++) {
        auto curr_conds = pop_conds(query->conds, tables[i]);
        // int index_no = get_indexNo(tables[i], curr_conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(tables[i], curr_conds, index_col_names);
        if (index_exist == false) {  // 该表没有索引
            index_col_names.clear();
            table_scan_executors[i] =
                std::make_shared<ScanPlan>(PlanTag::T_SeqScan, sm_manager_, tables[i], curr_conds, index_col_names);
        } else {  // 存在索引
            table_scan_executors[i] =
                std::make_shared<ScanPlan>(PlanTag::T_IndexScan, sm_manager_, tables[i], curr_conds, index_col_names);
        }
    }
    // 只有一个表，不需要join。
    if (tables.size() == 1) {
        return table_scan_executors[0];
    }
    // 获取where条件
    auto conds = std::move(query->conds);
    std::vector<JoinNode> jointree = std::move(query->jointree);
    for (auto &join_node : jointree) {
        // 将连接条件添加到conds中
        for (auto &cond : join_node.join_conds) {
            conds.emplace_back(std::move(cond));
        }
    }
    std::shared_ptr<Plan> table_join_executors;

    std::vector<int> scantbl(tables.size());
    for (size_t i = 0; i < tables.size(); i++) {
        scantbl[i] = -1;
    }
    // 剩下conds中的条件都不是值类型
    // 多表链接 隐式连接
    // SELECT * FROM A, B, C WHERE A.id = B.id AND B.id = C.id AND A.name = C.name
    if (conds.size() >= 1) {
        // 有连接条件

        // 根据连接条件，生成第一层join
        std::vector<std::string> joined_tables(tables.size());
        auto it = conds.begin();
        while (it != conds.end()) {
            std::shared_ptr<Plan> left, right;
            left = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
            right = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
            std::vector<Condition> join_conds{*it};
            // 建立join
            //  判断使用哪种join方式
            if (enable_nestedloop_join && enable_sortmerge_join) {
                // 默认nested loop join
                table_join_executors =
                    std::make_shared<JoinPlan>(PlanTag::T_NestLoop, std::move(left), std::move(right), join_conds);
            } else if (enable_nestedloop_join) {
                table_join_executors =
                    std::make_shared<JoinPlan>(PlanTag::T_NestLoop, std::move(left), std::move(right), join_conds);
            } else if (enable_sortmerge_join) {
                table_join_executors =
                    std::make_shared<JoinPlan>(PlanTag::T_SortMerge, std::move(left), std::move(right), join_conds);
            } else {
                // error
                throw RMDBError("No join executor selected!");
            }

            // table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right),
            // join_conds);
            it = conds.erase(it);
            break;
        }
        // 根据连接条件，生成第2-n层join
        it = conds.begin();
        while (it != conds.end()) {
            std::shared_ptr<Plan> left_need_to_join_executors = nullptr;
            std::shared_ptr<Plan> right_need_to_join_executors = nullptr;
            bool isneedreverse = false;
            if (std::find(joined_tables.begin(), joined_tables.end(), it->lhs_col.tab_name) == joined_tables.end()) {
                left_need_to_join_executors =
                    pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
            }
            if (std::find(joined_tables.begin(), joined_tables.end(), it->rhs_col.tab_name) == joined_tables.end()) {
                right_need_to_join_executors =
                    pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
                isneedreverse = true;
            }

            if (left_need_to_join_executors != nullptr && right_need_to_join_executors != nullptr) {
                std::vector<Condition> join_conds{*it};
                std::shared_ptr<Plan> temp_join_executors =
                    std::make_shared<JoinPlan>(PlanTag::T_NestLoop, std::move(left_need_to_join_executors),
                                               std::move(right_need_to_join_executors), join_conds);
                table_join_executors =
                    std::make_shared<JoinPlan>(PlanTag::T_NestLoop, std::move(temp_join_executors),
                                               std::move(table_join_executors), std::vector<Condition>());
            } else if (left_need_to_join_executors != nullptr || right_need_to_join_executors != nullptr) {
                if (isneedreverse) {
                    std::swap(it->lhs_col, it->rhs_col);
                    it->op = swap_op(it->op);
                    left_need_to_join_executors = std::move(right_need_to_join_executors);
                }
                std::vector<Condition> join_conds{*it};
                table_join_executors =
                    std::make_shared<JoinPlan>(PlanTag::T_NestLoop, std::move(left_need_to_join_executors),
                                               std::move(table_join_executors), join_conds);
            } else {
                push_conds(std::move(&(*it)), table_join_executors);
            }
            it = conds.erase(it);
        }
    } else {
        table_join_executors = table_scan_executors[0];
        scantbl[0] = 1;
    }

    // 连接剩余表
    for (size_t i = 0; i < tables.size(); i++) {
        if (scantbl[i] == -1) {
            table_join_executors =
                std::make_shared<JoinPlan>(PlanTag::T_NestLoop, std::move(table_scan_executors[i]),
                                           std::move(table_join_executors), std::vector<Condition>());
        }
    }

    return table_join_executors;
}

std::shared_ptr<Plan> Planner::generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan) {
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    if (!x->has_sort) {
        return plan;
    }
    std::vector<std::string> tables = query->tables;
    std::vector<ColMeta> all_cols;
    for (auto &sel_tab_name : tables) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
    TabCol sel_col;
    for (auto &col : all_cols) {
        if (col.name.compare(x->order->cols->col_name) == 0) {
            sel_col.tab_name = col.tab_name;
            sel_col.col_name = col.name;
        }
    }
    return std::make_shared<SortPlan>(PlanTag::T_Sort, std::move(plan), sel_col,
                                      x->order->orderby_dir == ast::OrderBy_DESC);
}

/**
 * @brief select plan 生成
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 */
std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query, Context *context) {
    // 逻辑优化
    query = logical_optimization(std::move(query), context);

    // 物理优化
    auto sel_cols = query->cols;
    // joinPlan Or scanPlan Or sortPlan
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);
    // if(auto x = std::dynamic_pointer_cast<ScanPlan>(plannerRoot)) {
    //     std::cerr<< "这是一个ScanPlan: " << x->tab_name_ << std::endl;
    // }else if(auto x = std::dynamic_pointer_cast<JoinPlan>(plannerRoot)) {
    //     std::cerr<< "这是一个JoinPlan:" << std::endl;
    // }else if(auto x = std::dynamic_pointer_cast<SortPlan>(plannerRoot)) {
    //     std::cerr<< "这是一个SortPlan: " << std::endl;
    // }
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    plannerRoot = std::make_shared<ProjectionPlan>(PlanTag::T_Projection, std::move(plannerRoot), std::move(sel_cols),
                                                   x->cols.empty());
    std::vector<TabCol> temp;
    std::vector<TabCol> temp2;
    plannerRoot = build_projection_plan(plannerRoot, temp, temp2);
    return plannerRoot;
}

// 生成DDL语句和DML语句的查询执行计划
std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query, Context *context) {
    std::shared_ptr<Plan> plannerRoot;
    if (auto x = std::dynamic_pointer_cast<ast::CreateTable>(query->parse)) {
        // create table;
        std::vector<ColDef> col_defs;
        for (auto &field : x->fields) {
            if (auto sv_col_def = std::dynamic_pointer_cast<ast::ColDef>(field)) {
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
                throw InternalError("Unexpected field type");
            }
        }
        plannerRoot =
            std::make_shared<DDLPlan>(PlanTag::T_CreateTable, x->tab_name, std::vector<std::string>(), col_defs);
    } else if (auto x = std::dynamic_pointer_cast<ast::DropTable>(query->parse)) {
        // drop table;
        plannerRoot = std::make_shared<DDLPlan>(PlanTag::T_DropTable, x->tab_name, std::vector<std::string>(),
                                                std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::CreateIndex>(query->parse)) {
        // create index;
        plannerRoot =
            std::make_shared<DDLPlan>(PlanTag::T_CreateIndex, x->tab_name, x->col_names, std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::DropIndex>(query->parse)) {
        // drop index
        plannerRoot = std::make_shared<DDLPlan>(PlanTag::T_DropIndex, x->tab_name, x->col_names, std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(query->parse)) {
        // insert;
        plannerRoot = std::make_shared<DMLPlan>(PlanTag::T_Insert, std::shared_ptr<Plan>(), x->tab_name, query->values,
                                                std::vector<Condition>(), std::vector<SetClause>());
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(query->parse)) {
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
                std::make_shared<ScanPlan>(PlanTag::T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        } else {  // 存在索引
            table_scan_executors = std::make_shared<ScanPlan>(PlanTag::T_IndexScan, sm_manager_, x->tab_name,
                                                              query->conds, index_col_names);
        }

        plannerRoot = std::make_shared<DMLPlan>(PlanTag::T_Delete, table_scan_executors, x->tab_name,
                                                std::vector<Value>(), query->conds, std::vector<SetClause>());
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(query->parse)) {
        // update;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);

        if (index_exist == false) {  // 该表没有索引
            index_col_names.clear();
            table_scan_executors =
                std::make_shared<ScanPlan>(PlanTag::T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        } else {  // 存在索引
            table_scan_executors = std::make_shared<ScanPlan>(PlanTag::T_IndexScan, sm_manager_, x->tab_name,
                                                              query->conds, index_col_names);
        }
        plannerRoot = std::make_shared<DMLPlan>(PlanTag::T_Update, table_scan_executors, x->tab_name,
                                                std::vector<Value>(), query->conds, query->set_clauses);
    } else if (auto x = std::dynamic_pointer_cast<ast::ExplainStmt>(query->parse)) {
        // explain
        std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
        plannerRoot = std::make_shared<DMLPlan>(PlanTag::T_explain, projection, std::string(), std::vector<Value>(),
                                                std::vector<Condition>(), std::vector<SetClause>());
    } else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse)) {
        // ?auto root = std::make_shared<plannerInfo>(x);   // 未使用的语句
        // 生成select语句的查询执行计划
        std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
        plannerRoot = std::make_shared<DMLPlan>(PlanTag::T_select, projection, std::string(), std::vector<Value>(),
                                                std::vector<Condition>(), std::vector<SetClause>());
    } else {
        throw InternalError("Unexpected AST root");
    }
    return plannerRoot;
}

/**
 * @brief 获取表的基数（记录数量）
 */
size_t Planner::get_table_cardinality(const std::string &tab_name) {
    // 通过文件句柄获取表的记录数量
    auto it = sm_manager_->fhs_.find(tab_name);
    size_t res = 0;
    if (it != sm_manager_->fhs_.end()) {
        res = it->second->get_file_hdr().record_num;
    }
    return res;
}
int Planner::get_table_col_num(const std::string &tab_name) {
    // 获取表的列数
    auto it = sm_manager_->db_.get_table(tab_name);
    if (it.cols.empty()) {
        return 0;  // 如果表不存在或没有列，返回0
    }
    return it.cols.size();
}

/**
 * @brief 使用贪心算法优化多表连接顺序
 */
std::shared_ptr<Plan> Planner::make_one_rel_optimized(std::shared_ptr<Query> query) {
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    std::vector<std::string> tables = query->tables;
    // 如果只有一个表，直接生成扫描计划
    if (tables.size() == 1) {
        auto curr_conds = pop_conds(query->conds, tables[0]);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(tables[0], curr_conds, index_col_names);
        if (index_exist == false) {
            return std::make_shared<ScanPlan>(PlanTag::T_SeqScan, sm_manager_, tables[0], curr_conds, index_col_names);
        } else {
            return std::make_shared<ScanPlan>(PlanTag::T_IndexScan, sm_manager_, tables[0], curr_conds,
                                              index_col_names);
        }
    }
    std::vector<JoinNode> jointree = std::move(query->jointree);
    for (auto &join_node : jointree) {
        // 将连接条件添加到conds中
        for (auto &cond : join_node.join_conds) {
            query->conds.emplace_back(std::move(cond));
        }
    }
    // 谓词下推
    std::vector<std::pair<std::shared_ptr<Plan>, size_t>> table_plans_with_cardinality(tables.size());
    for (size_t i = 0; i < tables.size(); i++) {
        auto curr_conds = pop_conds(query->conds, tables[i]);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(tables[i], curr_conds, index_col_names);
        size_t cardinality = get_table_cardinality(tables[i]);
        std::shared_ptr<Plan> scan_plan;
        if (index_exist == false) {  // 该表没有索引
            index_col_names.clear();
            scan_plan =
                std::make_shared<ScanPlan>(PlanTag::T_SeqScan, sm_manager_, tables[i], curr_conds, index_col_names);
        } else {  // 存在索引
            scan_plan =
                std::make_shared<ScanPlan>(PlanTag::T_IndexScan, sm_manager_, tables[i], curr_conds, index_col_names);
        }
        table_plans_with_cardinality[i] = {std::move(scan_plan), cardinality};
    }

    // 获取where条件
    auto conds = std::move(query->conds);
    std::shared_ptr<Plan> table_join_executors;
    // 对表扫描计划按基数排序
    // 这里使用贪心算法，选择基数最小的表开始连接
    std::sort(table_plans_with_cardinality.begin(), table_plans_with_cardinality.end(),
              [](const auto &a, const auto &b) { return a.second < b.second; });
    std::vector<std::shared_ptr<Plan>> sorted_table_plans;
    sorted_table_plans.reserve(tables.size());
    for (const auto &pair : table_plans_with_cardinality) {
        sorted_table_plans.push_back(pair.first);
    }
    // 构建左深树连接计划
    return build_left_deep_join_tree(sorted_table_plans, conds);
}

/**
 * @brief 构建左深树连接计划
 */
std::shared_ptr<Plan> Planner::build_left_deep_join_tree(std::vector<std::shared_ptr<Plan>> &table_plans,
                                                         std::vector<Condition> &join_conditions) {
    // 开始构建左深树
    std::shared_ptr<Plan> result = nullptr;
    std::set<std::string> joined_tables;
    // 获取第一个表的名称
    std::shared_ptr<ScanPlan> first_scan = std::dynamic_pointer_cast<ScanPlan>(table_plans[0]);
    joined_tables.insert(first_scan->tab_name_);
    std::vector<std::string> now_tables;

    result = table_plans[0];
    now_tables.push_back(first_scan->tab_name_);
    table_plans.erase(table_plans.begin());

    auto join_table = [&](std::string current_table, size_t index) -> void {
        now_tables.push_back(current_table);
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
                applicable_conds.push_back(*it);
                it = join_conditions.erase(it);
            } else {
                ++it;
            }
        }
        // 创建连接计划
        if (enable_nestedloop_join && enable_sortmerge_join) {
            result = std::make_shared<JoinPlan>(PlanTag::T_NestLoop, std::move(result), table_plans[index],
                                                applicable_conds, now_tables);
        } else if (enable_nestedloop_join) {
            result = std::make_shared<JoinPlan>(PlanTag::T_NestLoop, std::move(result), table_plans[index],
                                                applicable_conds, now_tables);
        } else if (enable_sortmerge_join) {
            result = std::make_shared<JoinPlan>(PlanTag::T_SortMerge, std::move(result), table_plans[index],
                                                applicable_conds, now_tables);
        } else {
            throw RMDBError("No join executor selected!");
        }
        table_plans.erase(table_plans.begin() + index);
        joined_tables.insert(current_table);
    };

    // 先连接基数最小的两个表
    first_scan = std::dynamic_pointer_cast<ScanPlan>(table_plans[0]);
    join_table(first_scan->tab_name_, 0);

    while (!table_plans.empty()) {
        bool flag = false;
        for (size_t i = 0; i < table_plans.size(); i++) {
            std::shared_ptr<ScanPlan> current_scan = std::dynamic_pointer_cast<ScanPlan>(table_plans[i]);
            std::string current_table = current_scan->tab_name_;

            // 检查当前表是否已经连接
            if (joined_tables.count(current_table) > 0) {
                table_plans.erase(table_plans.begin() + i);
                flag = true;
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
                join_table(current_table, i);
                flag = true;
                break;
            }
        }
        if (!flag) {
            while (!table_plans.empty()) {
                std::shared_ptr<ScanPlan> current_scan;
                if (auto x = std::dynamic_pointer_cast<ScanPlan>(table_plans[0])) {
                    current_scan = x;
                } else {
                    auto y = std::dynamic_pointer_cast<ProjectionPlan>(table_plans[0]);
                    current_scan = std::dynamic_pointer_cast<ScanPlan>(y->subplan_);
                }
                std::string current_table = current_scan->tab_name_;
                join_table(current_table, 0);
            }
        }
    }

    // 处理剩余的连接条件（如果有的话理论来说不会有）
    if (!join_conditions.empty()) {
        // 将剩余条件下推到结果计划中
        for (auto &cond : join_conditions) {
            push_conds(&cond, result);
        }
        join_conditions.clear();
    }

    return result;
}
std::shared_ptr<Plan> Planner::build_projection_plan(std::shared_ptr<Plan> plan, std::vector<TabCol> &need_cols,
                                                     std::vector<TabCol> &all_cols) {
    size_t siz = need_cols.size();
    all_cols.clear();
    if (auto x = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
        // 拿到需要投影到列
        for (auto &col : x->sel_cols_) {
            // 检查是否已经存在
            if (std::find(need_cols.begin(), need_cols.end(), col) == need_cols.end()) {
                need_cols.emplace_back(col);
            }
        }
        std::vector<TabCol> temp;
        build_projection_plan(x->subplan_, need_cols, temp);
        while (need_cols.size() > siz) {
            need_cols.pop_back();
        }
        return plan;
    } else if (auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
        for (auto &cond : x->conds_) {
            if (std::find(need_cols.begin(), need_cols.end(), cond.lhs_col) == need_cols.end()) {
                need_cols.emplace_back(cond.lhs_col);
            }
            if (std::find(need_cols.begin(), need_cols.end(), cond.rhs_col) == need_cols.end()) {
                need_cols.emplace_back(cond.rhs_col);
            }
        }
        std::vector<TabCol> left, right;
        x->left_ = build_projection_plan(x->left_, need_cols, left);
        x->right_ = build_projection_plan(x->right_, need_cols, right);
        left.insert(left.end(), right.begin(), right.end());
        sort(left.begin(), left.end());
        left.erase(std::unique(left.begin(), left.end()), left.end());
        while (need_cols.size() > siz) {
            need_cols.pop_back();
        }
        bool ok = false;
        for (auto col : left) {
            if (std::find(need_cols.begin(), need_cols.end(), col) == need_cols.end()) {
                ok = true;
            } else {
                all_cols.push_back(col);
            }
        }
        if (ok) {
            plan = std::make_shared<ProjectionPlan>(PlanTag::T_Projection, std::move(plan), all_cols);
        }
        return plan;
    } else if (auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
        for (auto &cond : x->conds_) {
            if (std::find(need_cols.begin(), need_cols.end(), cond.lhs_col) == need_cols.end()) {
                need_cols.emplace_back(cond.lhs_col);
            }
            if (std::find(need_cols.begin(), need_cols.end(), cond.rhs_col) == need_cols.end()) {
                need_cols.emplace_back(cond.rhs_col);
            }
        }
        while (need_cols.size() > siz) {
            need_cols.pop_back();
        }
        int cnt = 0;
        for (auto &col : need_cols) {
            if (col.tab_name.compare(x->tab_name_) == 0) {
                cnt++;
                all_cols.emplace_back(col);
            }
        }
        if (cnt != get_table_col_num(x->tab_name_)) {
            plan = std::make_shared<ProjectionPlan>(PlanTag::T_Projection, std::move(plan), all_cols);
        }
        return plan;
    }
}