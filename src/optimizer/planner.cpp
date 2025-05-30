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
#include <set>

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
        if ((it.lhs_col.tab_name.compare(tab_names) == 0 && it.is_rhs_val) ||
            (it.lhs_col.tab_name.compare(it.rhs_col.tab_name) == 0)) {
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
    if (query == nullptr)
        return nullptr;
    
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
            } else if(it->op == CompOp::OP_NE){
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
        for (const auto& col : query->cols) {
            if (!col.tab_name.empty()) {
                used_tables.insert(col.tab_name);
            }
        }
        
        // 从条件中收集表名
        for (const auto& cond : query->conds) {
            if (!cond.lhs_col.tab_name.empty()) {
                used_tables.insert(cond.lhs_col.tab_name);
            }
            if (!cond.is_rhs_val && !cond.rhs_col.tab_name.empty()) {
                used_tables.insert(cond.rhs_col.tab_name);
            }
        }

        std::remove_if(query->tables.begin(), query->tables.end(), [&used_tables](const std::string& table) {
            return used_tables.count(table);
        });
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
    //scanPlan JoinPlan
    std::shared_ptr<Plan> plan = make_one_rel(query);

    // 其他物理优化

    // 1. 索引选择优化：在make_one_rel中已经通过get_index_cols实现

    // 2. 连接算法选择：已经在make_one_rel中通过enable_nestedloop_join和enable_sortmerge_join实现

    // 3. 投影下推：尽早减少数据传输量（这里可以在未来实现）
    // 目前投影在最顶层进行，可以考虑将投影下推到扫描算子中

    // 4. 连接顺序优化：基于成本的连接顺序选择（需要统计信息支持）
    // 当前使用启发式规则：按照条件中出现的表的顺序进行连接

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
    for(auto &join_node : jointree) {
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
    //SELECT * FROM A, B, C WHERE A.id = B.id AND B.id = C.id AND A.name = C.name
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
        if (col.name.compare(x->order->cols->col_name) == 0) sel_col = {.tab_name = col.tab_name, .col_name = col.name};
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
    //joinPlan Or scanPlan Or sortPlan
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);
    // if(auto x = std::dynamic_pointer_cast<ScanPlan>(plannerRoot)) {
    //     std::cerr<< "这是一个ScanPlan: " << x->tab_name_ << std::endl;
    // }else if(auto x = std::dynamic_pointer_cast<JoinPlan>(plannerRoot)) {
    //     std::cerr<< "这是一个JoinPlan:" << std::endl;
    // }else if(auto x = std::dynamic_pointer_cast<SortPlan>(plannerRoot)) {
    //     std::cerr<< "这是一个SortPlan: " << std::endl;
    // }
    plannerRoot = std::make_shared<ProjectionPlan>(PlanTag::T_Projection, std::move(plannerRoot), std::move(sel_cols));

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
    } else if (auto x = std::dynamic_pointer_cast<ast::ExplainStmt>(query->parse)){
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
