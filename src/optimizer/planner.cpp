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

// 实现最左匹配原则的索引匹配规则
/**
 * @brief 根据查询条件选择最优索引
 * @param tab_name 表名
 * @param curr_conds 当前的查询条件
 * @param index_col_names 输出参数，存储选中的索引列名
 * @return 是否找到可用索引
 *
 * @details 索引选择策略：
 * 1. 支持的条件类型
 *    - 等值条件(=)
 *    - 范围条件(>, >=, <, <=)
 *
 * 2. 最左前缀匹配原则
 *    - 按索引定义顺序匹配列
 *    - 连续匹配直到不满足条件
 *    - 选择匹配列数最多的索引
 *
 * 3. 选择标准
 *    - 优先选择匹配列数最多的索引
 *    - 对于相同匹配数的索引，选择第一个
 */
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
/**
 * @brief 提取适用于特定表的条件
 * @param conds 所有条件列表
 * @param tab_names 目标表名
 * @return 提取出的适用条件
 *
 * @details 条件提取规则：
 * 1. 单表条件
 *    - 条件左侧列属于目标表
 *    - 右侧为常量值或同表的列
 *
 * 2. 条件处理
 *    - 从原条件列表中移除提取的条件
 *    - 保持其他条件不变
 *    - 返回提取出的条件列表
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
/**
 * @brief 将条件下推到查询计划树中
 * @param cond 要下推的条件指针
 * @param plan 当前计划节点
 * @return 条件处理状态码
 *
 * @details 返回值说明：
 * - 0: 条件与当前节点无关
 * - 1: 条件左侧列匹配
 * - 2: 条件右侧列匹配
 * - 3: 条件已完全处理
 *
 * @details 下推策略：
 * 1. 扫描节点(ScanPlan)
 *    - 检查条件列是否属于扫描表
 *    - 确定匹配状态
 *
 * 2. 连接节点(JoinPlan)
 *    - 递归下推到左右子树
 *    - 处理跨表的连接条件
 *    - 必要时交换条件的左右操作数
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
/**
 * @brief 从计划列表中提取指定表的扫描计划
 * @param scantbl 扫描表状态标记数组
 * @param table 目标表名
 * @param joined_tables 已连接表列表
 * @param plans 候选计划列表
 * @return 找到的扫描计划，如果未找到返回nullptr
 *
 * @details 处理逻辑：
 * 1. 遍历计划列表查找匹配表名的扫描计划
 * 2. 找到后标记该计划已使用
 * 3. 将表名添加到已连接表列表
 * 4. 返回找到的计划节点
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

    // // 3. 冗余表消除：删除不需要的表（没有在结果或条件中使用的表）
    // if (query->tables.size() > 1) {
    //     std::set<std::string> used_tables;

    //     // 从结果列中收集表名
    //     for (const auto &col : query->cols) {
    //         if (!col.tab_name.empty()) {
    //             used_tables.insert(col.tab_name);
    //         }
    //     }

    //     // 从条件中收集表名
    //     for (const auto &cond : query->conds) {
    //         if (!cond.lhs_col.tab_name.empty()) {
    //             used_tables.insert(cond.lhs_col.tab_name);
    //         }
    //         if (!cond.is_rhs_val && !cond.rhs_col.tab_name.empty()) {
    //             used_tables.insert(cond.rhs_col.tab_name);
    //         }
    //     }

    //     std::remove_if(query->tables.begin(), query->tables.end(),
    //                    [&used_tables](const std::string &table) { return used_tables.count(table); });
    // }

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
                push_conds(&(*it), table_join_executors);
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

/**
 * @brief 生成排序执行计划
 * @param query 待排序的查询
 * @param plan 原始执行计划
 * @return 添加排序操作后的执行计划
 *
 * @details 排序处理流程：
 * 1. 检查排序需求
 *    - 验证是否包含ORDER BY子句
 *    - 如果没有排序要求直接返回原计划
 *
 * 2. 排序列处理
 *    - 收集所有表的列信息
 *    - 确定排序列的表和列名
 *    - 处理列的歧义性
 *
 * 3. 创建排序计划
 *    - 构造SortPlan节点
 *    - 设置排序方向(ASC/DESC)
 *    - 连接原有计划
 */
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
    // query = logical_optimization(std::move(query), context);

    // 物理优化
    auto sel_cols = query->cols;
    // joinPlan Or scanPlan Or sortPlan
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);
    bool is_star = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse)->cols.empty();
    
    // 保证投影列不重复
    std::vector<TabCol> project_cols;
    std::vector<TabCol> temp2;
    for (auto &col : sel_cols) {
        if (std::find(project_cols.begin(), project_cols.end(), col) == project_cols.end()) {
            project_cols.emplace_back(col);
        }
    }
    plannerRoot = build_projection_plan(std::move(plannerRoot), project_cols, temp2);
    if(auto x = std::dynamic_pointer_cast<ProjectionPlan>(plannerRoot)) {
        plannerRoot = std::make_shared<ProjectionPlan>(PlanTag::T_Projection, std::move(x->subplan_), std::move(sel_cols),is_star);
    }else{
        plannerRoot = std::make_shared<ProjectionPlan>(PlanTag::T_Projection, std::move(plannerRoot), std::move(sel_cols),is_star);
    }
    return plannerRoot;
}

/**
 * @brief 生成DDL和DML语句的查询执行计划
 * @param query 查询对象
 * @param context 执行上下文
 * @return 生成的执行计划
 *
 * @details 支持的操作类型：
 * 1. DDL语句
 *    - CREATE TABLE: 创建新表
 *    - DROP TABLE: 删除表
 *    - CREATE INDEX: 创建索引
 *    - DROP INDEX: 删除索引
 *
 * 2. DML语句
 *    - INSERT: 插入数据
 *    - DELETE: 删除数据
 *    - UPDATE: 更新数据
 *    - SELECT: 查询数据
 *    - EXPLAIN: 解释执行计划
 *
 * @details 处理流程：
 * 1. 根据语句类型分派处理
 * 2. 收集必要的元数据
 * 3. 生成相应的执行计划
 * 4. 处理错误情况
 */
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
        //? 未使用的语句
        // auto root = std::make_shared<plannerInfo>(x);
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
/**
 * @brief 使用贪心算法优化的查询计划生成
 * @param query 查询对象
 * @return 优化后的查询执行计划
 *
 * @details 优化策略：
 * 1. 单表查询处理
 *    - 直接生成扫描计划
 *    - 选择合适的访问方法
 *
 * 2. 多表查询处理
 *    - 收集连接条件
 *    - 对每个表生成基础扫描计划
 *    - 计算表的基数(大小)
 *
 * 3. 贪心选择连接顺序
 *    - 按表大小排序
 *    - 优先选择小表
 *    - 构建左深连接树
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

    // 将JOIN连接条件添加到conds中
    for(auto &join_node : query->jointree) {
        for (auto &cond : join_node.join_conds) {
            query->conds.emplace_back(std::move(cond));
        }
    }
    query->jointree.clear();

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

    // 获取连接条件
    std::list<Condition> join_conditions;
    for(auto& cond : query->conds) {
        join_conditions.emplace_back(std::move(cond));
    }
    query->conds.clear();

    // 对表扫描计划按基数排序
    std::sort(table_plans_with_cardinality.begin(), table_plans_with_cardinality.end(),
              [](const auto& a, const auto& b) { return a.second < b.second; });
    
    std::list<std::shared_ptr<Plan>> tables_plans;
    for (auto& pair : table_plans_with_cardinality) {
        tables_plans.emplace_back(std::move(pair.first));
    }
    table_plans_with_cardinality.clear();

    //构建左深树连接计划
    return build_left_deep_join_tree(tables_plans, join_conditions);
}

/**
 * @brief 构建左深树连接计划
 * 这里使用贪心算法，选择基数最小的表开始连接
 */
std::shared_ptr<Plan> Planner::build_left_deep_join_tree(
    std::list<std::shared_ptr<Plan>>& table_plans,
    std::list<Condition>& join_conditions) {
    
    // 开始构建左深树
    std::shared_ptr<Plan> result = nullptr;
    std::unordered_set<std::string> joined_tables;
    std::vector<std::string> now_tables;
    
    
    // 获取第一个表作为初始结果
    std::shared_ptr<ScanPlan> first_scan = std::dynamic_pointer_cast<ScanPlan>(table_plans.front());
    joined_tables.insert(first_scan->tab_name_);
    result = table_plans.front();
    now_tables.push_back(first_scan->tab_name_);
    table_plans.pop_front();

    // 复杂度O(n)
    auto join_table = [&](const std::string &current_table, std::list<std::shared_ptr<Plan>>::iterator &table_it) -> void{
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
                applicable_conds.emplace_back(std::move(*it));
                it = join_conditions.erase(it);
            } else {
                ++it;
            }
        }
        // 创建连接计划
        if (enable_nestedloop_join && enable_sortmerge_join) {
            result = std::make_shared<JoinPlan>(PlanTag::T_NestLoop, std::move(result), std::move(*table_it), applicable_conds, now_tables);
        } else if (enable_nestedloop_join) {
            result = std::make_shared<JoinPlan>(PlanTag::T_NestLoop, std::move(result), std::move(*table_it), applicable_conds, now_tables);
        } else if (enable_sortmerge_join) {
            result = std::make_shared<JoinPlan>(PlanTag::T_SortMerge, std::move(result), std::move(*table_it), applicable_conds, now_tables);
        } else {
            throw RMDBError("No join executor selected!");
        }
        table_it = table_plans.erase(table_it);
        joined_tables.insert(current_table);
    };

    // 连接第二个表（基数第二小的表）
    first_scan = std::dynamic_pointer_cast<ScanPlan>(table_plans.front());
    
    {
        auto temp = table_plans.begin();
        join_table(first_scan->tab_name_, temp);
    }
    // 继续连接剩余的表
    while(!table_plans.empty()) {
        bool flag = false;
        auto it = table_plans.begin();
        while(it != table_plans.end()) {
            std::shared_ptr<ScanPlan> current_scan = std::dynamic_pointer_cast<ScanPlan>(*it);
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
                join_table(current_table, it);
                flag = true;
                break;
            }
        }
        // 如果没有找到可连接的表，强制连接剩余的第一个未使用表
        if(!flag) {
            auto temp = table_plans.begin();
            std::shared_ptr<ScanPlan> current_scan = std::dynamic_pointer_cast<ScanPlan>(*temp);
            join_table(current_scan->tab_name_, temp);
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
/**
 * @brief 构建投影计划
 * @param plan 输入的执行计划
 * @param need_cols 需要投影的列
 * @param all_cols 所有涉及的列
 * @return 优化后的执行计划
 *
 * @details 投影优化策略：
 * 1. ProjectionPlan节点处理
 *    - 收集需要的投影列
 *    - 递归处理子计划
 *    - 维护投影列大小
 *
 * 2. JoinPlan节点处理
 *    - 添加连接条件涉及的列
 *    - 分别处理左右子树
 *    - 合并左右子树的列
 *    - 去重和排序
 *
 * 3. ScanPlan节点处理
 *    - 添加条件中涉及的列
 *    - 检查表的列覆盖情况
 *    - 必要时创建新的投影计划
 */
std::shared_ptr<Plan> Planner::build_projection_plan(std::shared_ptr<Plan> plan, std::vector<TabCol> &need_cols,
                                                     std::vector<TabCol> &all_cols) {
    size_t siz = need_cols.size();
    all_cols.clear();
    if (auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
        for (auto &cond : x->conds_) {
            // 检查是否已经存在
            if (std::find(need_cols.begin(), need_cols.end(), cond.lhs_col) == need_cols.end()) {
                need_cols.emplace_back(cond.lhs_col);
            }
            if (std::find(need_cols.begin(), need_cols.end(), cond.rhs_col) == need_cols.end()) {
                need_cols.emplace_back(cond.rhs_col);
            }
        }
        std::vector<TabCol> left, right;
        x->left_ = build_projection_plan(std::move(x->left_), need_cols, left);
        x->right_ = build_projection_plan(std::move(x->right_), need_cols, right);
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
            return std::make_shared<ProjectionPlan>(PlanTag::T_Projection, std::move(plan), all_cols);
        }
        return x;
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
            return std::make_shared<ProjectionPlan>(PlanTag::T_Projection, std::move(plan), all_cols);
        }
        return x;
    } else if (auto x = std::dynamic_pointer_cast<SortPlan>(plan)) {
        //!目前SortPlan的排序只有一个
        if (std::find(need_cols.begin(), need_cols.end(), x->sel_col_) == need_cols.end()) {
            need_cols.emplace_back(x->sel_col_);
        }
        while(need_cols.size() > siz) {
            need_cols.pop_back();
        }
        x->subplan_ = build_projection_plan(std::move(x->subplan_), need_cols, all_cols);
        return x;
    } else {
        throw InternalError("Unexpected plan type in projection optimization");
    }
    
}