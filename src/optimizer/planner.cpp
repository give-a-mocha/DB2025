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

/**
 * @brief 为指定表选择最优索引（实现最左匹配原则）
 *
 * 根据查询条件为表选择最适合的索引，应用最左匹配原则来确定索引的可用性。
 * 最左匹配原则：复合索引只有从最左边的列开始连续匹配才能使用索引。
 *
 * 算法流程：
 * 1. 从查询条件中提取涉及目标表的列
 * 2. 支持等值条件和范围条件
 * 3. 遍历表的所有索引，找到连续匹配列数最多的索引
 * 4. 应用最左匹配原则：如果某列不匹配则停止匹配
 *
 * @param tab_name 目标表名
 * @param curr_conds 当前查询条件列表
 * @param index_col_names 输出参数，返回选中索引的列名列表
 * @return true表示找到可用索引，false表示无可用索引
 *
 * @example
 * 对于索引(a,b,c)和条件"WHERE a=1 AND c=3"，只能使用索引的a列部分
 * 对于索引(a,b,c)和条件"WHERE a=1 AND b>2"，可以使用索引的a,b列部分
 */
bool Planner::get_index_cols(std::string tab_name, std::vector<Condition> curr_conds,
                             std::vector<std::string> &index_col_names) {
    // 初始代码，完全一致匹配原则
    // index_col_names.clear();
    // for(auto& cond: curr_conds) {
    //     if(cond.is_rhs_val && cond.op == OP_EQ && cond.lhs_col.tab_name.compare(tab_name) == 0)
    //         index_col_names.emplace_back(cond.lhs_col.col_name);
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
            index_col_names.emplace_back(ans->cols[i].name);
        }
        return true;
    }
    return false;
}

/**
 * @brief 条件下推：提取适用于单表的扫描条件
 *
 * 从全局条件列表中分离出可以应用于指定表的单表条件，实现条件下推优化。
 * 条件下推是查询优化的重要技术，能够尽早过滤数据，减少后续操作的数据量。
 *
 * 提取的条件类型：
 * 1. 单表条件：table.column op value（如 students.age > 20）
 * 2. 自连接条件：table.col1 op table.col2（如 emp.salary > emp.min_salary）
 *
 * 算法逻辑：
 * - 遍历所有条件，识别涉及目标表的条件
 * - 将匹配的条件从原列表中移除并返回
 * - 保留不匹配的条件在原列表中供后续处理
 *
 * @param conds 输入输出参数，全局条件列表，匹配的条件会被移除
 * @param tab_names 目标表名
 * @return 适用于该表的条件列表
 *
 * @note 此函数会修改输入的conds参数，移除已提取的条件
 *
 * @example
 * 输入条件：[students.age > 20, students.name = 'John', courses.credit >= 3]
 * 对表"students"调用后：
 * - 返回：[students.age > 20, students.name = 'John']
 * - conds剩余：[courses.credit >= 3]
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
            // 需要将左右两边的条件变换位置
            std::function<CompOp(CompOp)> swap_op = [](CompOp op) {
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
            };
            std::swap(cond->lhs_col, cond->rhs_col);
            cond->op = swap_op(cond->op);
        }
        x->conds_.emplace_back(std::move(*cond));
        return 3;
    }
    return false;
}

/**
 * @brief 从扫描计划列表中查找并提取指定表的扫描计划
 *
 * 在多表连接的计划构建过程中，此函数用于查找特定表的扫描计划并将其标记为已使用。
 * 这是连接计划构建算法的重要组成部分，确保每个表的扫描计划只被使用一次。
 *
 * 算法流程：
 * 1. 遍历所有候选的扫描计划
 * 2. 通过动态类型转换识别ScanPlan节点
 * 3. 比较表名找到匹配的扫描计划
 * 4. 标记该计划为已使用，并记录到已连接表列表中
 *
 * @param scantbl 扫描表标记数组，标记哪些扫描计划已被使用（1=已使用，-1=未使用）
 *                数组大小应与plans向量大小相同
 * @param table 要查找的目标表名
 * @param joined_tables 已连接表名列表，找到的表会被添加到此列表中
 * @param plans 候选扫描计划列表，包含所有表的扫描计划
 * @return 匹配的扫描计划共享指针，如果未找到则返回nullptr
 *
 * @note 此函数会修改scantbl数组和joined_tables向量的状态
 *
 * @example
 * 对于连接条件"students.id = enrollments.student_id"：
 * - 调用pop_scan查找students表的扫描计划
 * - 调用pop_scan查找enrollments表的扫描计划
 * - 两个扫描计划将用于构建连接计划
 */
std::shared_ptr<Plan> pop_scan(int *scantbl, std::string table, std::vector<std::string> &joined_tables,
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

/**
 * @brief 逻辑优化：应用与具体执行算法无关的优化规则
 *
 * 逻辑优化阶段专注于查询的逻辑结构优化，不涉及具体的物理执行算法选择。
 * 主要目标是减少查询处理的数据量和计算复杂度。
 *
 * 当前实现的优化规则：
 * 1. 条件下推：通过pop_conds函数将单表条件下推到扫描算子
 *
 * 计划中的优化规则（TODO）：
 * 2. 常量折叠：预计算常量表达式（如1+2替换为3）
 * 3. 谓词简化：去除恒真（1=1）或恒假（1=0）条件
 * 4. 连接重排序：基于表大小重新安排连接顺序（需要统计信息支持）
 * 5. 子查询优化：将相关子查询转换为连接
 * 6. 视图展开：将视图定义内联到查询中
 *
 * @param query 待优化的查询对象
 * @param context 查询执行上下文
 * @return 优化后的查询对象
 *
 * @note 当前版本主要通过条件下推进行优化，更多规则有待实现
 * @see make_one_rel() 实际的条件下推在该函数中通过pop_conds实现
 */
std::shared_ptr<Query> Planner::logical_optimization(std::shared_ptr<Query> query, Context *context) {
    // TODO 实现逻辑优化规则

    // 1. 条件下推优化：将单表条件尽早执行
    // 这里已经在make_one_rel中通过pop_conds实现了条件下推

    // 2. 常量折叠：预计算常量表达式
    // 暂时跳过，因为当前系统不支持复杂表达式

    // 3. 谓词简化：去除恒真或恒假条件
    // auto it = query->conds.begin();
    // while (it != query->conds.end()) {
    //     // 检查是否是恒真或恒假条件（如 1=1 或 1=0）
    //     if (it->is_rhs_val && it->lhs_col.tab_name.empty() && it->lhs_col.col_name.empty()) {
    //         // 这是一个常量比较，应该在语法分析阶段就处理了
    //         it = query->conds.erase(it);
    //     } else {
    //         ++it;
    //     }
    // }

    // 4. 连接重排序：将小表放在外层（暂时跳过，需要统计信息）

    return query;
}

/**
 * @brief 物理优化：选择具体的执行算法和访问路径
 *
 * 物理优化阶段在逻辑优化的基础上，为查询选择具体的执行算法和数据访问方法。
 * 这个阶段的决策直接影响查询的执行性能。
 *
 * 优化步骤：
 * 1. 基础计划生成：调用make_one_rel生成扫描和连接的基础计划
 * 2. 索引选择：通过get_index_cols选择最优索引
 * 3. 连接算法选择：在嵌套循环连接和排序合并连接间选择
 * 4. 分组计划：处理GROUP BY子句
 * 5. 聚合计划：处理聚合函数
 * 6. 排序计划：处理ORDER BY子句
 *
 * 当前实现的优化：
 * - 索引选择（支持最左匹配原则）
 * - 连接算法选择（支持配置开关）
 * - 完整的SQL子句处理流水线
 *
 * 未来可扩展的优化：
 * - 投影下推：将投影操作下推到扫描算子以减少数据传输
 * - 基于成本的连接顺序优化（需要统计信息）
 * - 并行执行计划生成
 *
 * @param query 经过逻辑优化的查询对象
 * @param context 查询执行上下文
 * @return 完整的物理执行计划
 *
 * @see make_one_rel() 核心的扫描和连接计划生成
 * @see get_index_cols() 索引选择算法
 */
std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query, Context *context) {
    // 生成基础的扫描和连接计划
    std::shared_ptr<Plan> plan = make_one_rel(query);

    // 其他物理优化

    // 1. 索引选择优化：在make_one_rel中已经通过get_index_cols实现

    // 2. 连接算法选择：已经在make_one_rel中通过enable_nestedloop_join和enable_sortmerge_join实现

    // 3. 投影下推：尽早减少数据传输量（这里可以在未来实现）
    // 目前投影在最顶层进行，可以考虑将投影下推到扫描算子中

    // 4. 连接顺序优化：基于成本的连接顺序选择（需要统计信息支持）
    // 当前使用启发式规则：按照条件中出现的表的顺序进行连接

    // 检查是否有GROUP BY子句
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    plan = generate_group_plan(query, std::move(plan));
    plan = generate_aggregate_plan(query, std::move(plan));
    // 处理ORDER BY子句
    plan = generate_sort_plan(query, std::move(plan));

    return plan;
}

/**
 * @brief 核心函数：构建查询的基础扫描和连接计划
 *
 * 这是查询优化器的核心函数，负责将查询中的所有表转换为扫描计划，
 * 并根据连接条件构建连接计划树。整个过程分为两个主要阶段：
 *
 * 第一阶段 - 单表扫描计划生成：
 * 1. 为每个表提取适用的单表条件（条件下推）
 * 2. 根据条件和可用索引选择访问路径（顺序扫描 vs 索引扫描）
 * 3. 生成对应的ScanPlan节点
 *
 * 第二阶段 - 多表连接计划生成：
 * 1. 处理连接条件，按顺序构建连接层次
 * 2. 选择连接算法（嵌套循环 vs 排序合并）
 * 3. 处理剩余没有连接条件的表（笛卡尔积）
 *
 * 算法特点：
 * - 应用条件下推优化，尽早过滤数据
 * - 支持索引选择，提高数据访问效率
 * - 支持多种连接算法，适应不同场景
 * - 处理复杂的多表连接查询
 *
 * @param query 查询对象，包含表列表、条件列表等信息
 * @return 完整的扫描和连接计划树，单表查询返回ScanPlan，多表查询返回JoinPlan
 *
 * @note 对于n个表的连接，算法复杂度为O(n²)，适合中小规模的多表连接
 * @see get_index_cols() 索引选择算法
 * @see pop_conds() 条件下推实现
 * @see push_conds() 连接条件处理
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
    std::shared_ptr<Plan> table_join_executors;

    int scantbl[tables.size()];
    for (size_t i = 0; i < tables.size(); i++) {
        scantbl[i] = -1;
    }
    // 假设在ast中已经添加了jointree，这里需要修改的逻辑是，先处理jointree，然后再考虑剩下的部分
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
                    std::function<CompOp(CompOp)> swap_op = [](CompOp op) {
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
                    };
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
    if (!x->has_sort || x->order.empty()) {
        return plan;
    }
    std::vector<std::string> tables = query->tables;
    std::vector<ColMeta> all_cols;
    for (auto &sel_tab_name : tables) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }

    std::vector<TabCol> sel_cols;
    std::vector<bool> is_desc;

    for (auto &order_by : x->order) {
        TabCol sel_col;
        for (auto &col : all_cols) {
            if (col.name.compare(order_by->cols->col_name) == 0) {
                sel_col = {.tab_name = col.tab_name, .col_name = col.name};
                break;
            }
        }
        sel_cols.emplace_back(sel_col);
        is_desc.emplace_back(order_by->orderby_dir == ast::OrderBy_DESC);
    }

    return std::make_shared<SortPlan>(PlanTag::T_Sort, std::move(plan), sel_cols, is_desc);
}

/**
 * @brief SELECT查询计划生成的主入口函数
 *
 * 这是SELECT语句查询计划生成的主控函数，协调逻辑优化和物理优化两个阶段，
 * 最终生成完整的查询执行计划。
 *
 * 处理流程：
 * 1. 逻辑优化：应用与算法无关的优化规则（条件下推、谓词简化等）
 * 2. 物理优化：生成具体的执行计划（扫描、连接、排序、聚合等）
 * 3. 投影处理：在计划树顶层添加投影节点，选择最终输出的列
 *
 * 生成的计划树结构（从顶到底）：
 * ProjectionPlan (选择输出列)
 * └── SortPlan (ORDER BY，可选)
 *     └── AggregatePlan (聚合函数，可选)
 *         └── GroupPlan (GROUP BY，可选)
 *             └── JoinPlan/ScanPlan (连接/扫描)
 *
 * @param query 查询对象，包含SELECT语句的所有信息
 * @param context 查询执行上下文
 * @return 完整的SELECT查询执行计划，顶层为ProjectionPlan
 *
 * @note 投影节点总是位于计划树的最顶层，确保输出格式符合SELECT子句要求
 * @see logical_optimization() 逻辑优化实现
 * @see physical_optimization() 物理优化实现
 */
std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query, Context *context) {
    // 逻辑优化
    query = logical_optimization(std::move(query), context);

    // 物理优化
    auto sel_cols = query->cols;
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);
    plannerRoot = std::make_shared<ProjectionPlan>(PlanTag::T_Projection, std::move(plannerRoot), std::move(sel_cols));

    return plannerRoot;
}

std::shared_ptr<Plan> Planner::generate_aggregate_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan) {
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);

    // 检查query->cols中是否有聚合函数
    bool has_aggregate = std::any_of(query->cols.begin(), query->cols.end(),
                                     [](const TabCol &col) { return col.aggregate != AggregateType::NONE; });

    if (!has_aggregate) {
        return plan;  // 没有聚合函数，直接返回原计划
    }

    return std::make_shared<AggregatePlan>(PlanTag::T_Aggregate, std::move(plan), query->cols);
}

std::shared_ptr<Plan> Planner::generate_group_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan) {
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    if (x->group.empty()) {
        return plan;
    }
    return std::make_shared<GroupPlan>(PlanTag::T_Group, std::move(plan), query->cols, query->group_cols,
                                       query->having_conds);
}

/**
 * @brief 查询计划生成的总入口函数
 *
 * 这是RMDB查询优化器的主入口函数，负责将各种类型的SQL语句转换为对应的执行计划。
 * 根据AST节点的类型，分发到不同的计划生成逻辑。
 *
 * 支持的SQL语句类型：
 *
 * DDL语句（数据定义语言）：
 * - CREATE TABLE: 创建表结构
 * - DROP TABLE: 删除表
 * - CREATE INDEX: 创建索引
 * - DROP INDEX: 删除索引
 *
 * DML语句（数据操作语言）：
 * - INSERT: 插入数据，直接生成DMLPlan
 * - UPDATE: 更新数据，包含扫描计划和更新逻辑
 * - DELETE: 删除数据，包含扫描计划和删除逻辑
 * - SELECT: 查询数据，调用复杂的查询优化流程
 *
 * 处理策略：
 * - 简单操作（INSERT）：直接构造对应的计划节点
 * - 复杂操作（SELECT）：调用完整的查询优化流程
 * - 需要扫描的操作（UPDATE/DELETE）：先生成扫描计划，再包装为DML计划
 *
 * @param query 查询对象，包含解析后的AST和相关信息
 * @param context 查询执行上下文，包含事务信息等
 * @return 完整的执行计划，类型根据SQL语句类型而定
 *
 * @note 这个函数是整个查询优化器的协调中心，连接语法分析和执行引擎
 * @see generate_select_plan() SELECT查询的专门处理
 * @see get_index_cols() 索引选择逻辑
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
                col_defs.emplace_back(col_def);
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
    } else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse)) {
        std::shared_ptr<plannerInfo> root = std::make_shared<plannerInfo>(x);
        // 生成select语句的查询执行计划
        std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
        plannerRoot = std::make_shared<DMLPlan>(PlanTag::T_select, projection, std::string(), std::vector<Value>(),
                                                std::vector<Condition>(), std::vector<SetClause>());
    } else {
        throw InternalError("Unexpected AST root");
    }
    return plannerRoot;
}
