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
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include "analyze/analyze.h"
#include "common/common.h"
#include "common/context.h"
#include "execution/execution_defs.h"
#include "execution/execution_manager.h"
#include "parser/parser.h"
#include "plan.h"
#include "record/rm.h"
#include "system/sm.h"

class Planner {
   private:
    SmManager *sm_manager_;

    bool enable_nestedloop_join = true;
    bool enable_sortmerge_join = false;

   public:
    /**
     * @brief Planner 类的构造函数。
     * @param sm_manager 系统管理器对象的指针。
     */
    Planner(SmManager *sm_manager) : sm_manager_(sm_manager) {}

    /**
     * @brief 为给定的查询生成执行计划。
     * @param query 要执行的查询对象。
     * @param context 当前查询的上下文。
     * @return 生成的执行计划。
     */
    std::shared_ptr<Plan> do_planner(std::shared_ptr<Query> query, Context *context);

    /**
     * @brief 设置是否启用嵌套循环连接。
     * @param set_val true 表示启用，false 表示禁用。
     */
    void set_enable_nestedloop_join(bool set_val) { enable_nestedloop_join = set_val; }

    /**
     * @brief 设置是否启用排序合并连接。
     * @param set_val true 表示启用，false 表示禁用。
     */
    void set_enable_sortmerge_join(bool set_val) { enable_sortmerge_join = set_val; }

   private:
    /**
     * @brief 对查询进行逻辑优化。
     * @param query 要优化的查询对象。
     * @param context 当前查询的上下文。
     * @return 优化后的查询对象。
     */
    std::shared_ptr<Query> logical_optimization(std::shared_ptr<Query> query, Context *context);
    /**
     * @brief 对查询进行物理优化。
     * @param query 要优化的查询对象。
     * @param context 当前查询的上下文。
     * @return 生成的物理执行计划。
     */
    std::shared_ptr<Plan> physical_optimization(std::shared_ptr<Query> query, Context *context);

    /**
     * @brief 为单关系查询（或多关系查询的单个表部分）创建扫描计划。
     * @param query 查询对象。
     * @return 生成的扫描计划或基础连接计划。
     */
    std::shared_ptr<Plan> make_one_rel(std::shared_ptr<Query> query);

    /**
     * @brief 如果查询中包含 ORDER BY 子句，则生成排序计划。
     * @param query 查询对象。
     * @param plan 当前的执行计划。
     * @return 如果需要排序，则返回排序计划；否则返回原始计划。
     */
    std::shared_ptr<Plan> generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);

    /**
     * @brief 为 SELECT 语句生成完整的查询计划。
     * @param query SELECT 查询对象。
     * @param context 当前查询的上下文。
     * @return 生成的 SELECT 查询计划。
     */
    std::shared_ptr<Plan> generate_select_plan(std::shared_ptr<Query> query, Context *context);

    // int get_indexNo(std::string tab_name, std::vector<Condition> curr_conds);

    /**
     * @brief 根据当前条件获取用于索引扫描的列名。
     * @param tab_name 表名。
     * @param curr_conds 当前应用于该表的条件。
     * @param index_col_names 输出参数，用于存储索引列的名称。
     * @return 如果找到了合适的索引列，则返回 true；否则返回 false。
     */
    bool get_index_cols(std::string tab_name, std::vector<Condition> curr_conds,
                        std::vector<std::string> &index_col_names);
    /**
     * @brief 获取表的列数
     * @param tab_name 表名
     * @return 表的列数
     */
    int get_table_col_num(const std::string &tab_name);
    /**
     * @brief 获取表的基数（记录数量）
     * @param tab_name 表名
     * @return 表中记录的数量
     */
    size_t get_table_cardinality(const std::string &tab_name);

    /**
     * @brief 使用贪心算法优化多表连接顺序
     * @param query 查询对象
     * @return 优化后的查询计划
     */
    std::shared_ptr<Plan> make_one_rel_optimized(std::shared_ptr<Query> query);

    std::shared_ptr<Plan> build_projection_plan(std::shared_ptr<Plan> plan, std::vector<TabCol> &need_cols,
                                                std::vector<TabCol> &all_cols);
    /**
     * @brief 构建左深树连接计划
     * @param table_plans 表扫描计划列表
     * @param join_conditions 连接条件
     * @return 连接计划
     */
    std::shared_ptr<Plan> build_left_deep_join_tree(std::vector<std::shared_ptr<Plan>> &table_plans,
                                                    std::vector<Condition> &join_conditions);

    /**
     * @brief 将 AST 中的数据类型转换为系统内部的列类型。
     * @param sv_type AST 中的数据类型。
     * @return 系统内部的列类型。
     */
    ColType interp_sv_type(ast::SvType sv_type) {
        switch (sv_type) {
            case ast::SV_TYPE_INT:
                return ColType::TYPE_INT;
            case ast::SV_TYPE_FLOAT:
                return ColType::TYPE_FLOAT;
            case ast::SV_TYPE_STRING:
                return ColType::TYPE_STRING;
            default:
                throw InternalError("Unsupported sv_type: " + std::to_string(sv_type));
        }
    }
};
