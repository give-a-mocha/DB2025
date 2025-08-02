/**
 * @file planner.h
 * @author RMDB Development Team
 * @brief 查询计划生成器的实现
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * 查询计划生成的主要功能：
 * 1. 查询优化
 *    - 逻辑优化：谓词下推、投影下推等
 *    - 物理优化：访问路径选择、连接顺序优化
 *
 * 2. 执行计划生成
 *    - 扫描算子：表扫描、索引扫描
 *    - 连接算子：嵌套循环连接、排序合并连接
 *    - 投影和排序算子
 *
 * 3. 代价估算
 *    - 基于统计信息
 *    - 考虑算子代价
 *    - 优化执行效率
 */

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

extern SmManager sm_manager;

/**
 * @brief 查询计划生成器类
 */
class Planner {
   private:
    bool enable_nestedloop_join = true;  // 是否启用嵌套循环连接
    bool enable_sortmerge_join = false;  // 是否启用排序合并连接

   public:
    /**
     * @brief Planner 类的构造函数。
     * @param sm_manager 系统管理器对象的指针。
     */
    Planner() = default;

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
    std::shared_ptr<Plan> physical_optimization(std::shared_ptr<Query> query, Context *context);

    std::shared_ptr<Plan> generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);

    std::shared_ptr<Plan> generate_group_and_aggregate_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);

    std::shared_ptr<Plan> generate_aggregate_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);

    std::shared_ptr<Plan> generate_group_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);

    std::shared_ptr<Plan> generate_select_plan(std::shared_ptr<Query> query, Context *context);

    bool get_index_cols(std::string tab_name, std::vector<Condition> curr_conds,
                        std::vector<std::string> &index_col_names);

    int get_table_col_num(const std::string &tab_name);

    size_t get_table_cardinality(const std::string &tab_name);

    std::shared_ptr<Plan> make_one_rel_optimized(std::shared_ptr<Query> query);

    std::shared_ptr<Plan> build_projection_plan_just_scan(std::shared_ptr<Plan> plan, std::vector<TabCol> &need_cols);

    std::shared_ptr<Plan> build_left_deep_join_tree(std::list<std::shared_ptr<Plan>> &table_plans,
                                                    std::list<Condition> &join_conditions);

    std::shared_ptr<Plan> add_join(std::shared_ptr<Plan> result, std::unordered_set<std::string> &joined_tables,
                                   std::list<std::shared_ptr<Plan>> &table_plans, std::list<Condition> &join_conditions,
                                   bool &flag);

    std::shared_ptr<Plan> join_tables(std::shared_ptr<Plan> result, const std::string &current_table,
                                      std::shared_ptr<Plan> current_scan,
                                      std::unordered_set<std::string> &joined_tables,
                                      std::list<Condition> &join_conditions, JoinType join_type);
    /**
     * @brief 将 AST 中的数据类型转换为系统内部的列类型。
     * @param sv_type AST 中的数据类型。
     * @return 系统内部的列类型。
     */
    /**
     * @brief 解释AST中的数据类型到系统内部类型
     * @param sv_type AST中的数据类型
     * @return 系统内部的列类型
     * @throw InternalError 当遇到不支持的类型时
     *
     * @details 支持的类型映射：
     * - SV_TYPE_INT -> TYPE_INT
     * - SV_TYPE_FLOAT -> TYPE_FLOAT
     * - SV_TYPE_STRING -> TYPE_STRING
     *
     * @note 确保类型转换的安全性和精度
     */
    ColType interp_sv_type(ast::SvType sv_type) {
        static ColType type_map[] = {
            ColType::TYPE_INT,
            ColType::TYPE_FLOAT,
            ColType::TYPE_STRING,
        };
        assert(sv_type >= ast::SvType::SV_TYPE_INT && sv_type <= ast::SvType::SV_TYPE_STRING);
        return type_map[static_cast<int>(sv_type)];
    }
};
