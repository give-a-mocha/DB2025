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

/**
 * @brief 查询计划生成器类
 *
 * 职责：
 * 1. 查询优化
 *    - 应用启发式规则进行逻辑优化
 *    - 基于代价模型进行物理优化
 *    - 选择最优的执行计划
 *
 * 2. 计划生成
 *    - 生成算子树
 *    - 确定访问方法
 *    - 选择连接策略
 *
 * 3. 资源管理
 *    - 控制连接算法的选择
 *    - 管理优化器参数
 *    - 维护系统配置
 */
class Planner {
   private:
    SmManager *sm_manager_;          // 系统管理器指针

    bool enable_nestedloop_join = true;   // 是否启用嵌套循环连接
    bool enable_sortmerge_join = false;   // 是否启用排序合并连接

   public:
    
    Planner(SmManager *sm_manager) : sm_manager_(sm_manager) {}

    std::shared_ptr<Plan> do_planner(std::shared_ptr<Query> query, Context *context);

    void set_enable_nestedloop_join(bool set_val) { enable_nestedloop_join = set_val; }

    void set_enable_sortmerge_join(bool set_val) { enable_sortmerge_join = set_val; }

   private:
    
    std::shared_ptr<Query> logical_optimization(std::shared_ptr<Query> query, Context *context);
    
    std::shared_ptr<Plan> physical_optimization(std::shared_ptr<Query> query, Context *context);
    
    std::shared_ptr<Plan> make_one_rel(std::shared_ptr<Query> query);
    
    std::shared_ptr<Plan> generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);
    
    std::shared_ptr<Plan> generate_select_plan(std::shared_ptr<Query> query, Context *context);

    // int get_indexNo(std::string tab_name, std::vector<Condition> curr_conds);

    
    bool get_index_cols(std::string tab_name, std::vector<Condition> curr_conds,
                       std::vector<std::string> &index_col_names);
   
    int get_table_col_num(const std::string &tab_name);

    size_t get_table_cardinality(const std::string &tab_name);
    
    std::shared_ptr<Plan> make_one_rel_optimized(std::shared_ptr<Query> query);

    std::shared_ptr<Plan> build_projection_plan(std::shared_ptr<Plan> plan,
                                              std::vector<TabCol> &need_cols,
                                              std::vector<TabCol> &all_cols);
    
    /**
     * @brief 构建左深树连接计划
     * @param table_plans 表扫描计划列表
     * @param join_conditions 连接条件
     * @return 连接计划
     */
    std::shared_ptr<Plan> build_left_deep_join_tree(
        std::list<std::shared_ptr<Plan>>& table_plans,
        std::list<Condition>& join_conditions,
        std::list<JoinNode>& semi_join,
        std::list<std::shared_ptr<Plan>>& semi_join_plans);

    std::shared_ptr<Plan> add_semi_join(
        std::shared_ptr<Plan> result,
        std::unordered_set<std::string>& joined_tables,
        std::list<JoinNode>& semi_join,
        std::list<std::shared_ptr<Plan>>& semi_join_plans
    );

    std::shared_ptr<Plan> add_join(
        std::shared_ptr<Plan> result,
        std::unordered_set<std::string>& joined_tables,
        std::list<std::shared_ptr<Plan>>& table_plans,
        std::list<Condition>& join_conditions,
        bool& flag
    );

    std::shared_ptr<Plan> join_tables(
        std::shared_ptr<Plan> result,
        const std::string &current_table,
        std::shared_ptr<Plan> current_scan,
        std::unordered_set<std::string>& joined_tables,
        std::list<Condition> &join_conditions,
        JoinType join_type
    );
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
