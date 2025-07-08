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
 */
class Planner {
   private:
    SmManager *sm_manager_;  // 系统管理器指针

    bool enable_nestedloop_join = true;  // 是否启用嵌套循环连接
    bool enable_sortmerge_join = false;  // 是否启用排序合并连接

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
     * @brief 对查询进行逻辑优化
     * @param query 要优化的查询对象
     * @param context 当前查询的上下文
     * @return 优化后的查询对象
     * @note 逻辑优化不涉及具体的执行方法选择，
     * 主要关注查询语义等价变换
     */
    std::shared_ptr<Query> logical_optimization(std::shared_ptr<Query> query, Context *context);
    /**
     * @brief 对查询进行物理优化
     * @param query 要优化的查询对象
     * @param context 当前查询的上下文
     * @return 生成的物理执行计划
     */
    std::shared_ptr<Plan> physical_optimization(std::shared_ptr<Query> query, Context *context);

    /**
     * @brief 为ORDER BY生成排序计划
     * @param query 查询对象
     * @param plan 输入计划
     * @return 添加了排序的计划或原计划
     * @note 排序是耗费资源的操作，
     * 应当尽可能利用已有顺序和索引
     */
    std::shared_ptr<Plan> generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);

    /**
     * @brief 为SELECT语句生成完整执行计划
     * @param query SELECT查询对象
     * @param context 执行上下文
     * @return 完整的查询执行计划
     * @throw PlanError 当计划生成失败时
     */

    std::shared_ptr<Plan> generate_aggregate_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);

    std::shared_ptr<Plan> generate_group_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);

    std::shared_ptr<Plan> generate_select_plan(std::shared_ptr<Query> query, Context *context);

    // int get_indexNo(std::string tab_name, std::vector<Condition> curr_conds);
    /**
     * @brief 识别条件中可用的索引列
     * @param tab_name 表名
     * @param curr_conds 当前条件集合
     * @param index_col_names 输出参数，存储可用的索引列名
     * @return 是否找到可用的索引列
     * @throw TableNotFoundError 当表不存在时
     */
    bool get_index_cols(std::string tab_name, std::vector<Condition> curr_conds,
                        std::vector<std::string> &index_col_names);
    /**
     * @brief 获取表的列数信息
     * @param tab_name 表名
     * @return 表的总列数
     * @throw TableNotFoundError 当表不存在时
     * @note 该信息用于：
     * - 资源分配
     * - 执行计划生成
     * - 结果集处理
     */
    int get_table_col_num(const std::string &tab_name);
    /**
     * @brief 获取表的记录数统计
     * @param tab_name 表名
     * @return 表中的记录总数
     * @throw TableNotFoundError 当表不存在时
     */
    size_t get_table_cardinality(const std::string &tab_name);

    /**
     * @brief 使用贪心算法优化多表连接顺序
     * @param query 查询对象
     * @return 优化后的查询计划
     * @throw PlanError 当无法生成有效计划时
     * @note 虽然不保证全局最优，但通常可以得到
     * 较好的局部最优解，且计算开销可控
     */
    std::shared_ptr<Plan> make_one_rel_optimized(std::shared_ptr<Query> query);

    /**
     * @brief 构建投影计划
     * @param plan 输入计划
     * @param need_cols 需要投影的列
     * @param all_cols 原始的所有列
     * @return 添加了投影的新计划
     */
    std::shared_ptr<Plan> build_projection_plan(std::shared_ptr<Plan> plan, std::vector<TabCol> &need_cols,
                                                std::vector<TabCol> &all_cols);
    /**
     * @brief 构建左深树连接计划
     * @param table_plans 表扫描计划列表
     * @param join_conditions 连接条件
     * @return 连接计划
     */
    std::shared_ptr<Plan> build_left_deep_join_tree(std::list<std::shared_ptr<Plan>> &table_plans,
                                                    std::list<Condition> &join_conditions,
                                                    std::list<JoinNode> &semi_join,
                                                    std::list<std::shared_ptr<Plan>> &semi_join_plans);

    std::shared_ptr<Plan> add_semi_join(std::shared_ptr<Plan> result, std::unordered_set<std::string> &joined_tables,
                                        std::list<JoinNode> &semi_join,
                                        std::list<std::shared_ptr<Plan>> &semi_join_plans);

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
