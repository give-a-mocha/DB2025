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
     *
     * @details 优化策略：
     * 1. 谓词下推
     *    - 将过滤条件尽早应用
     *    - 减少中间结果数据量
     *
     * 2. 投影下推
     *    - 尽早去除不需要的列
     *    - 减少数据传输量
     *
     * 3. 表达式重写
     *    - 常量表达式化简
     *    - 等价谓词传递
     *
     * @note 逻辑优化不涉及具体的执行方法选择，
     * 主要关注查询语义等价变换
     */
    std::shared_ptr<Query> logical_optimization(std::shared_ptr<Query> query, Context *context);
    /**
     * @brief 对查询进行物理优化
     * @param query 要优化的查询对象
     * @param context 当前查询的上下文
     * @return 生成的物理执行计划
     *
     * @details 优化过程：
     * 1. 访问路径选择
     *    - 选择合适的索引
     *    - 评估全表扫描代价
     *    - 考虑索引覆盖查询
     *
     * 2. 连接方法选择
     *    - 嵌套循环连接
     *    - 排序合并连接
     *    - 代价比较和选择
     *
     * 3. 连接顺序优化
     *    - 贪心算法选择顺序
     *    - 考虑选择性和中间结果
     *
     * 4. 其他优化
     *    - 物化点选择
     *    - 并行执行计划
     *    - 内存使用评估
     */
    std::shared_ptr<Plan> physical_optimization(std::shared_ptr<Query> query, Context *context);

    /**
     * @brief 为单表查询生成访问计划
     * @param query 查询对象
     * @return 生成的扫描计划
     * @throw PlanError 当无法生成有效计划时
     *
     * @details 计划生成过程：
     * 1. 访问方法选择
     *    - 评估可用索引
     *    - 计算全表扫描代价
     *    - 选择最优访问路径
     *
     * 2. 过滤条件处理
     *    - 分析谓词选择性
     *    - 确定索引使用方式
     *    - 处理复合条件
     *
     * 3. 投影列处理
     *    - 确定需要的列
     *    - 检查索引覆盖
     *    - 优化列读取
     *
     * @note 这是生成执行计划的基础步骤，
     * 为后续的连接和其他操作提供输入
     */
    std::shared_ptr<Plan> make_one_rel(std::shared_ptr<Query> query);

    /**
     * @brief 为ORDER BY生成排序计划
     * @param query 查询对象
     * @param plan 输入计划
     * @return 添加了排序的计划或原计划
     *
     * @details 排序处理：
     * 1. 排序需求分析
     *    - 检查ORDER BY子句
     *    - 分析排序键
     *    - 确定排序方向
     *
     * 2. 优化机会
     *    - 利用现有索引顺序
     *    - 合并多个排序
     *    - 评估排序代价
     *
     * 3. 内存考虑
     *    - 估算排序内存需求
     *    - 选择内存排序或外排
     *    - 设置排序缓冲区大小
     *
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
     *
     * @details 计划生成步骤：
     * 1. 基础计划生成
     *    - 处理FROM子句
     *    - 生成表访问计划
     *    - 构建连接树
     *
     * 2. 过滤和投影
     *    - 处理WHERE条件
     *    - 添加投影操作
     *    - 处理GROUP BY
     *
     * 3. 结果处理
     *    - 实现ORDER BY
     *    - 处理LIMIT/OFFSET
     *    - 处理聚合函数
     *
     * 4. 优化和调整
     *    - 应用启发式规则
     *    - 调整算子顺序
     *    - 优化资源使用
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
    /**
     * @brief 识别条件中可用的索引列
     * @param tab_name 表名
     * @param curr_conds 当前条件集合
     * @param index_col_names 输出参数，存储可用的索引列名
     * @return 是否找到可用的索引列
     * @throw TableNotFoundError 当表不存在时
     *
     * @details 索引选择过程：
     * 1. 条件分析
     *    - 识别等值条件和范围条件
     *    - 提取涉及的列
     *    - 检查联合索引机会
     *
     * 2. 索引匹配
     *    - 查找表的所有索引
     *    - 评估索引适用性
     *    - 处理多列索引的前缀匹配
     *
     * 3. 选择策略
     *    - 评估索引选择性
     *    - 考虑索引维护成本
     *    - 选择最优索引组合
     */
    bool get_index_cols(std::string tab_name, std::vector<Condition> curr_conds,
                       std::vector<std::string> &index_col_names);
   /**
    * @brief 获取表的列数信息
    * @param tab_name 表名
    * @return 表的总列数
    * @throw TableNotFoundError 当表不存在时
    *
    * @details 统计内容：
    * 1. 列类型统计
    *    - 用户定义列数量
    *    - 系统列数量
    *    - 虚拟列数量
    *
    * 2. 元数据验证
    *    - 检查表结构完整性
    *    - 验证列定义有效性
    *    - 处理隐藏列
    *
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
    *
    * @details 统计过程：
    * 1. 基础统计
    *    - 活跃记录数
    *    - 已删除记录数
    *    - 总页面数
    *
    * 2. 更新机制
    *    - 统计信息缓存
    *    - 定期更新策略
    *    - 增量维护方法
    *
    * 3. 优化器使用
    *    - 估算查询代价
    *    - 选择执行计划
    *    - 预测中间结果大小
    */
    size_t get_table_cardinality(const std::string &tab_name);

    /**
     * @brief 使用贪心算法优化多表连接顺序
     * @param query 查询对象
     * @return 优化后的查询计划
     * @throw PlanError 当无法生成有效计划时
     *
     * @details 优化过程：
     * 1. 基表选择
     *    - 评估基表大小
     *    - 分析过滤条件
     *    - 考虑索引可用性
     *
     * 2. 连接顺序选择
     *    - 计算连接选择性
     *    - 评估中间结果大小
     *    - 应用贪心策略
     *
     * 3. 代价估算
     *    - CPU代价
     *    - I/O代价
     *    - 内存使用
     *
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
     *
     * @details 投影处理：
     * 1. 列选择
     *    - 保留need_cols中指定的列
     *    - 处理列的重命名
     *    - 处理表达式计算
     *
     * 2. 优化处理
     *    - 合并连续的投影
     *    - 删除冗余的投影
     *    - 投影下推优化
     */
    std::shared_ptr<Plan> build_projection_plan(std::shared_ptr<Plan> plan,
                                              std::vector<TabCol> &need_cols,
                                              std::vector<TabCol> &all_cols);
    /**
     * @brief 构建左深树连接计划
     * @param table_plans 表扫描计划列表
     * @param join_conditions 连接条件
     * @return 连接计划
     */
    /**
     * @brief 构建左深树连接计划
     * @param table_plans 基表的扫描计划列表
     * @param join_conditions 连接条件列表
     * @return 构建的连接计划树
     *
     * @details 构建过程：
     * 1. 连接顺序
     *    - 贪心选择连接顺序
     *    - 考虑选择性和基表大小
     *    - 应用启发式规则
     *
     * 2. 连接方法
     *    - 根据配置选择连接算法
     *    - 估算中间结果大小
     *    - 考虑索引的利用
     *
     * 3. 优化策略
     *    - 谓词下推
     *    - 投影及时应用
     *    - 考虑列的使用情况
     *
     * @note 左深树的特点：
     * - 便于流水线执行
     * - 减少中间结果存储
     * - 适合迭代器模型
     */
    std::shared_ptr<Plan> build_left_deep_join_tree(std::vector<std::shared_ptr<Plan>> &table_plans,
                                                   std::vector<Condition> &join_conditions);

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
