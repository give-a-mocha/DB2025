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
#include <string>
#include <vector>

#include "common/common.h"
#include "parser/parser.h"
#include "system/sm.h"

/**
 * @brief 列检查器类，用于高效验证列引用的有效性
 *
 * 该类缓存列名信息，提供快速查找机制，用于检查列是否存在和解决列名歧义。
 * 预先构建映射表以提高查询效率，适用于大量列检查操作的场景。
 */
class ColCheck {
   public:
    // 列名到表名的映射，用于处理无表名列引用
    std::map<std::string, std::vector<std::string>> mp;

    // (表名,列名)对到出现次数的映射，用于检测重复列
    std::map<std::pair<std::string, std::string>, int> cols;

    /**
     * @brief 构造函数，预处理所有列信息建立索引
     *
     * @param all_cols 所有相关表的列元数据集合
     */
    ColCheck(const std::vector<ColMeta> &all_cols) {
        for (const auto &col : all_cols) {
            // 记录每个(表名,列名)对的出现次数
            cols[{col.tab_name, col.name}]++;

            // 构建列名到表名的映射，最多记录两个表
            // (如果有两个以上的表包含同名列，我们只需知道有歧义即可)
            auto it = mp.find(col.name);
            if (it == mp.end()) {
                // 第一次遇到该列名，创建新条目
                mp[col.name] = std::vector<std::string>({col.tab_name});
            } else {
                // 已经存在该列名，检查是否需要添加新表名
                if (it->second.size() < 2) {
                    it->second.push_back(col.tab_name);
                }
            }
        }
    }

    /**
     * @brief 检查给定的列引用是否有效，并解决表名推断
     *
     * 如果列引用中没有指定表名，则尝试推断；如果指定了表名，则验证其存在性。
     * 遇到歧义或不存在的列时抛出相应异常。
     *
     * @param target_col 需要检查的目标列引用
     * @return TabCol 处理后的列引用(可能添加了推断出的表名)
     */
    TabCol check(TabCol target_col) {
        if (target_col.tab_name.empty()) {
            // 情况1: 未指定表名，需要查找并可能推断表名
            auto it = mp.find(target_col.col_name);
            if (it == mp.end()) {
                // 列名不存在
                throw ColumnNotFoundError(target_col.col_name);
            }
            if (it->second.size() > 1) {
                // 存在多个表包含该列名，无法唯一确定
                throw AmbiguousColumnError(target_col.col_name);
            }
            // 找到唯一的表名并设置
            target_col.tab_name = it->second[0];
        } else {
            // 情况2: 已指定表名，验证(表名,列名)对是否存在
            auto it = cols.find({target_col.tab_name, target_col.col_name});
            if (it == cols.end()) {
                // 在指定表中未找到该列
                throw ColumnNotFoundError(target_col.col_name);
            }
            if (it->second > 1) {
                // 在同一表中存在多个同名列(应该不会发生，但以防万一)
                throw AmbiguousColumnError(target_col.col_name);
            }
        }
        return target_col;  // 返回处理后的列引用
    }
};

/**
 * @brief 查询对象类，存储经过语义分析后的查询信息
 *
 * 该类包含查询执行所需的所有结构化信息，如表名、列、条件、连接等。
 * 负责存储从语法树转换而来的已验证查询组件。
 */
class Query {
   public:
    std::shared_ptr<ast::TreeNode> parse;  // 原始语法树节点

    // JOIN树 - 存储所有的JOIN操作
    std::vector<JoinNode> jointree;

    // WHERE条件列表
    std::vector<Condition> conds;

    // 投影列(SELECT子句中的列)
    std::vector<TabCol> cols;

    // 查询涉及的表名列表
    std::vector<std::string> tables;

    // UPDATE语句的SET子句值
    std::vector<SetClause> set_clauses;

    // INSERT语句的VALUES值列表
    std::vector<Value> values;

    Query() = default;
};

/**
 * @brief 语义分析器类，负责SQL语句的语义检查和查询对象构建
 *
 * 该类接收语法分析生成的语法树，进行语义验证并构建查询执行计划。
 * 主要职责包括：检查表和列是否存在、解析表别名、类型检查、条件验证等。
 */
class Analyze {
   private:
    SmManager *sm_manager_;  // 系统管理器指针，用于访问数据库元数据

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     */
    Analyze(SmManager *sm_manager) : sm_manager_(sm_manager) {}

    /**
     * @brief 析构函数
     */
    ~Analyze() {}

    /**
     * @brief 执行语义分析
     * @param root 语法树根节点
     * @return 处理后的查询对象
     */
    std::shared_ptr<Query> do_analyze(std::shared_ptr<ast::TreeNode> root);

   private:
    /**
     * @brief 检查列是否存在，返回列的元数据
     * @param all_cols 所有相关表的列元数据集合
     * @param target 目标列引用
     * @return 验证后的列引用
     */
    TabCol check_column(const std::vector<ColMeta> &all_cols, TabCol target);

    /**
     * @brief 将表别名转换为真实表名
     * @param all_cols 所有相关表的列元数据
     * @param target 需要处理的表列引用(会被修改)
     * @param tab_refs 查询中的所有表引用
     */
    void convert_tabname(const std::vector<ColMeta> &all_cols, TabCol &target, const std::vector<TabRef> &tab_refs);

    /**
     * @brief 获取所有相关表的列元数据
     * @param tab_names 表名列表
     * @param all_cols 输出参数，存储收集到的所有列元数据
     */
    void get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols);

    /**
     * @brief 处理WHERE条件转换(不处理表别名)
     * @param sv_conds 语法树条件表达式集合
     * @param conds 输出参数，存储转换后的条件
     */
    void get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds);

    /**
     * @brief 处理带有表别名的WHERE条件转换
     * @param all_cols 所有相关表的列元数据
     * @param sv_conds 语法树条件表达式集合
     * @param conds 输出参数，存储转换后的条件
     * @param tab_refs 查询涉及的表引用
     */
    void get_clause_alias(const std::vector<ColMeta> &all_cols,
                          const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds,
                          const std::vector<TabRef> &tab_refs);

    /**
     * @brief 检查条件表达式的有效性及类型兼容性
     * @param tab_names 条件中涉及的表名列表
     * @param conds 需要检查的条件表达式集合
     */
    void check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds);

    /**
     * @brief 使用自定义列检查器验证条件有效性
     * @param tab_names 条件中涉及的表名列表
     * @param conds 需要检查的条件表达式集合
     * @param col_check 自定义列检查器
     */
    void check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds, ColCheck &col_check);

    /**
     * @brief 将语法树中的值对象转换为系统内部的Value对象
     * @param sv_val 语法树中的值对象
     * @return 转换后的系统内部值对象
     */
    Value convert_sv_value(const std::shared_ptr<ast::Value> &sv_val);

    /**
     * @brief 将语法树中的比较操作符转换为系统内部的CompOp枚举值
     * @param op 语法树中的比较操作符
     * @return 转换后的系统内部比较操作符
     */
    CompOp convert_sv_comp_op(ast::SvCompOp op);

    /**
     * @brief 将语法树中的JOIN类型转换为系统内部的JoinType枚举值
     * @param type 语法树中的JOIN类型
     * @return 转换后的系统内部JOIN类型
     */
    JoinType convert_sv_join_type(ast::JoinType type);
};