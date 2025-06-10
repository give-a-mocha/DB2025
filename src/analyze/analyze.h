/**
 * @file analyze.h
 * @author RMDB Development Team
 * @brief SQL语句的语义分析器
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * 语义分析器的主要功能：
 * 1. 验证和转换
 *    - 检查表和列的存在性
 *    - 解析和验证表别名
 *    - 处理列名歧义
 *    - 验证数据类型兼容性
 *
 * 2. 查询分析
 *    - 构建JOIN树结构
 *    - 收集WHERE条件
 *    - 处理子查询
 *    - 标识聚合函数
 *
 * 3. 错误处理
 *    - 表不存在检查
 *    - 列不存在检查
 *    - 列名歧义解决
 *    - 类型不匹配检查
 */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/Hash.h"
#include "common/TraceStack.hpp"
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
   private:
    // 列名到表名的映射, bool判断是否重复
    std::unordered_map<uint64, std::pair<std::string, bool>> cols;

   public:
    /**
     * @brief 构造函数，预处理所有列信息建立索引
     *
     * @param all_cols 所有相关表的列元数据集合
     */
    ColCheck(const std::vector<ColMeta> &all_cols) {
        TRACE_FUNCTION
        for (const auto &col : all_cols) {
            uint64 hashcode = getHashCode(col.name);
            hashcode = getHashCode(col.tab_name, hashcode);

            auto it = cols.find(hashcode);
            if (it == cols.end()) {
                cols.emplace(hashcode, std::make_pair(col.tab_name, false));
            } else {
                it->second.second = true;
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
        TRACE_FUNCTION
        uint64 hashcode = getHashCode(target_col.col_name);

        if (target_col.tab_name.empty()) {
            auto it = cols.find(hashcode);

            if (it == cols.end()) {
                // 列名不存在
                throw ColumnNotFoundError(target_col.col_name);
            } else {
                if (it->second.second) {
                    // 列名重复，无法确定表名
                    throw AmbiguousColumnError(target_col.col_name);
                } else {
                    target_col.tab_name = it->second.first;
                }
            }

        } else {
            hashcode = getHashCode(target_col.tab_name, hashcode);
            // 情况2: 已指定表名，验证(表名,列名)对是否存在
            auto it = cols.find(hashcode);
            if (it == cols.end()) {
                // 在指定表中未找到该列
                throw ColumnNotFoundError(target_col.col_name);
            }
            if (it->second.second == true) {
                // 在同一表中存在多个同名列(应该不会发生，但以防万一)
                throw AmbiguousColumnError(target_col.col_name);
            }
        }
        return target_col;  // 返回处理后的列引用
    }
};

/**
 * @brief 查询对象类，表示经过语义分析的SQL语句
 *
 * Query对象存储了一个SQL语句的完整语义信息：
 * 1. 查询结构
 *    - parse: 原始语法树，保留完整的语法结构
 *    - jointree: JOIN操作的层次结构
 *    - cols: 需要返回的列
 *    - tables: 涉及的表
 *
 * 2. 查询条件
 *    - conds: WHERE子句的条件表达式
 *    - 支持：比较操作、逻辑运算、IN子句等
 *
 * 3. 数据修改
 *    - set_clauses: UPDATE的SET子句
 *    - values: INSERT的VALUES值列表
 *
 * @note 该类设计支持SELECT、INSERT、UPDATE、DELETE等
 * 不同类型的SQL语句，相应字段根据语句类型使用
 */
class Query {
   public:
    // 查询的语法结构
    std::shared_ptr<ast::TreeNode> parse;  // 语法分析树根节点

    // 查询的逻辑结构
    std::vector<JoinNode> jointree;   // JOIN操作的层次结构
    std::vector<TabCol> cols;         // 投影列表(SELECT子句)
    std::vector<std::string> tables;  // 相关表名列表

    // 查询条件和约束
    std::vector<Condition> conds;  // WHERE子句条件列表

    // 数据修改相关
    std::vector<SetClause> set_clauses;  // UPDATE的SET子句
    std::vector<Value> values;           // INSERT的VALUES列表

    std::vector<TabCol> group_cols;  // GROUP BY子句的列

    std::vector<Condition> having_conds;  // HAVING子句的条件

    std::vector<OrderbyInfo> order_bys;  // ORDER BY 子句
    /**
     * @brief 默认构造函数
     * @note 创建一个空的查询对象，各容器保持为空
     */
    Query() = default;
};

/**
 * @brief 语义分析器类，处理SQL语句的验证和转换
 *
 * 负责功能：
 * 1. 语法树分析
 *    - 遍历和解析语法树节点
 *    - 提取查询组件(表、列、条件等)
 *    - 构建规范化的查询结构
 *
 * 2. 语义检查
 *    - 验证表和列的存在性
 *    - 检查类型兼容性
 *    - 解决命名冲突
 *    - 验证约束条件
 *
 * 3. 错误处理
 *    - 抛出语义错误异常
 *    - 提供详细的错误信息
 *    - 支持事务回滚
 *
 * @note 分析器依赖系统管理器(SmManager)获取
 * 数据库的元数据信息，如表结构、列类型等
 */
class Analyze {
   private:
    SmManager *sm_manager_;  // 系统管理器，提供元数据访问

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     */
    Analyze(SmManager *sm_manager) : sm_manager_(sm_manager) {}

    /**
     * @brief 析构函数
     * @note 析构时不会释放sm_manager_，因为它是外部传入的
     */
    ~Analyze() = default;

    /**
     * @brief 执行SQL语句的语义分析
     * @param root 语法分析产生的语法树根节点
     * @return 包含完整查询信息的Query对象
     * @throw SemanticError 当存在语义错误时
     *
     * @details 分析过程：
     * 1. 验证语法树的基本结构
     * 2. 根据SQL类型选择相应的处理流程
     * 3. 收集和验证所有查询组件
     * 4. 构建规范化的查询对象
     *
     * @note 该方法是语义分析的入口点，会触发
     * 完整的语义检查流程
     */
    std::shared_ptr<Query> do_analyze(std::shared_ptr<ast::TreeNode> root);

   private:
    /**
     * @brief 检查列是否存在并验证其元数据
     * @param all_cols 所有相关表的列元数据集合
     * @param target 目标列引用
     * @return 验证后的列引用(可能包含推断出的表名)
     * @throw ColumnNotFoundError 当列不存在时
     * @throw AmbiguousColumnError 当列名有歧义时
     *
     * @details 检查过程：
     * 1. 验证列是否存在
     * 2. 处理未限定的列名(无表名前缀)
     * 3. 解决可能的列名歧义
     * 4. 验证数据类型是否合法
     *
     * @note 该方法是列验证的核心，确保查询中的列引用有效
     */
    TabCol check_column(const std::vector<ColMeta> &all_cols, TabCol target);

    /**
     * @brief 解析表别名并转换为真实表名
     * @param all_cols 所有相关表的列元数据集合
     * @param target 要处理的表列引用(会被修改)
     * @param tab_refs 查询中的所有表引用信息
     * @throw TableAliasError 当别名无效或有歧义时
     *
     * @details 转换过程：
     * 1. 别名处理
     *    - 检查别名是否在tab_refs中定义
     *    - 验证别名的唯一性
     *    - 将别名替换为实际表名
     *
     * 2. 表名验证
     *    - 确保表名在数据库中存在
     *    - 处理大小写敏感性
     *
     * 3. 模式限定
     *    - 处理模式名前缀(如果有)
     *    - 验证模式的存在性
     *
     * @note target参数会被直接修改，包含转换后的表名
     */
    void convert_tabname(const std::vector<ColMeta> &all_cols, TabCol &target, const std::vector<TabRef> &tab_refs);

    /**
     * @brief 获取所有相关表的列元数据
     * @param tab_names 表名列表
     * @param all_cols 输出参数，存储收集到的所有列元数据
     */
    void get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols);

    /**
     * @brief 处理WHERE条件的基本转换
     * @param sv_conds 语法树中的条件表达式集合
     * @param conds 输出参数，存储转换结果
     * @throw SemanticError 当条件表达式存在语法或语义错误
     *
     * @details 转换过程：
     * 1. 表达式处理
     *    - 解析比较运算符
     *    - 处理常量表达式
     *    - 转换数据类型
     *
     * 2. 条件组合
     *    - 处理AND/OR逻辑关系
     *    - 合并相关条件
     *    - 优化条件顺序
     *
     * 3. 类型检查
     *    - 验证操作数类型匹配
     *    - 处理隐式类型转换
     *
     * @note 该方法不处理表别名，需要配合get_clause_alias使用
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
     * @brief 将SQL比较操作符转换为系统操作符
     * @param op SQL语法树中的原始操作符
     * @return 系统内部的CompOp枚举值
     * @throw InvalidCompOpError 当操作符无效时
     *
     * @details 支持的操作符：
     * 1. 关系运算
     *    - 等于(=)
     *    - 不等于(<>, !=)
     *    - 大于(>)
     *    - 大于等于(>=)
     *    - 小于(<)
     *    - 小于等于(<=)
     *
     * 2. 特殊操作
     *    - IS NULL
     *    - IS NOT NULL
     *    - IN / NOT IN
     *    - EXISTS
     *
     * @note 操作符转换会考虑类型兼容性
     */
    CompOp convert_sv_comp_op(ast::SvCompOp op);

    /**
     * @brief 转换JOIN操作类型
     * @param type SQL语法树中的JOIN类型
     * @return 系统内部的JoinType枚举值
     * @throw InvalidJoinTypeError 当JOIN类型无效时
     *
     * @details 支持的JOIN类型：
     * 1. 基本连接
     *    - INNER JOIN（内连接）
     *    - LEFT JOIN（左外连接）
     *    - RIGHT JOIN（右外连接）
     *    - FULL JOIN（全外连接）
     *
     * 2. 特殊连接
     *    - CROSS JOIN（交叉连接）
     *    - NATURAL JOIN（自然连接）
     *
     * @note
     * 1. 不同JOIN类型会影响连接条件的处理
     * 2. 外连接需要特殊处理NULL值
     * 3. 自然连接需要自动推断连接条件
     */
    JoinType convert_sv_join_type(ast::JoinType type);

    /**
     * @brief 检查WHERE条件中是否包含聚合函数
     * @param conds WHERE子句的条件表达式集合
     * @throw SemanticError 当条件中包含聚合函数时
     *
     * @details 检查过程：
     * 1. 遍历所有条件表达式
     * 2. 检测是否有聚合函数调用
     * 3. 如果存在，抛出语义错误异常
     *
     * @note 聚合函数通常只能在HAVING子句中使用
     */
    void check_where_with_aggregate(const std::vector<Condition> &conds);

    /**
     * @brief 检查HAVING子句中的条件表达式
     * @param conds HAVING子句的条件表达式集合
     * @param group_cols GROUP BY子句的列列表
     * @throw SemanticError 当HAVING条件不合法时
     *
     * @details 检查过程：
     * 1. 确保HAVING条件中只包含聚合函数或GROUP BY列
     * 2. 验证聚合函数的使用是否符合SQL标准
     * 3. 抛出错误如果条件不满足要求
     */
    void check_having_conds(const std::vector<Condition> &conds, const std::vector<TabCol> &group_cols);

    void check_select_and_group(const std::vector<TabCol> &cols, const std::vector<TabCol> &group_cols);
};