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
 * @brief 查询对象类，表示经过语义分析的SQL语句
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

    std::pair<int, int> limit;  // LIMIT子句的偏移和数量
    /**
     * @brief 默认构造函数
     * @note 创建一个空的查询对象，各容器保持为空
     */
    Query() = default;
};

/**
 * @brief 语义分析器类，处理SQL语句的验证和转换

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
     * @note 该方法是列验证的核心，确保查询中的列引用有效
     */
    TabCol check_column(const std::vector<ColMeta> &all_cols, TabCol target);

    /**
     * @brief 解析表别名并转换为真实表名
     * @param all_cols 所有相关表的列元数据集合
     * @param target 要处理的表列引用(会被修改)
     * @param tab_refs 查询中的所有表引用信息
     * @throw TableAliasError 当别名无效或有歧义时
     * @note target参数会被直接修改，包含转换后的表名
     */
    void convert_tabname(const std::vector<ColMeta> &all_cols, TabCol &target, const std::vector<TabRef> &tab_refs);

    void convert_tabname(const std::vector<ColMeta> &all_cols, TabCol &target, const std::vector<TabRef> &tab_refs,
                         const std::unordered_map<std::string, TabCol> &col_refs);

    bool convert_col_alias(const std::unordered_map<std::string, TabCol> &col_refs, TabCol &target);
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
     * @note 操作符转换会考虑类型兼容性
     */
    CompOp convert_sv_comp_op(ast::SvCompOp op);

    /**
     * @brief 转换JOIN操作类型
     * @param type SQL语法树中的JOIN类型
     * @return 系统内部的JoinType枚举值
     * @throw InvalidJoinTypeError 当JOIN类型无效时
     * @note
     * 1. 不同JOIN类型会影响连接条件的处理
     * 2. 外连接需要特殊处理NULL值
     * 3. 自然连接需要自动推断连接条件
     */
    JoinType convert_sv_join_type(ast::JoinType type);

    /**
     * @brief 将语法树中的算术操作符转换为系统内部的 ArithOp
     * @param op 语法树中的算术操作符
     * @return 系统内部的 ArithOp 枚举值
     */
    ArithOp convert_sv_arith_op(ast::SvArithOp op);

    /**
     * @brief 递归分析语法树中的表达式节点 (Value, Col, ArithExpr) 并转换为 ExprTerm
     * @param ast_expr 语法树中的表达式节点
     * @param all_cols 所有相关表的列元数据
     * @param tab_refs 查询涉及的表引用
     * @return 转换后的 ExprTerm 对象
     */
    std::shared_ptr<ExprTerm> AnalyzeExprTerm(const std::shared_ptr<ast::Expr> &ast_expr,
                                              const std::vector<ColMeta> &all_cols,
                                              const std::vector<TabRef> &tab_refs);
    /**
     * @brief 递归检查算术表达式中的项是否都是数值类型
     * @param term 要检查的表达式项
     * @param all_cols 所有相关表的列元数据
     * @throw IncompatibleTypeError 如果发现非数值类型
     */
    void CheckArithExprType(std::shared_ptr<ExprTerm> term, const std::vector<ColMeta> &all_cols);
    /**
     * @brief 检查WHERE条件中是否包含聚合函数
     * @param conds WHERE子句的条件表达式集合
     * @throw SemanticError 当条件中包含聚合函数时
     * @note 聚合函数通常只能在HAVING子句中使用
     */
    void check_where_with_aggregate(const std::vector<Condition> &conds);

    /**
     * @brief 检查HAVING子句中的条件表达式
     * @param conds HAVING子句的条件表达式集合
     * @param group_cols GROUP BY子句的列列表
     * @throw SemanticError 当HAVING条件不合法时
     */
    void check_having_conds(const std::vector<Condition> &conds, const std::vector<TabCol> &group_cols);

    void check_select_and_group(const std::vector<TabCol> &cols, const std::vector<TabCol> &group_cols);

    void check_orderby_with_group(const std::vector<OrderbyInfo> &order_bys, const std::vector<TabCol> &select_cols,
                                  const std::vector<TabCol> &group_cols);
};