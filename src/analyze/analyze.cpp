/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze.h"

#include "common/TraceStack.hpp"
#include "common/print.hpp"

extern SmManager sm_manager;

namespace {
/**
 * @brief 辅助函数：检查列是否在cols中
 *
 * @param check_col 需要检查的列
 * @param cols 列集合
 * @return true 如果列在cols中
 * @return false 如果列不在cols中
 */
bool is_column_in_cols(const TabCol &check_col, const std::vector<TabCol> &cols) {
    return std::any_of(cols.begin(), cols.end(), [&](const TabCol &col) {
        return std::tie(col.tab_name, col.col_name, col.agg_type) ==
               std::tie(check_col.tab_name, check_col.col_name, check_col.agg_type);
    });
}
}  // namespace

/**
 * @brief 将表的别名转换为真实表名，并处理相关表名和列名歧义
 *
 * 此函数用于处理SQL查询中的表别名和列引用，确保每个列引用都能正确关联到对应的表。
 * 根据不同的输入情况（有表名无别名、有别名、无表名无别名）进行不同的处理逻辑。
 *
 * @param all_cols 所有相关表的列元数据
 * @param target 需要处理的表列引用对象(会被修改)
 * @param tab_refs 查询中的所有表引用(包含表名和别名信息)
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse) {
    TRACE_FUNCTION
    std::shared_ptr<Query> query = std::make_shared<Query>();  // 创建查询对象
    switch (parse->getType()) {
        case ast::AstType::SelectStmt: {
            auto x = std::static_pointer_cast<ast::SelectStmt>(parse);  // 处理SELECT查询
            // 检查所有表是否存在并处理表名和别名
            std::vector<TabRef> tab_refs;  // 存储表引用及其别名
            tab_refs.reserve(x->tabs.size());
            query->tables.reserve(x->tabs.size());
            for (const auto &sv_tab : x->tabs) {
                std::string tab_name = sv_tab->tab_name;
                tab_refs.push_back(TabRef(tab_name, sv_tab->alias));  // 添加表引用
                if (!sm_manager.db_.is_table(tab_name)) {             // 检查表是否存在
                    throw TableNotFoundError(tab_name);
                }
                query->tables.push_back(tab_name);  // 添加到查询的表列表
            }

            // 处理JOIN表
            if (!x->jointree.empty()) {
                for (const auto &join_expr : x->jointree) {
                    // 检查JOIN右侧的表是否存在
                    std::string right_tab_name = join_expr->right->tab_name;
                    if (!sm_manager.db_.is_table(right_tab_name)) {
                        throw TableNotFoundError(right_tab_name);
                    }
                    TabRef right_table(right_tab_name, join_expr->right->alias);
                    query->tables.push_back(right_tab_name);
                    tab_refs.push_back(right_table);
                }
            }

            // 获取所有相关表的全部列
            std::vector<ColMeta> all_cols;
            get_all_cols(query->tables, all_cols);

            for (auto &sv_group_col : x->group) {
                if (sv_group_col->cols->aggregate_type != ast::SvAggregateType::NONE) {
                    throw AggregateError("GROUP BY column cannot have aggregate function");
                }
                TabCol group_col = {"", sv_group_col->cols->col_name, sv_group_col->cols->tab_name};
                convert_tabname(all_cols, group_col, tab_refs);  // 处理表名和别名
                query->group_cols.push_back(group_col);          // 添加到查询的分组列列表
            }

            // 处理要查询的列：
            // 情况1: 如果没有明确指定列(SELECT *)，则查询所有列
            if (x->cols.empty()) {
                query->cols.reserve(all_cols.size());
                for (const auto &col : all_cols) {
                    TabCol sel_col = {col.tab_name, col.name};     // 创建表列引用
                    convert_tabname(all_cols, sel_col, tab_refs);  // 处理表名和别名
                    query->cols.push_back(sel_col);                // 添加到查询列表
                }
            } else {
                query->cols.reserve(x->cols.size());
                for (auto &sv_sel_col : x->cols) {
                    // 创建列引用，初始状态下表名可能为空
                    TabCol sel_col = {"", sv_sel_col->col_name, sv_sel_col->tab_name};
                    sel_col.set_col_alias(sv_sel_col->alias);          // 设置列别名
                    sel_col.set_agg_type(sv_sel_col->aggregate_type);  // 设置聚合类型
                    convert_tabname(all_cols, sel_col, tab_refs);      // 处理表名和别名
                    query->cols.push_back(sel_col);                    // 添加到查询列表
                }
            }

            {
                // 查询列必须的是group by中的列或聚合函数
                std::unordered_set<TabCol, TabColHash> group_cols_set(query->group_cols.begin(),
                                                                      query->group_cols.end());
                bool has_aggregate = false;
                bool has_non_aggregate = false;
                for (const auto &col : query->cols) {
                    if (col.agg_type == AggregateType::NONE) {
                        has_non_aggregate = true;  // 存在非聚合列
                        // 如果是普通列，必须在GROUP BY中
                        if (!query->group_cols.empty() && group_cols_set.find(col) == group_cols_set.end()) {
                            throw InternalError("Column '" + col.to_string() +
                                                "' must be in GROUP BY clause or an aggregate function");
                        }
                    } else {
                        has_aggregate = true;  // 存在聚合列
                    }
                }
                if (query->group_cols.empty() && has_non_aggregate && has_aggregate) {
                    throw InternalError(
                        "Cannot mix aggregate and non-aggregate columns in SELECT clause when group by is empty");
                }
            }

            // 处理WHERE条件子句
            get_clause_alias(all_cols, x->conds, query->conds, tab_refs);
            check_clause_with_cols(all_cols, query->tables, query->conds, true);  // 检查WHERE条件的有效性

            // 处理having条件
            get_clause_alias(all_cols, x->having_conds, query->having_conds, tab_refs);
            check_clause_with_cols(all_cols, query->tables, query->having_conds, false);
            // 检查having条件中是否有不是聚合函数也不是group by的列
            check_having_conds(query->having_conds, query->group_cols);

            // 如果没有GROUP BY子句但有HAVING条件，则抛出错误
            if (query->group_cols.empty() && !query->having_conds.empty()) {
                throw InternalError("HAVING clause without GROUP BY is not allowed");
            }

            // 处理JOIN操作及其条件
            if (!x->jointree.empty()) {
                query->jointree.reserve(x->jointree.size());
                for (auto &join_expr : x->jointree) {
                    std::string right_tab_name = join_expr->right->tab_name;
                    TabRef right_table(right_tab_name, join_expr->right->alias);

                    // 转换JOIN条件
                    std::vector<Condition> join_conds;
                    get_clause_alias(all_cols, join_expr->conds, join_conds, tab_refs);
                    check_clause(query->tables, join_conds, true);
                    // 创建JOIN节点并指定JOIN类型
                    JoinType join_type = convert_sv_join_type(join_expr->type);
                    query->jointree.emplace_back(std::move(right_tab_name), std::move(join_conds), join_type);
                }
            }

            // 处理ORDER BY子句
            if (x->has_sort) {
                for (auto &sv_order_by : x->orders) {
                    OrderbyInfo order_by_info;
                    order_by_info.dir = sv_order_by->orderby_dir;

                    TabCol order_by_col = {"", sv_order_by->cols->col_name, sv_order_by->cols->tab_name};
                    order_by_col.set_agg_type(sv_order_by->cols->aggregate_type);
                    convert_tabname(all_cols, order_by_col, tab_refs);
                    order_by_info.col = check_column(all_cols, order_by_col);
                    query->order_bys.push_back(order_by_info);
                }
                check_orderby_with_group(query->order_bys, query->cols, query->group_cols);
            }

            // 处理 limit
            query->limit = std::make_pair(0, std::numeric_limits<int>::max());  // 默认不限制数量
            if (x->limit) {
                int offset = 0;
                int count = std::numeric_limits<int>::max();  // 默认不限制数量
                if (x->limit->offset->getType() == ast::AstType::IntLit) {
                    auto offset_val = std::static_pointer_cast<ast::IntLit>(x->limit->offset);
                    offset = offset_val->val;
                }
                if (x->limit->count->getType() == ast::AstType::IntLit) {
                    auto count_val = std::static_pointer_cast<ast::IntLit>(x->limit->count);
                    count = count_val->val;
                }
                if (offset < 0 || count < 0) {
                    throw InternalError("LIMIT offset and count must be non-negative");
                }
                query->limit = std::make_pair(offset, count);  // 设置LIMIT偏移
            }
            break;
        }
        case ast::AstType::UpdateStmt: {
            auto x = std::static_pointer_cast<ast::UpdateStmt>(parse);  // 处理UPDATE查询
            // 添加被更新的表
            if (!sm_manager.db_.is_table(x->tab_name->tab_name)) {  // 检查表是否存在
                throw TableNotFoundError(x->tab_name->tab_name);
            }
            query->tables.push_back(x->tab_name->tab_name);
            // 获取相关表的所有列，用于后续校验
            std::vector<ColMeta> all_cols;
            get_all_cols(query->tables, all_cols);
            std::vector<TabRef> tab_refs = {TabRef(x->tab_name->tab_name, x->tab_name->alias)};  // 创建表引用
            // 准备SET子句
            query->set_clauses.reserve(x->set_clauses.size());
            // 处理每个SET赋值
            for (auto &sv_set_clause : x->set_clauses) {
                SetClause set_clause;
                set_clause.lhs.tab_name = x->tab_name->tab_name;    // 设置左侧列的表名
                set_clause.lhs.tab_alias = x->tab_name->alias;      // 设置左侧列的别名
                set_clause.lhs.col_name = sv_set_clause->col_name;  // 设置左侧列名

                auto rhs_term = AnalyzeExprTerm(sv_set_clause->val, all_cols, tab_refs);
                if (rhs_term->term_type == TermType::VALUE) {
                    set_clause.rhs_type = SetRhsType::SET_RHS_VALUE;
                    set_clause.rhs_val = rhs_term->val;
                } else if (rhs_term->term_type == TermType::EXPR) {
                    set_clause.rhs_type = SetRhsType::SET_RHS_EXPR;
                    set_clause.rhs_expr = rhs_term->expr;
                } else if (rhs_term->term_type == TermType::COLUMN) {  // TermType::COLUMN
                    set_clause.rhs_type = SetRhsType::SET_RHS_COL;
                    set_clause.rhs_col = rhs_term->col;  // 设置右侧列引用
                } else {
                    throw RMDBError("Unsupported right-hand side type in UPDATE statement");
                }
                query->set_clauses.push_back(std::move(set_clause));  // 使用 move 提高效率
            }

            // 处理WHERE条件
            get_clause_alias(all_cols, x->conds, query->conds, tab_refs);
            check_clause(query->tables, query->conds, true);  // 检查WHERE条件的有效性

            // 检查每个SET子句的有效性和类型兼容性
            for (auto &set_clause : query->set_clauses) {
                set_clause.lhs = check_column(all_cols, set_clause.lhs);  // 检查列是否存在

                if (set_clause.rhs_type == SetRhsType::SET_RHS_VALUE) {
                    TabMeta &tab = sm_manager.db_.get_table(set_clause.lhs.tab_name);
                    auto col = tab.get_col(set_clause.lhs.col_name);
                    // 允许数值类型之间的转换(INT与FLOAT)
                    bool is_numeric = (col->type == ColType::TYPE_INT || col->type == ColType::TYPE_FLOAT) &&
                                      (set_clause.rhs_val.type == ColType::TYPE_INT ||
                                       set_clause.rhs_val.type == ColType::TYPE_FLOAT);
                    if (col->type != set_clause.rhs_val.type && !is_numeric) {
                        throw IncompatibleTypeError(coltype2str(col->type), coltype2str(set_clause.rhs_val.type));
                    }
                    set_clause.rhs_val.init_raw(col->len);  // 初始化值的原始数据
                } else if (set_clause.rhs_type == SetRhsType::SET_RHS_COL) {
                    // 检查右侧列是否存在
                    set_clause.rhs_col = check_column(all_cols, set_clause.rhs_col);
                    // 类型兼容校验：允许 INT↔FLOAT，其他必须完全一致
                    TabMeta &lhs_tab = sm_manager.db_.get_table(set_clause.lhs.tab_name);
                    auto l_col = lhs_tab.get_col(set_clause.lhs.col_name);
                    TabMeta &rhs_tab = sm_manager.db_.get_table(set_clause.rhs_col.tab_name);
                    auto r_col = rhs_tab.get_col(set_clause.rhs_col.col_name);

                    bool is_numeric = (l_col->type == ColType::TYPE_INT || l_col->type == ColType::TYPE_FLOAT) &&
                                      (r_col->type == ColType::TYPE_INT || r_col->type == ColType::TYPE_FLOAT);
                    if (l_col->type != r_col->type && !is_numeric) {
                        throw IncompatibleTypeError(coltype2str(l_col->type), coltype2str(r_col->type));
                    }
                } else if (set_clause.rhs_type == SetRhsType::SET_RHS_EXPR) {
                    std::shared_ptr<ExprTerm> temp = std::make_shared<ExprTerm>(set_clause.rhs_expr);
                    std::vector<ColMeta> ltable_cols;
                    get_all_cols({set_clause.lhs.tab_name}, ltable_cols);  // 获取左侧表的所有列
                    CheckArithExprType(temp, ltable_cols);
                    set_clause.rhs_expr = std::move(temp->expr);
                    TabMeta &tab = sm_manager.db_.get_table(set_clause.lhs.tab_name);
                    auto col = tab.get_col(set_clause.lhs.col_name);
                    if (col->type == ColType::TYPE_STRING) {
                        throw IncompatibleTypeError(coltype2str(col->type), coltype2str(ColType::TYPE_FLOAT));
                    }
                }
            }
            break;
        }
        case ast::AstType::DeleteStmt: {
            auto x = std::static_pointer_cast<ast::DeleteStmt>(parse);  // 处理DELETE查询
            // 添加要删除数据的表名
            query->tables.push_back(x->tab_name);
            std::vector<ColMeta> all_cols;                                      // 存储相关表的列元数据
            get_all_cols(query->tables, all_cols);                              // 获取所有相关表的列
            std::vector<TabRef> tab_refs = {TabRef(x->tab_name, x->tab_name)};  // 创建表引用
            // 处理WHERE条件
            get_clause_alias(all_cols, x->conds, query->conds, tab_refs);  // 获取WHERE条件并处理别名
            check_clause({x->tab_name}, query->conds, false);              // 检查WHERE条件的有效性
            break;
        }
        case ast::AstType::InsertStmt: {
            auto x = std::static_pointer_cast<ast::InsertStmt>(parse);  // 处理INSERT查询
            // 处理INSERT的VALUES值
            for (auto &sv_val : x->vals) {
                query->values.push_back(convert_sv_value(sv_val));  // 转换并添加每个插入值
            }
            break;
        }
        default:
            // 其他类型的SQL语句，不做处理
            break;
    }

    query->parse = std::move(parse);  // 保存原始语法树
    return query;                     // 返回处理后的查询对象
}

/**
 * @brief 将表的别名转换为真实表名，并处理相关表名和列名歧义
 *
 * 此函数用于处理SQL查询中的表别名和列引用，确保每个列引用都能正确关联到对应的表。
 * 根据不同的输入情况（有表名无别名、有别名、无表名无别名）进行不同的处理逻辑。
 *
 * @param all_cols 所有相关表的列元数据
 * @param target 需要处理的表列引用对象(会被修改)
 * @param tab_refs 查询中的所有表引用(包含表名和别名信息)
 */
void Analyze::convert_tabname(const std::vector<ColMeta> &all_cols, TabCol &target,
                              const std::vector<TabRef> &tab_refs) {
    TRACE_FUNCTION
    // COUNT(*) 的特殊情况 - 不需要表名和别名
    if (target.col_name == "*" && target.agg_type == AggregateType::COUNT) {
        return;
    }
    // 情况1: 有表名但没有别名 - 尝试找到表名对应的真实表并处理表别名
    if (target.tab_alias.empty() && !target.tab_name.empty()) {
        TabRef res = {target.tab_name, target.tab_alias};  // 初始化结果
        int cnt = 0;                                       // 计数器，用于检测歧义
        for (const auto &tab_ref : tab_refs) {
            if (tab_ref.name == target.tab_name) {
                cnt++;
                res = tab_ref;
                if (cnt > 1) {
                    // 如果找到多个匹配的表名，抛出列名歧义错误
                    throw AmbiguousColumnError(target.col_name);
                }
            }
        }
        if (cnt == 0) {
            // 如果没找到匹配的表，抛出表不存在错误
            throw TableNotFoundError(target.tab_name);
        }
        target.tab_name = res.name;    // 设置真实表名
        target.tab_alias = res.alias;  // 设置表别名
        return;
    }
    // 情况2: 有别名 - 尝试通过别名找到真实表名
    else if (!target.tab_alias.empty()) {
        TabRef res = {target.tab_name, target.tab_alias};  // 默认表名和别名相同
        int cnt = 0;
        for (const auto &tab_ref : tab_refs) {
            if (tab_ref.get_name() == target.tab_alias) {
                cnt++;
                res = tab_ref;
                if (cnt > 1) {
                    // 如果找到多个匹配的别名，抛出列名歧义错误
                    throw AmbiguousColumnError(target.col_name);
                }
            }
        }
        if (cnt == 0) {
            // 如果没找到匹配的别名，抛出列不存在错误
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = res.name;    // 设置真实表名
        target.tab_alias = res.alias;  // 设置表别名
        return;
    }
    // 情况3: 既没有表名也没有别名 - 尝试从所有表的列中推断表名
    else {
        std::string tab_name;
        for (auto &col : all_cols) {
            if (col.name == target.col_name) {
                if (!tab_name.empty()) {
                    // 如果在多个表中找到同名列，抛出列名歧义错误
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
            }
        }
        if (tab_name.empty()) {
            // 如果没找到匹配的列，抛出列不存在错误
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;                   // 设置推断出的表名
        convert_tabname(all_cols, target, tab_refs);  // 递归调用，进一步解析表名和别名
    }
}

void Analyze::convert_tabname(const std::vector<ColMeta> &all_cols, TabCol &target, const std::vector<TabRef> &tab_refs,
                              const std::unordered_map<std::string, TabCol> &col_refs) {
    TRACE_FUNCTION
    if (target.tab_alias.empty() && target.tab_name.empty() && convert_col_alias(col_refs, target)) {
        return;  // 如果列别名转换成功，直接返回
    }
    // 如果列别名转换失败，继续使用原有的 convert_tabname 逻辑
    convert_tabname(all_cols, target, tab_refs);
}

bool Analyze::convert_col_alias(const std::unordered_map<std::string, TabCol> &col_refs, TabCol &target) {
    TRACE_FUNCTION
    if (col_refs.count(target.col_name)) {
        // 如果列名在别名映射中存在，直接使用对应的列引用
        target = col_refs.at(target.col_name);
        return true;
    }
    return false;
}

/**
 * @brief 检查并验证目标列是否存在，并处理表名推断
 *
 * @param all_cols 所有相关表的列元数据集合
 * @param target 需要检查的目标列引用
 * @return TabCol 验证后的列引用
 */
TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
    TRACE_FUNCTION
    // COUNT(*) 的特殊情况 - 不需要表名和别名
    if (target.col_name == "*" && target.agg_type == AggregateType::COUNT) {
        return target;
    }
    if (target.tab_name.empty()) {
        // 情况1: 未指定表名，需要从列名推断表名
        std::string tab_name;
        for (auto &col : all_cols) {
            if (col.name == target.col_name) {
                if (!tab_name.empty()) {
                    // 如果多个表中存在同名列，则抛出列名歧义错误
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;  // 记录找到的表名
            }
        }
        if (tab_name.empty()) {
            // 如果没有找到匹配的列，抛出列不存在错误
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;  // 设置推断出的表名
    } else {
        // 情况2: 已指定表名，验证列是否在指定的表中存在
        int count = 0;
        for (auto &col : all_cols) {  // 遍历查找是否存在以及是否重复
            if (col.name == target.col_name && col.tab_name == target.tab_name) {
                count++;
                if (count > 1) {
                    // 如果在同一表中发现重复列名(异常情况)，抛出歧义错误
                    throw AmbiguousColumnError(target.col_name);
                }
            }
        }
        if (count == 0) {  // 如果未能找到
            // 表中不存在该列，抛出列不存在错误
            throw ColumnNotFoundError(target.col_name);
        }
    }
    return target;  // 返回验证后的列引用
}

/**
 * @brief 获取所有相关表的所有列元数据
 *
 * 该函数遍历所有参与查询的表，收集它们的所有列信息，
 * 用于后续的列引用解析、类型检查和表名推断。
 *
 * @param tab_names 参与查询的表名列表
 * @param all_cols 输出参数，用于存储收集到的所有列元数据
 */
void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols) {
    TRACE_FUNCTION
    for (auto &sel_tab_name : tab_names) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager.db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());  // 将表的所有列添加到结果集中
    }
}

/**
 * @brief 处理带有表别名的WHERE条件转换
 *
 * 将语法树中的条件表达式转换为系统内部使用的条件对象，并处理表别名。
 * 支持处理列与值比较以及列与列比较的情况，同时解析表别名为实际表名。
 *
 * @param all_cols 所有相关表的列元数据
 * @param sv_conds 语法树中的条件表达式集合
 * @param conds 输出参数，用于存储转换后的条件对象
 * @param tab_refs 查询涉及的表引用(包含表名和别名信息)
 */
void Analyze::get_clause_alias(const std::vector<ColMeta> &all_cols,
                               const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds,
                               std::vector<Condition> &conds, const std::vector<TabRef> &tab_refs) {
    TRACE_FUNCTION
    conds.clear();                   // 清空输出条件向量
    conds.reserve(sv_conds.size());  // 预分配空间以提高性能
    std::unordered_map<TabCol, Value, TabColHash> col_values;
    for (auto &expr : sv_conds) {
        Condition cond;
        // 处理条件左侧的列引用
        cond.lhs_col.tab_alias = expr->lhs->tab_name;          // 设置左侧列的表别名
        cond.lhs_col.col_name = expr->lhs->col_name;           // 设置左侧列名
        cond.lhs_col.set_col_alias(expr->lhs->alias);          // 设置左侧列的别名
        cond.lhs_col.set_agg_type(expr->lhs->aggregate_type);  // 设置左侧列的聚合类型
        convert_tabname(all_cols, cond.lhs_col, tab_refs);     // 处理表别名，转换为真实表名
        cond.op = convert_sv_comp_op(expr->op);                // 转换比较操作符

        // 处理右侧操作数，可能是值、列引用或算术表达式
        //! where条件暂时不涉及表达式计算
        auto rhs_term = AnalyzeExprTerm(expr->rhs, all_cols, tab_refs);
        if (rhs_term->term_type == TermType::VALUE) {
            cond.rhs_type = ConditionRhsType::RHS_VALUE;
            cond.rhs_val = rhs_term->val;
            if (cond.op == CompOp::OP_EQ) {
                col_values[cond.lhs_col] = cond.rhs_val;  // 记录列值
            }
        } else if (rhs_term->term_type == TermType::COLUMN) {
            if (col_values.count(rhs_term->col)) {
                // 如果是等于操作且右侧列已经有值，直接使用已记录的值
                cond.rhs_type = ConditionRhsType::RHS_VALUE;
                cond.rhs_val = col_values[rhs_term->col];
            } else {
                // 否则，设置为列引用
                cond.rhs_type = ConditionRhsType::RHS_COLUMN;
                cond.rhs_col = rhs_term->col;  // 已经由 AnalyzeExprTerm 处理过别名和检查
            }
        } else if (rhs_term->term_type == TermType::EXPR) {
            cond.rhs_type = ConditionRhsType::RHS_EXPR;
            cond.rhs_expr = rhs_term->expr;
        } else {
            throw InternalError("Unsupported expression term type in condition analysis");
        }

        conds.push_back(cond);  // 添加条件到结果集
    }
}
/**
 * @brief 检查条件表达式的有效性及类型兼容性
 *
 * 该函数检查WHERE或JOIN条件中涉及的列是否存在，以及比较操作的两侧是否类型兼容。
 * 对于每个条件，确保左右两侧的类型相同或都是数值类型(INT和FLOAT之间可以相互比较)。
 *
 * @param tab_names 条件中涉及的表名列表
 * @param conds 需要检查的条件表达式集合(将被修改以填充完整的列信息)
 */
void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds, bool agg_check) {
    TRACE_FUNCTION
    // 获取相关表的所有列信息
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);
    check_clause_with_cols(all_cols, tab_names, conds, agg_check);
}

/**
 * @brief 检查条件表达式的有效性及类型兼容性
 *
 * 该函数检查WHERE或JOIN条件中涉及的列是否存在，以及比较操作的两侧是否类型兼容。
 * 对于每个条件，确保左右两侧的类型相同或都是数值类型(INT和FLOAT之间可以相互比较)。
 *
 * @param tab_names 条件中涉及的表名列表
 * @param conds 需要检查的条件表达式集合(将被修改以填充完整的列信息)
 */
void Analyze::check_clause_with_cols(const std::vector<ColMeta> &all_cols, const std::vector<std::string> &tab_names,
                                     std::vector<Condition> &conds, bool agg_check) {
    TRACE_FUNCTION

    // 遍历检查每个条件
    for (auto &cond : conds) {
        // 推断并验证左侧列的表名
        cond.lhs_col = check_column(all_cols, cond.lhs_col);

        // 如果右侧是列引用(而非常量值)，也需要推断和验证表名
        if (cond.rhs_type == ConditionRhsType::RHS_COLUMN) {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        ColType lhs_type;
        std::vector<ColMeta>::iterator lhs_col;
        size_t lhs_col_len = 0;
        // 获取左侧列的类型信息
        if (cond.lhs_col.agg_type != AggregateType::COUNT) {
            TabMeta &lhs_tab = sm_manager.db_.get_table(cond.lhs_col.tab_name);
            lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
            lhs_type = lhs_col->type;  // 获取左侧列的类型
            if (cond.lhs_col.agg_type != AggregateType::NONE && lhs_type == ColType::TYPE_STRING) {
                // 如果是聚合函数且列类型为字符串，抛出不支持的聚合类型错误
                throw AggregateError("Unsupported aggregate type for string column: " + coltype2str(lhs_type));
            }
            lhs_col_len = lhs_col->len;
        } else {
            lhs_type = ColType::TYPE_INT;
            lhs_col_len = sizeof(int);
        }

        // 获取右侧的类型信息(可能是值或列)
        ColType rhs_type;
        if (cond.rhs_type == ConditionRhsType::RHS_VALUE) {
            // 如果右侧是常量值，初始化其原始数据，并获取类型
            cond.rhs_val.init_raw(lhs_col_len);
            rhs_type = cond.rhs_val.type;
        } else if (cond.rhs_type == ConditionRhsType::RHS_COLUMN) {
            if (cond.rhs_col.agg_type != AggregateType::COUNT) {
                // 如果右侧是列引用，获取其类型
                TabMeta &rhs_tab = sm_manager.db_.get_table(cond.rhs_col.tab_name);
                auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
                rhs_type = rhs_col->type;
                if (cond.rhs_col.agg_type != AggregateType::NONE && rhs_type == ColType::TYPE_STRING) {
                    // 如果是聚合函数且列类型为字符串，抛出不支持的聚合类型错误
                    throw AggregateError("Unsupported aggregate type for string column: " + coltype2str(rhs_type));
                }
            } else {
                rhs_type = ColType::TYPE_INT;
            }
        } else {  // rhs_type == ConditionRhsType::RHS_EXPR
            // 检查表达式内部是否都是数值类型
            // TODO: 暂时表达式涉及的列只能是左边表中的列
            // std::vector<ColMeta> ltable_cols;
            // get_all_cols({cond.lhs_col.tab_name}, ltable_cols);
            // CheckArithExprType(cond.rhs_expr->lhs, ltable_cols);
            // CheckArithExprType(cond.rhs_expr->rhs, ltable_cols);
            // 假设算术表达式的结果总是数值类型 (例如 FLOAT 用于比较)
            // 更精确的类型推断可以后续添加 (例如 INT + INT = INT, INT + FLOAT = FLOAT)
            rhs_type = ColType::TYPE_FLOAT;  // Assume float for comparison simplicity
        }

        // 允许数值类型之间的比较(INT与FLOAT)
        bool is_numeric = (lhs_type == ColType::TYPE_INT || lhs_type == ColType::TYPE_FLOAT) &&
                          (rhs_type == ColType::TYPE_INT || rhs_type == ColType::TYPE_FLOAT);

        // 如果类型不匹配且不是数值类型比较，则抛出类型不兼容错误
        if (lhs_type != rhs_type && !is_numeric) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
        if (agg_check) {
            // 如果是聚合检查，确保WHERE条件中不包含聚合列
            if (cond.lhs_col.agg_type != AggregateType::NONE ||
                (cond.rhs_type == ConditionRhsType::RHS_COLUMN && cond.rhs_col.agg_type != AggregateType::NONE)) {
                throw AggregateError("WHERE clause cannot contain aggregate columns");
            }
        }
    }
}

/**
 * @brief 将语法树中的值对象转换为系统内部的Value对象
 *
 * 该函数负责将语法分析阶段识别的不同类型的值(整数、浮点数、字符串)
 * 转换为系统内部统一的Value表示形式。
 *
 * @param sv_val 语法树中的值对象
 * @return Value 转换后的系统内部值对象
 * @throws InternalError 当遇到未知的值类型时抛出异常
 */
Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
    TRACE_FUNCTION
    Value val;
    switch (sv_val->getType()) {
        case ast::AstType::IntLit: {
            auto int_lit = std::static_pointer_cast<ast::IntLit>(sv_val);
            val.set_int(int_lit->val);
            break;
        }
        case ast::AstType::FloatLit: {
            auto float_lit = std::static_pointer_cast<ast::FloatLit>(sv_val);
            val.set_float(float_lit->val);
            break;
        }
        case ast::AstType::StringLit: {
            auto str_lit = std::static_pointer_cast<ast::StringLit>(sv_val);
            val.set_str(str_lit->val);
            break;
        }
        default:
            throw InternalError("Unexpected sv value type");
    }
    return val;
}

/**
 * @brief 将语法树中的比较操作符转换为系统内部的CompOp枚举值
 *
 * 将SQL语句中的比较操作符(=, !=, <, >, <=, >=)转换为系统内部使用的
 * CompOp枚举类型，便于后续的条件处理和优化。
 *
 * @param op 语法树中的比较操作符
 * @return CompOp 转换后的系统内部比较操作符
 * @throws InternalError 当遇到未知的比较操作符时抛出异常
 */
CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
    TRACE_FUNCTION
    switch (op) {
        case ast::SV_OP_EQ:  // 等于
            return CompOp::OP_EQ;
        case ast::SV_OP_NE:  // 不等于
            return CompOp::OP_NE;
        case ast::SV_OP_LT:  // 小于
            return CompOp::OP_LT;
        case ast::SV_OP_GT:  // 大于
            return CompOp::OP_GT;
        case ast::SV_OP_LE:  // 小于等于
            return CompOp::OP_LE;
        case ast::SV_OP_GE:  // 大于等于
            return CompOp::OP_GE;
        default:
            // 未知的比较操作符，抛出错误
            throw InternalError("Unknown comparison operator in semantic analysis");
    }
}

/**
 * @brief 将语法树中的JOIN类型转换为系统内部的JoinType枚举值
 *
 * 将SQL语句中指定的不同类型的JOIN操作(INNER JOIN, LEFT JOIN等)
 * 转换为系统内部使用的JoinType枚举类型，用于后续的连接操作处理。
 *
 * @param type 语法树中的JOIN类型
 * @return JoinType 转换后的系统内部JOIN类型
 * @throws InternalError 当遇到未知的JOIN类型时抛出异常
 */
JoinType Analyze::convert_sv_join_type(ast::JoinType type) {
    TRACE_FUNCTION
    switch (type) {
        case ast::JoinType::SV_INNER_JOIN:  // 内连接
            return JoinType::INNER_JOIN;
        case ast::JoinType::SV_LEFT_JOIN:  // 左外连接
            return JoinType::LEFT_JOIN;
        case ast::JoinType::SV_RIGHT_JOIN:  // 右外连接
            return JoinType::RIGHT_JOIN;
        case ast::JoinType::SV_FULL_JOIN:  // 全外连接
            return JoinType::FULL_JOIN;
        case ast::JoinType::SV_SEMI_JOIN:  // 半连接
            return JoinType::SEMI_JOIN;
        default:
            // 未知的JOIN类型，抛出错误
            throw InternalError("Unknown join type in semantic analysis");
    }
}

void Analyze::check_having_conds(const std::vector<Condition> &conds, const std::vector<TabCol> &group_cols) {
    TRACE_FUNCTION
    for (auto &cond : conds) {
        if (cond.lhs_col.agg_type == AggregateType::NONE) {
            bool found = is_column_in_cols(cond.lhs_col, group_cols);
            if (!found) {
                throw AggregateError("having_cond: Non aggregate column not in group by");
            }
        }
        if (cond.rhs_type == ConditionRhsType::RHS_COLUMN && cond.rhs_col.agg_type == AggregateType::NONE) {
            bool found = is_column_in_cols(cond.rhs_col, group_cols);
            if (!found) {
                throw AggregateError("having_cond: Non aggregate column not in group by");
            }
        }
    }
}

void Analyze::check_orderby_with_group(const std::vector<OrderbyInfo> &order_bys,
                                       const std::vector<TabCol> &select_cols, const std::vector<TabCol> &group_cols) {
    TRACE_FUNCTION
    if (group_cols.empty()) {
        // 如果没有GROUP BY子句，ORDER BY只能包含无聚合值
        bool has_aggr = std::any_of(order_bys.begin(), order_bys.end(),
                                    [](const OrderbyInfo &info) { return info.col.agg_type != AggregateType::NONE; });
        if (has_aggr) {
            throw AggregateError("ORDER BY cannot contain aggregate columns without GROUP BY");
        }
        return;
    }
    for (const auto &order_by_info : order_bys) {
        const auto &order_col = order_by_info.col;
        bool found_in_select = is_column_in_cols(order_col, select_cols);
        bool found_in_group = is_column_in_cols(order_col, group_cols);

        if (!found_in_select && !found_in_group) {
            throw AggregateError("ORDER BY column not in SELECT list or GROUP BY clause: " + order_col.col_name);
        }
    }
}

/**
 * @brief 将语法树中的算术操作符转换为系统内部的 ArithOp
 * @param op 语法树中的算术操作符
 * @return 系统内部的 ArithOp 枚举值
 */
ArithOp Analyze::convert_sv_arith_op(ast::SvArithOp op) {
    switch (op) {
        case ast::SV_ARITH_PLUS:
            return ArithOp::OP_PLUS;
        case ast::SV_ARITH_MINUS:
            return ArithOp::OP_MINUS;
        case ast::SV_ARITH_MULTIPLY:
            return ArithOp::OP_MULTIPLY;
        case ast::SV_ARITH_DIVIDE:
            return ArithOp::OP_DIVIDE;
        default:
            throw InternalError("Unknown arithmetic operator in semantic analysis");
    }
}

/**
 * @brief 递归分析语法树中的表达式节点 (Value, Col, ArithExpr) 并转换为 ExprTerm
 * @param ast_expr 语法树中的表达式节点
 * @param all_cols 所有相关表的列元数据
 * @param tab_refs 查询涉及的表引用
 * @return 转换后的 ExprTerm 对象
 */
std::shared_ptr<ExprTerm> Analyze::AnalyzeExprTerm(const std::shared_ptr<ast::Expr> &ast_expr,
                                                   const std::vector<ColMeta> &all_cols,
                                                   const std::vector<TabRef> &tab_refs) {
    TRACE_FUNCTION

    if (!ast_expr) {
        throw InternalError("Null expression encountered during analysis");
    }

    switch (ast_expr->getType()) {
        case ast::AstType::IntLit:
        case ast::AstType::FloatLit:
        case ast::AstType::StringLit:
        case ast::AstType::BoolLit: {
            auto sv_val = std::static_pointer_cast<ast::Value>(ast_expr);
            Value common_val = convert_sv_value(sv_val);
            return std::make_shared<ExprTerm>(common_val);
        }
        case ast::AstType::Col: {
            auto sv_col = std::static_pointer_cast<ast::Col>(ast_expr);
            TabCol common_col = {"", sv_col->col_name, sv_col->tab_name};
            common_col.set_col_alias(sv_col->alias);
            common_col.set_agg_type(sv_col->aggregate_type);
            convert_tabname(all_cols, common_col, tab_refs);
            check_column(all_cols, common_col);
            return std::make_shared<ExprTerm>(common_col);
        }
        case ast::AstType::ArithExpr: {
            auto sv_arith_expr = std::static_pointer_cast<ast::ArithExpr>(ast_expr);
            auto lhs_term = AnalyzeExprTerm(sv_arith_expr->lhs, all_cols, tab_refs);
            auto rhs_term = AnalyzeExprTerm(sv_arith_expr->rhs, all_cols, tab_refs);
            ArithOp common_op = convert_sv_arith_op(sv_arith_expr->op);
            auto common_arith_expr = std::make_shared<ArithExpr>(lhs_term, common_op, rhs_term);
            return std::make_shared<ExprTerm>(common_arith_expr);
        }
        default:
            throw InternalError("Unsupported expression type encountered during analysis");
    }
}
/**
 * @brief 递归检查算术表达式中的项是否都是数值类型
 * @param term 要检查的表达式项
 * @param all_cols 所有相关表的列元数据
 * @throw IncompatibleTypeError 如果发现非数值类型
 */
void Analyze::CheckArithExprType(std::shared_ptr<ExprTerm> term, const std::vector<ColMeta> &all_cols) {
    if (!term) {
        throw InternalError("Null expression term encountered during type checking");
    }

    switch (term->term_type) {
        case TermType::VALUE:
            if (term->val.type == ColType::TYPE_STRING) {
                throw IncompatibleTypeError("Arithmetic expression", coltype2str(term->val.type));
            }
            term->val.init_raw();
            break;
        case TermType::COLUMN: {
            // 如果是列检查是不是在全部列中，
            // TODO: 可能需要更加详细的检查，比如防止出现别的表的列
            bool found = false;
            for (const auto &col_meta : all_cols) {
                // Need to compare based on resolved table name, not alias
                if (col_meta.tab_name == term->col.tab_name && col_meta.name == term->col.col_name) {
                    if (col_meta.type != ColType::TYPE_INT && col_meta.type != ColType::TYPE_FLOAT) {
                        throw IncompatibleTypeError("Arithmetic expression", coltype2str(col_meta.type));
                    }
                    found = true;
                    break;  // Found the column
                }
            }
            if (!found) {
                // This should ideally not happen if check_column was called before, but as a safeguard:
                throw ColumnNotFoundError(term->col.to_string());
            }
            break;
        }
        case TermType::EXPR:
            if (!term->expr) {
                throw InternalError("Null nested expression encountered during type checking");
            }
            // Recursively check left and right operands
            CheckArithExprType(term->expr->lhs, all_cols);
            CheckArithExprType(term->expr->rhs, all_cols);
            break;
        default:
            throw InternalError("Unknown expression term type during type checking");
    }
}
