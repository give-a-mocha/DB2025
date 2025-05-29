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

#include <set>

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse) {
    std::cerr << "DEBUG: Starting semantic analysis..." << std::endl;
    std::shared_ptr<Query> query = std::make_shared<Query>();
    if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse)) {
        // 处理表名
        query->tables = std::move(x->tabs);
        /** TODO: 检查表是否存在 */
        // 检查所有表是否存在
        for (const auto &tab_name : query->tables) {
            if (!sm_manager_->db_.is_table(tab_name)) {
                throw TableNotFoundError(tab_name);
            }
        }

        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);
        ColCheck col_check(all_cols);

        // 如果没有指定列，比如*则查询所有列
        if (x->cols.empty()) {
            query->cols.reserve(all_cols.size());
            for (auto &col : all_cols) {
                TabCol sel_col = {.tab_name = col.tab_name,
                                  .col_name = col.name,
                                  .as_name = col.name,
                                  .aggregate = AggregateType::NONE};
                query->cols.push_back(sel_col);
            }
        } else {
            // 把列加入并进行校验，添加表名
            query->cols.reserve(x->cols.size());
            for (auto &sv_sel_col : x->cols) {
                TabCol sel_col = {.tab_name = sv_sel_col->tab_name,
                                  .col_name = sv_sel_col->col_name,
                                  .as_name = sv_sel_col->as_name,
                                  .aggregate = sv_sel_col->aggregate};
                if (sel_col.aggregate == AggregateType::AGG_COUNT &&
                    (sel_col.col_name.empty() || sel_col.col_name == "*")) {
                    sel_col.col_name = "*";  // COUNT(*)特殊处理
                }
                // !初始代码 列元数据校验
                // sel_col = check_column(all_cols, sel_col);
                sel_col = col_check.check(sel_col);
                query->cols.push_back(sel_col);
            }
        }

        // 处理group by
        for (auto &sv_group_col : x->group) {
            TabCol group_col = {.tab_name = sv_group_col->cols->tab_name,
                                .col_name = sv_group_col->cols->col_name,
                                .as_name = sv_group_col->cols->as_name,
                                .aggregate = sv_group_col->cols->aggregate};
            query->group_cols.push_back(group_col);
            // group by 不能使用聚合函数
            if (group_col.aggregate != AggregateType::NONE) {
                throw InternalError("GROUP BY clause cannot contain aggregate functions");
            }
        }
        // 如果有group by，检查group by的列是否存在
        for (auto &group_col : query->group_cols) {
            group_col = col_check.check(group_col);
        }
        // 语义检查：SELECT列表中的非聚集列必须在GROUP BY中
        check_group_by_semantics(query->cols, query->group_cols);

        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds, col_check);
        // 检查WHERE子句中不能使用聚集函数
        check_where_aggregates(query->conds);

        // 处理 HAVING 子句
        get_clause(x->having_conds, query->having_conds);
        check_clause(query->tables, query->having_conds, col_check);
        // 检查having条件中是否有不是聚合函数也不是group by的列
        check_having_conds(query->having_conds, query->group_cols);
        // 检测group by不存在时，是否有having条件
        check_without_group(query->group_cols, query->having_conds);
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {
        /** TODO: */
        // 处理表名
        query->tables.push_back(x->tab_name);

        // 检查表是否存在
        query->set_clauses.reserve(x->set_clauses.size());
        // 处理set子句
        for (auto &sv_set_clause : x->set_clauses) {
            SetClause set_clause;
            set_clause.lhs = {.tab_name = x->tab_name, .col_name = sv_set_clause->col_name};
            set_clause.rhs = convert_sv_value(sv_set_clause->val);
            query->set_clauses.push_back(set_clause);
        }

        // 检查set子句中的列是否存在并进行类型校验
        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);
        ColCheck col_check(all_cols);

        //! 初始代码,处理where条件
        // get_clause(x->conds, query->conds);
        // check_clause(query->tables, query->conds);

        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds, col_check);
        // 检查WHERE子句中不能使用聚集函数
        check_where_aggregates(query->conds);

        for (auto &set_clause : query->set_clauses) {
            //! 初始代码
            // set_clause.lhs = check_column(all_cols, set_clause.lhs);
            set_clause.lhs = col_check.check(set_clause.lhs);
            // 检查类型兼容性
            TabMeta &tab = sm_manager_->db_.get_table(set_clause.lhs.tab_name);
            auto col = tab.get_col(set_clause.lhs.col_name);
            // Allow numeric type assignment (INT vs FLOAT)
            bool is_numeric = (col->type == ColType::TYPE_INT || col->type == ColType::TYPE_FLOAT) &&
                              (set_clause.rhs.type == ColType::TYPE_INT || set_clause.rhs.type == ColType::TYPE_FLOAT);
            if (col->type != set_clause.rhs.type && !is_numeric) {
                throw IncompatibleTypeError(coltype2str(col->type), coltype2str(set_clause.rhs.type));
            }
            set_clause.rhs.init_raw(col->len);
        }
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
        // !初始代码
        // 处理where条件
        // get_clause(x->conds, query->conds);
        // check_clause({x->tab_name}, query->conds);

        query->tables.push_back(x->tab_name);
        get_clause(x->conds, query->conds);
        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);
        ColCheck col_check(all_cols);
        check_clause(query->tables, query->conds, col_check);
        // 检查WHERE子句中不能使用聚集函数
        check_where_aggregates(query->conds);

    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
        // 处理insert 的values值
        for (auto &sv_val : x->vals) {
            query->values.push_back(convert_sv_value(sv_val));
        }
    } else {
        // do nothing
    }
    query->parse = std::move(parse);
    return query;
}

TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
    if (target.tab_name.empty()) {
        // Table name not specified, infer table name from column name
        std::string tab_name;
        for (auto &col : all_cols) {
            if (col.name == target.col_name) {
                if (!tab_name.empty()) {
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
            }
        }
        if (tab_name.empty()) {
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;
    } else {
        /** TODO: Make sure target column exists */
        int count = 0;
        for (auto &col : all_cols) {  // 遍历查找是否存在以及是否重复
            if (col.name == target.col_name && col.tab_name == target.tab_name) {
                count++;
                if (count > 1) {
                    throw AmbiguousColumnError(target.col_name);
                }
            }
        }
        if (count == 0) {  // 如果未能找到
            throw ColumnNotFoundError(target.col_name);
        }
    }
    return target;
}

void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols) {
    for (auto &sel_tab_name : tab_names) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
}

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds) {
    conds.clear();
    conds.reserve(sv_conds.size());
    for (auto &expr : sv_conds) {
        Condition cond;
        cond.lhs_col = {.tab_name = expr->lhs->tab_name,
                        .col_name = expr->lhs->col_name,
                        .as_name = expr->lhs->as_name,
                        .aggregate = expr->lhs->aggregate};
        cond.op = convert_sv_comp_op(expr->op);
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = rhs_col->tab_name,
                            .col_name = rhs_col->col_name,
                            .as_name = rhs_col->as_name,
                            .aggregate = rhs_col->aggregate};
        }
        conds.push_back(cond);
    }
}

void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds) {
    // auto all_cols = get_all_cols(tab_names);
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);
    // Get raw values in where clause
    for (auto &cond : conds) {
        // Infer table name from column name
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        if (!cond.is_rhs_val) {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }
        // Allow numeric type comparison (INT vs FLOAT)
        bool is_numeric = (lhs_type == ColType::TYPE_INT || lhs_type == ColType::TYPE_FLOAT) &&
                          (rhs_type == ColType::TYPE_INT || rhs_type == ColType::TYPE_FLOAT);
        if (lhs_type != rhs_type && !is_numeric) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
    }
}

void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds,
                           ColCheck &col_check) {
    for (auto &cond : conds) {
        // Infer table name from column name
        cond.lhs_col = col_check.check(cond.lhs_col);
        if (!cond.is_rhs_val) {
            cond.rhs_col = col_check.check(cond.rhs_col);
        }
        if (cond.lhs_col.aggregate == AggregateType::AGG_COUNT && cond.lhs_col.col_name == "*") {
            // COUNT(*)特殊处理
            return;
        }
        TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }
        // Allow numeric type comparison (INT vs FLOAT)
        bool is_numeric = (lhs_type == ColType::TYPE_INT || lhs_type == ColType::TYPE_FLOAT) &&
                          (rhs_type == ColType::TYPE_INT || rhs_type == ColType::TYPE_FLOAT);
        if (lhs_type != rhs_type && !is_numeric) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
    }
}

Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
    Value val;
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
        val.set_int(int_lit->val);
    } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
        val.set_float(float_lit->val);
    } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
        val.set_str(str_lit->val);
    } else {
        throw InternalError("Unexpected sv value type");
    }
    return val;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
    switch (op) {
        case ast::SV_OP_EQ:
            return CompOp::OP_EQ;
        case ast::SV_OP_NE:
            return CompOp::OP_NE;
        case ast::SV_OP_LT:
            return CompOp::OP_LT;
        case ast::SV_OP_GT:
            return CompOp::OP_GT;
        case ast::SV_OP_LE:
            return CompOp::OP_LE;
        case ast::SV_OP_GE:
            return CompOp::OP_GE;
        default:
            throw InternalError("Unknown comparison operator in semantic analysis");
    }
}

/**
 * @description: 检查WHERE子句中不能使用聚集函数
 */
void Analyze::check_where_aggregates(std::vector<Condition> &conds) {
    for (const auto &cond : conds) {
        // 检查左侧列是否使用了聚集函数
        if (cond.lhs_col.aggregate != AggregateType::NONE) {
            throw InternalError("WHERE子句中不能使用聚集函数: " + aggregate2str(cond.lhs_col.aggregate) + "(" +
                                cond.lhs_col.col_name + ")");
        }
    }
}

/**
 * @description: 检查GROUP BY语义：SELECT列表中的非聚集列必须在GROUP BY中
 */
void Analyze::check_group_by_semantics(const std::vector<TabCol> &select_cols,
                                       const std::vector<TabCol> &group_by_clause_cols) {
    // 如果没有GROUP BY子句，则不需要检查
    if (group_by_clause_cols.empty()) {
        // 如果没有GROUP BY子句，则SELECT列表中的所有列必须全是非聚集函数或者聚集函数
        bool has_aggr = std::any_of(select_cols.begin(), select_cols.end(),
                                    [](const TabCol &col) { return col.aggregate != AggregateType::NONE; });
        bool has_non_aggr = std::any_of(select_cols.begin(), select_cols.end(),
                                        [](const TabCol &col) { return col.aggregate == AggregateType::NONE; });
        if (has_aggr && has_non_aggr) {
            throw InternalError("如果没有GROUP BY子句，则SELECT列表中的所有列必须全是非聚集函数或者聚集函数");
        }
        return;
    }
    // 将 GROUP BY 列存入 set 以便快速查找
    std::set<std::pair<std::string, std::string>> group_by_cols_set;
    for (const auto &group_col : group_by_clause_cols) {
        group_by_cols_set.emplace(group_col.tab_name, group_col.col_name);
    }

    // 检查SELECT列表中的每一列
    for (const auto &sel_col : select_cols) {
        // 如果是聚集函数，则跳过检查
        if (sel_col.aggregate != AggregateType::NONE) {
            continue;
        }

        // 如果不是聚集函数，则必须在GROUP BY中
        std::pair<std::string, std::string> col_pair = {sel_col.tab_name, sel_col.col_name};
        if (group_by_cols_set.find(col_pair) == group_by_cols_set.end()) {
            throw InternalError("SELECT列表中的非聚集列 '" + sel_col.col_name +
                                "' 必须出现在GROUP BY子句中或用作聚合函数");
        }
    }
}

void Analyze::check_having_conds(const std::vector<Condition> &having_conds, const std::vector<TabCol> &group_cols) {
    // 将 GROUP BY 列存入 set 以便快速查找
    std::set<std::pair<std::string, std::string>> group_cols_set;
    for (const auto &group_col : group_cols) {
        group_cols_set.emplace(group_col.tab_name, group_col.col_name);
    }

    // 检查 HAVING 子句中的每个条件
    for (const auto &cond : having_conds) {
        // 如果是聚集函数，则跳过检查
        if (cond.lhs_col.aggregate != AggregateType::NONE) {
            continue;
        }

        // 如果不是聚集函数，则必须在 GROUP BY 中
        std::pair<std::string, std::string> col_pair = {cond.lhs_col.tab_name, cond.lhs_col.col_name};
        if (group_cols_set.find(col_pair) == group_cols_set.end()) {
            throw InternalError("HAVING子句中的非聚集列 '" + cond.lhs_col.col_name +
                                "' 必须出现在GROUP BY子句中或用作聚合函数");
        }
    }
}

void Analyze::check_without_group(const std::vector<TabCol> &group_cols, const std::vector<Condition> &having_conds) {
    if (group_cols.empty() && !having_conds.empty()) {
        throw InternalError("HAVING子句只能在GROUP BY子句存在时使用");
    }
}
