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
#include "common/print.hpp"

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse) {
    std::cerr << "DEBUG: Starting semantic analysis..." << std::endl;
    std::shared_ptr<Query> query = std::make_shared<Query>();
    if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse)) {
        
        /** TODO: 检查表是否存在 */
        // 检查所有表是否存在
        // 处理表名
        std::vector<TabRef> tab_refs;
        tab_refs.reserve(x->tabs.size());
        query->tables.reserve(x->tabs.size());
        for (const auto &sv_tab : x->tabs) {
            std::string tab_name = sv_tab->tab_name;
            tab_refs.push_back(TabRef(tab_name, sv_tab->alias));
            if (!sm_manager_->db_.is_table(tab_name)) {
                throw TableNotFoundError(tab_name);
            }
            query->tables.push_back(tab_name);
        }

        // 处理JOIN表达式
        if (!x->jointree.empty()) {
            query->jointree.reserve(x->jointree.size());
            for (const auto &join_expr : x->jointree) {
                // 检查右表是否存在
                std::string right_tab_name = join_expr->right->tab_name;
                if (!sm_manager_->db_.is_table(right_tab_name)) {
                    throw TableNotFoundError(right_tab_name);
                }
                
                TabRef right_table(right_tab_name, join_expr->right->alias);

                // 添加右表到表列表和tab_refs
                query->tables.push_back(right_tab_name);
                tab_refs.push_back(right_table);
                
                // 转换JOIN条件
                std::vector<Condition> join_conds;
                get_clause_alias(join_expr->conds, join_conds, tab_refs);
                
                // 创建JoinNode
                JoinType join_type = convert_sv_join_type(join_expr->type);
                query->jointree.emplace_back(right_tab_name, join_conds, join_type);
            }
        }

        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);
        // 如果没有指定列，比如*则查询所有列
        if(x->cols.empty()){
            query->cols.reserve(all_cols.size());
            for (auto &col : all_cols) {
                TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};
                query->cols.push_back(sel_col);
            }
        }else{
            //把列加入并进行校验，添加表名
            query->cols.reserve(x->cols.size());
            for (auto &sv_sel_col : x->cols) {
                TabCol sel_col = {.tab_name = sv_sel_col->tab_name, .col_name = sv_sel_col->col_name};
                convert_tabname(sel_col, tab_refs);
                // 列元数据校验
                sel_col = check_column(all_cols, sel_col); 
                query->cols.push_back(sel_col);
            }
        }
        // 处理where条件
        get_clause_alias(x->conds, query->conds, tab_refs);
        check_clause(query->tables, query->conds);
        
        // 校验JOIN条件
        for (auto &join_node : query->jointree) {
            check_clause(query->tables, join_node.join_conds);
        }
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
        

        //处理where条件
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);
        
        for (auto &set_clause : query->set_clauses) {
            set_clause.lhs = check_column(all_cols, set_clause.lhs);
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
        // 处理where条件
        query->tables.push_back(x->tab_name);
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);
        
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
        // 处理insert 的values值
        for (auto &sv_val : x->vals) {
            query->values.push_back(convert_sv_value(sv_val));
        }
    } else {
        // do nothing
        LOG("analyze: do nothing");
    }
    query->parse = std::move(parse);
    return query;
}
// 如果将表的别名转为真名
void Analyze::convert_tabname(TabCol &target, const std::vector<TabRef> &tab_refs) {
    if(target.tab_name.empty()) {
        return ;
    } else {
        std::string tab_name = target.tab_name;
        int cnt = 0;
        for (const auto &tab_ref : tab_refs) {
            if (tab_ref.get_name() == target.tab_name) {
                cnt++;
                tab_name = tab_ref.name;
                if(cnt > 1){
                    throw AmbiguousColumnError(target.col_name);
                }
            }
        }
        if(cnt == 0){
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;
        return ;
    }
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
// where条件转换，表别名转换
void Analyze::get_clause_alias(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds, const std::vector<TabRef> &tab_refs){
    conds.clear();
    conds.reserve(sv_conds.size());
    for (auto &expr : sv_conds) {
        Condition cond;
        cond.lhs_col = {.tab_name = expr->lhs->tab_name, .col_name = expr->lhs->col_name};
        convert_tabname(cond.lhs_col, tab_refs);
        cond.op = convert_sv_comp_op(expr->op);
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
            convert_tabname(cond.rhs_col, tab_refs);
        }
        conds.push_back(cond);
    }
}

/**
 * @brief 将抽象语法树中的二元表达式转换为查询条件
 * 
 * 该函数负责解析WHERE子句中的条件表达式，并将其转换为系统内部使用的Condition对象。
 * 支持处理列与值的比较以及列与列之间的比较。
 *
 * @param sv_conds 输入的语法树中的二元表达式集合
 * @param conds 输出参数，用于存储转换后的条件对象
 */
void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds) {
    // 清空输出条件向量
    conds.clear();
    // 预留足够空间以避免后续插入时的内存重分配
    conds.reserve(sv_conds.size());
    
    // 遍历每个二元表达式并转换为条件对象
    for (auto &expr : sv_conds) {
        Condition cond;
        // 处理左侧操作数(通常是列)
        cond.lhs_col = {.tab_name = expr->lhs->tab_name, .col_name = expr->lhs->col_name};
        // 转换比较操作符(如 =, <, >, !=等)
        cond.op = convert_sv_comp_op(expr->op);
        
        // 处理右侧操作数，可能是值或列
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
            // 右侧是常量值
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
            // 右侧是列名
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
        }
        
        // 将转换后的条件添加到结果集中
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
        bool is_numeric =
            (lhs_type == ColType::TYPE_INT || lhs_type == ColType::TYPE_FLOAT) && (rhs_type == ColType::TYPE_INT || rhs_type == ColType::TYPE_FLOAT);
        if (lhs_type != rhs_type && !is_numeric) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
    }
}

void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds, ColCheck &col_check) {
    for (auto &cond : conds) {
        // Infer table name from column name
        cond.lhs_col = col_check.check(cond.lhs_col);
        if (!cond.is_rhs_val) {
            cond.rhs_col = col_check.check(cond.rhs_col);
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
        bool is_numeric =
            (lhs_type == ColType::TYPE_INT || lhs_type == ColType::TYPE_FLOAT) && (rhs_type == ColType::TYPE_INT || rhs_type == ColType::TYPE_FLOAT);
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
JoinType Analyze::convert_sv_join_type(ast::JoinType type) {
    switch (type) {
        case ast::JoinType::SV_INNER_JOIN:
            return JoinType::INNER_JOIN;
        case ast::JoinType::SV_LEFT_JOIN:
            return JoinType::LEFT_JOIN;
        case ast::JoinType::SV_RIGHT_JOIN:
            return JoinType::RIGHT_JOIN;
        case ast::JoinType::SV_FULL_JOIN:
            return JoinType::FULL_JOIN;
        default:
            throw InternalError("Unknown join type in semantic analysis");
    }
}