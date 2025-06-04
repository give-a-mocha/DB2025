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
    std::cerr << "DEBUG: Starting semantic analysis..." << std::endl;
    std::shared_ptr<Query> query = std::make_shared<Query>();          // 创建查询对象
    if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse)) {  // 处理SELECT查询
        // 检查所有表是否存在并处理表名和别名
        std::vector<TabRef> tab_refs;  // 存储表引用及其别名
        tab_refs.reserve(x->tabs.size());
        query->tables.reserve(x->tabs.size());
        for (const auto &sv_tab : x->tabs) {
            std::string tab_name = sv_tab->tab_name;
            tab_refs.push_back(TabRef(tab_name, sv_tab->alias));  // 添加表引用
            if (!sm_manager_->db_.is_table(tab_name)) {           // 检查表是否存在
                throw TableNotFoundError(tab_name);
            }
            query->tables.push_back(tab_name);  // 添加到查询的表列表
        }

        // 处理JOIN表
        std::vector<std::string> semi_join_tables;
        std::vector<TabRef> semi_join_tableRefs;
        
        if (!x->jointree.empty()) {
            for (const auto &join_expr : x->jointree) {
                // 检查JOIN右侧的表是否存在
                std::string right_tab_name = join_expr->right->tab_name;
                if (!sm_manager_->db_.is_table(right_tab_name)) {
                    throw TableNotFoundError(right_tab_name);
                }
                TabRef right_table(right_tab_name, join_expr->right->alias);
                bool isSemiJoin = (convert_sv_join_type(join_expr->type) == JoinType::SEMI_JOIN);
                // 在做列检查时不需要把半连接的表加入
                if(isSemiJoin) {
                    // 半连接表
                    semi_join_tables.push_back(right_tab_name);  
                    semi_join_tableRefs.push_back(right_table);
                } else {
                    // 普通JOIN表
                    query->tables.push_back(right_tab_name);
                    tab_refs.push_back(right_table);
                }
            }
        }

        // 获取所有相关表的全部列
        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);

        // 处理要查询的列：
        // 情况1: 如果没有明确指定列(SELECT *)，则查询所有列
        if (x->cols.empty()) {
            query->cols.reserve(all_cols.size());
            for (auto &col : all_cols) {
                TabCol sel_col = {col.tab_name, col.name};     // 创建表列引用
                convert_tabname(all_cols, sel_col, tab_refs);  // 处理表名和别名
                query->cols.push_back(sel_col);                // 添加到查询列表
            }
        }
        // 情况2: 查询指定的列
        else {
            query->cols.reserve(x->cols.size());
            for (auto &sv_sel_col : x->cols) {
                // 创建列引用，初始状态下表名可能为空
                TabCol sel_col = {"", sv_sel_col->col_name, sv_sel_col->tab_name};
                convert_tabname(all_cols, sel_col, tab_refs);  // 处理表名和别名
                query->cols.push_back(sel_col);                // 添加到查询列表
            }
        }

        // 处理条件，加入半连接的表
        for(auto &semi_join_table : semi_join_tables) {
            query->tables.push_back(semi_join_table);
        }
        for(auto &semi_join_tableRef : semi_join_tableRefs) {
            tab_refs.push_back(semi_join_tableRef);
        }
        get_all_cols(semi_join_tables, all_cols);

        // 处理WHERE条件子句
        get_clause_alias(all_cols, x->conds, query->conds, tab_refs);
        check_clause(query->tables, query->conds);  // 检查WHERE条件的有效性

        // 处理JOIN操作及其条件
        if (!x->jointree.empty()) {
            query->jointree.reserve(x->jointree.size());
            for (auto &join_expr : x->jointree) {
                // 转换JOIN条件
                std::string right_tab_name = join_expr->right->tab_name;
                std::vector<Condition> join_conds;
                get_clause_alias(all_cols, join_expr->conds, join_conds, tab_refs);

                // 创建JOIN节点并指定JOIN类型
                JoinType join_type = convert_sv_join_type(join_expr->type);
                query->jointree.emplace_back(right_tab_name, join_conds, join_type);
            }
        }

        // 校验所有JOIN条件的有效性
        for (auto &join_node : query->jointree) {
            check_clause(query->tables, join_node.join_conds);
        }
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {  // 处理UPDATE查询
        // 添加被更新的表
        query->tables.push_back(x->tab_name);

        // 准备SET子句
        query->set_clauses.reserve(x->set_clauses.size());
        // 处理每个SET赋值
        for (auto &sv_set_clause : x->set_clauses) {
            SetClause set_clause;
            set_clause.lhs.tab_name = x->tab_name;                  // 设置左侧列的表名
            set_clause.lhs.col_name = sv_set_clause->col_name;      // 设置左侧列名
            set_clause.rhs = convert_sv_value(sv_set_clause->val);  // 转换右侧值
            query->set_clauses.push_back(set_clause);               // 添加到SET子句列表
        }

        // 获取相关表的所有列，用于后续校验
        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);

        // 处理WHERE条件
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);  // 检查WHERE条件的有效性

        // 检查每个SET子句的有效性和类型兼容性
        for (auto &set_clause : query->set_clauses) {
            set_clause.lhs = check_column(all_cols, set_clause.lhs);  // 检查列是否存在
            // 检查赋值类型的兼容性
            TabMeta &tab = sm_manager_->db_.get_table(set_clause.lhs.tab_name);
            auto col = tab.get_col(set_clause.lhs.col_name);
            // 允许数值类型之间的转换(INT与FLOAT)
            bool is_numeric = (col->type == ColType::TYPE_INT || col->type == ColType::TYPE_FLOAT) &&
                              (set_clause.rhs.type == ColType::TYPE_INT || set_clause.rhs.type == ColType::TYPE_FLOAT);
            if (col->type != set_clause.rhs.type && !is_numeric) {
                throw IncompatibleTypeError(coltype2str(col->type), coltype2str(set_clause.rhs.type));
            }
            set_clause.rhs.init_raw(col->len);  // 初始化值的原始数据
        }
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {  // 处理DELETE查询
        // 添加要删除数据的表名
        query->tables.push_back(x->tab_name);
        // 处理WHERE条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);                            // 检查WHERE条件的有效性
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {  // 处理INSERT查询
        // 处理INSERT的VALUES值
        for (auto &sv_val : x->vals) {
            query->values.push_back(convert_sv_value(sv_val));  // 转换并添加每个插入值
        }
    } else {
        // 其他类型的SQL语句，不做处理
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

/**
 * @brief 检查并验证目标列是否存在，并处理表名推断
 *
 * @param all_cols 所有相关表的列元数据集合
 * @param target 需要检查的目标列引用
 * @return TabCol 验证后的列引用
 */
TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
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
    for (auto &sel_tab_name : tab_names) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
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
    conds.clear();                   // 清空输出条件向量
    conds.reserve(sv_conds.size());  // 预分配空间以提高性能
    for (auto &expr : sv_conds) {
        Condition cond;
        // 处理条件左侧的列引用
        cond.lhs_col.tab_alias = expr->lhs->tab_name;       // 设置左侧列的表别名
        cond.lhs_col.col_name = expr->lhs->col_name;        // 设置左侧列名
        convert_tabname(all_cols, cond.lhs_col, tab_refs);  // 处理表别名，转换为真实表名
        cond.op = convert_sv_comp_op(expr->op);             // 转换比较操作符

        // 处理右侧操作数，可能是值或列引用
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
            // 右侧是常量值
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);  // 转换右侧值
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
            // 右侧是列引用
            cond.is_rhs_val = false;
            cond.rhs_col.tab_alias = rhs_col->tab_name;         // 设置右侧列的表别名
            cond.rhs_col.col_name = rhs_col->col_name;          // 设置右侧列名
            convert_tabname(all_cols, cond.rhs_col, tab_refs);  // 处理表别名，转换为真实表名
        }
        conds.push_back(cond);  // 添加条件到结果集
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
        cond.lhs_col.tab_name = expr->lhs->tab_name;
        cond.lhs_col.col_name = expr->lhs->col_name;
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
            cond.rhs_col.tab_name = rhs_col->tab_name;
            cond.rhs_col.col_name = rhs_col->col_name;
        }

        // 将转换后的条件添加到结果集中
        conds.push_back(cond);
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
void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds) {
    // 获取相关表的所有列信息
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);

    // 遍历检查每个条件
    for (auto &cond : conds) {
        // 推断并验证左侧列的表名
        cond.lhs_col = check_column(all_cols, cond.lhs_col);

        // 如果右侧是列引用(而非常量值)，也需要推断和验证表名
        if (!cond.is_rhs_val) {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }

        // 获取左侧列的类型信息
        TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;

        // 获取右侧的类型信息(可能是值或列)
        ColType rhs_type;
        if (cond.is_rhs_val) {
            // 如果右侧是常量值，初始化其原始数据，并获取类型
            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            // 如果右侧是列引用，获取其类型
            TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }

        // 允许数值类型之间的比较(INT与FLOAT)
        bool is_numeric = (lhs_type == ColType::TYPE_INT || lhs_type == ColType::TYPE_FLOAT) &&
                          (rhs_type == ColType::TYPE_INT || rhs_type == ColType::TYPE_FLOAT);

        // 如果类型不匹配且不是数值类型比较，则抛出类型不兼容错误
        if (lhs_type != rhs_type && !is_numeric) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
    }
}

/**
 * @brief 使用自定义列检查器验证条件表达式的有效性及类型兼容性
 *
 * 该函数是check_clause的重载版本，接受一个列检查器对象来进行列存在性验证，
 * 适用于需要自定义列检查逻辑的场景。
 *
 * @param tab_names 条件中涉及的表名列表
 * @param conds 需要检查的条件表达式集合(将被修改以填充完整的列信息)
 * @param col_check 自定义的列检查器对象
 */
void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds,
                           ColCheck &col_check) {
    // 遍历检查每个条件
    for (auto &cond : conds) {
        // 使用自定义检查器推断并验证左侧列
        cond.lhs_col = col_check.check(cond.lhs_col);

        // 如果右侧是列引用(而非常量值)，也需要使用自定义检查器进行验证
        if (!cond.is_rhs_val) {
            cond.rhs_col = col_check.check(cond.rhs_col);
        }

        // 获取左侧列的类型信息
        TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;

        // 获取右侧的类型信息(可能是值或列)
        ColType rhs_type;
        if (cond.is_rhs_val) {
            // 如果右侧是常量值，初始化其原始数据，并获取类型
            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            // 如果右侧是列引用，获取其类型
            TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }

        // 允许数值类型之间的比较(INT与FLOAT)
        bool is_numeric = (lhs_type == ColType::TYPE_INT || lhs_type == ColType::TYPE_FLOAT) &&
                          (rhs_type == ColType::TYPE_INT || rhs_type == ColType::TYPE_FLOAT);

        // 如果类型不匹配且不是数值类型比较，则抛出类型不兼容错误
        if (lhs_type != rhs_type && !is_numeric) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
    }
}

/**
 * @brief 将语法树中的值对象转换为系统内部的Value对象
 *
 * 根据语法树中值的类型(整数、浮点数或字符串)，创建并返回相应的Value对象。
 * 如果无法识别值的类型，则抛出内部错误。
 *
 * @param sv_val 语法树中的值对象
 * @return Value 转换后的系统内部值对象
 */
Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
    Value val;
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
        // 整数类型值
        val.set_int(int_lit->val);
    } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
        // 浮点数类型值
        val.set_float(float_lit->val);
    } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
        // 字符串类型值
        val.set_str(str_lit->val);
    } else {
        // 未知类型值，抛出错误
        throw InternalError("Unexpected sv value type");
    }
    return val;
}

/**
 * @brief 将语法树中的比较操作符转换为系统内部的CompOp枚举值
 *
 * 将语法分析阶段识别的比较操作符(如等于、不等于、大于、小于等)
 * 转换为系统内部使用的CompOp枚举类型，方便后续处理。
 *
 * @param op 语法树中的比较操作符
 * @return CompOp 转换后的系统内部比较操作符
 */
CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
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
 * 将SQL语句中指定的JOIN类型(如INNER JOIN、LEFT JOIN等)
 * 转换为系统内部使用的JoinType枚举类型，用于后续的JOIN操作处理。
 *
 * @param type 语法树中的JOIN类型
 * @return JoinType 转换后的系统内部JOIN类型
 */
JoinType Analyze::convert_sv_join_type(ast::JoinType type) {
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