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

#include <memory>
#include <string>
#include <vector>

#include "common/Format.h"

namespace ast {

enum JoinType { SV_INNER_JOIN, SV_LEFT_JOIN, SV_RIGHT_JOIN, SV_FULL_JOIN, SV_SEMI_JOIN };

enum SvType { SV_TYPE_INT, SV_TYPE_FLOAT, SV_TYPE_STRING, SV_TYPE_BOOL };

enum SvCompOp { SV_OP_EQ, SV_OP_NE, SV_OP_LT, SV_OP_GT, SV_OP_LE, SV_OP_GE };

enum OrderByDir { OrderBy_DEFAULT, OrderBy_ASC, OrderBy_DESC };

enum SvArithOp { SV_ARITH_PLUS, SV_ARITH_MINUS, SV_ARITH_MULTIPLY, SV_ARITH_DIVIDE };

enum SetKnobType { EnableNestLoop, EnableSortMerge };

enum class SvAggregateType { NONE, COUNT, SUM, AVG, MAX, MIN };

inline std::string SvAggregateType2Str(SvAggregateType agg_type) {
    static const std::string agg_type_str[] = {"none", "count", "sum", "avg", "max", "min"};
    return agg_type_str[static_cast<int>(agg_type)];
}

inline std::string generate_alias(std::string tab_name, std::string col_name, SvAggregateType agg_type) {
    if (agg_type == SvAggregateType::NONE) {
        if (!tab_name.empty()) {
            tab_name += ".";
            tab_name += col_name;
            return tab_name;
        } else {
            return col_name;
        }
    }
    if (!tab_name.empty()) {
        tab_name += ".";
        tab_name += col_name;
        // 防止列名与聚合类型冲突
        return util::format("{}_{}", SvAggregateType2Str(agg_type), tab_name);
    } else {
        return util::format("{}_{}", SvAggregateType2Str(agg_type), col_name);
    }
}

// Base class for tree nodes
struct TreeNode {
    virtual ~TreeNode() = default;  // enable polymorphism
};

struct Help : public TreeNode {};

struct ShowTables : public TreeNode {};

struct ShowIndex : public TreeNode {
    std::string tab_name;

    ShowIndex(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct TxnBegin : public TreeNode {};

struct TxnCommit : public TreeNode {};

struct TxnAbort : public TreeNode {};

struct TxnRollback : public TreeNode {};

struct CreateStaticCheckpoint : public TreeNode {};

// Load command AST node
struct LoadStmt : public TreeNode {
    std::string file_name;
    std::string table_name;

    LoadStmt(std::string file_name_, std::string table_name_)
        : file_name(std::move(file_name_)), table_name(std::move(table_name_)) {}
};

// Set output_file command AST node
struct SetOutputStmt : public TreeNode {
    bool enable; // true for on, false for off

    SetOutputStmt(bool enable_) : enable(enable_) {}
};

struct TypeLen : public TreeNode {
    SvType type;
    int len;

    TypeLen(SvType type_, int len_) : type(type_), len(len_) {}
};

struct Field : public TreeNode {};

struct ColDef : public Field {
    std::string col_name;
    std::shared_ptr<TypeLen> type_len;

    ColDef(std::string col_name_, std::shared_ptr<TypeLen> type_len_)
        : col_name(std::move(col_name_)), type_len(std::move(type_len_)) {}
};

struct CreateTable : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<Field>> fields;

    CreateTable(std::string tab_name_, std::vector<std::shared_ptr<Field>> fields_)
        : tab_name(std::move(tab_name_)), fields(std::move(fields_)) {}
};

struct DropTable : public TreeNode {
    std::string tab_name;

    DropTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct DescTable : public TreeNode {
    std::string tab_name;

    DescTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct CreateIndex : public TreeNode {
    std::string tab_name;
    std::vector<std::string> col_names;

    CreateIndex(std::string tab_name_, std::vector<std::string> col_names_)
        : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

struct DropIndex : public TreeNode {
    std::string tab_name;
    std::vector<std::string> col_names;

    DropIndex(std::string tab_name_, std::vector<std::string> col_names_)
        : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

struct Expr : public TreeNode {};

struct Value : public Expr {};

struct IntLit : public Value {
    int val;

    IntLit(int val_) : val(val_) {}
};

struct FloatLit : public Value {
    float val;

    FloatLit(float val_) : val(val_) {}
};

struct StringLit : public Value {
    std::string val;

    StringLit(std::string val_) : val(std::move(val_)) {}
};

struct BoolLit : public Value {
    bool val;

    BoolLit(bool val_) : val(val_) {}
};
// 表别名结构
struct TableRef : public TreeNode {
    std::string tab_name;
    std::string alias;  // 表别名

    TableRef(std::string tab_name_) : tab_name(std::move(tab_name_)), alias("") {}
    TableRef(std::string tab_name_, std::string alias_) : tab_name(std::move(tab_name_)), alias(std::move(alias_)) {}
};

struct Col : public Expr {
    std::string tab_name;
    std::string col_name;
    std::string alias;  // 列别名

    SvAggregateType aggregate_type{SvAggregateType::NONE};  // 聚合类型

    Col(std::string tab_name_, std::string col_name_)
        : tab_name(std::move(tab_name_)),
          col_name(std::move(col_name_)),
          alias(generate_alias(tab_name, col_name, SvAggregateType::NONE)) {}

    Col(std::string tab_name_, std::string col_name_, std::string alias_)
        : tab_name(std::move(tab_name_)), col_name(std::move(col_name_)) {
        if (alias_.empty()) {
            alias = generate_alias(tab_name, col_name, SvAggregateType::NONE);
        } else {
            alias = std::move(alias_);
        }
    }

    Col(std::string tab_name_, std::string col_name_, std::string alias_, SvAggregateType aggregate_type_)
        : tab_name(std::move(tab_name_)), col_name(std::move(col_name_)), aggregate_type(aggregate_type_) {
        if (alias_.empty()) {
            alias = generate_alias(tab_name, col_name, aggregate_type);
        } else {
            alias = std::move(alias_);
        }
    }
};

// ArithExpr class to represent arithmetic expressions
struct ArithExpr : public Expr {
    std::shared_ptr<Expr> lhs;
    SvArithOp op;
    std::shared_ptr<Expr> rhs;

    ArithExpr(std::shared_ptr<Expr> lhs_, SvArithOp op_, std::shared_ptr<Expr> rhs_)
        : lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
};

struct SetClause : public TreeNode {
    std::string col_name;
    std::shared_ptr<Expr> val;  // Changed from Value to Expr

    SetClause(std::string col_name_, std::shared_ptr<Expr> val_)  // Changed from Value to Expr
        : col_name(std::move(col_name_)), val(std::move(val_)) {}
};

struct BinaryExpr : public TreeNode {
    std::shared_ptr<Col> lhs;
    SvCompOp op;
    std::shared_ptr<Expr> rhs;

    BinaryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_, std::shared_ptr<Expr> rhs_)
        : lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
};

struct OrderBy : public TreeNode {
    std::shared_ptr<Col> cols;
    OrderByDir orderby_dir;
    OrderBy(std::shared_ptr<Col> cols_, OrderByDir orderby_dir_)
        : cols(std::move(cols_)), orderby_dir(std::move(orderby_dir_)) {}
};

struct GroupBy : public TreeNode {
    std::shared_ptr<Col> cols;
    GroupBy(std::shared_ptr<Col> cols_) : cols(std::move(cols_)) {}
};

struct Limit : public TreeNode {
    std::shared_ptr<Value> offset;
    std::shared_ptr<Value> count;

    Limit(std::shared_ptr<Value> offset_, std::shared_ptr<Value> count_)
        : offset(std::move(offset_)), count(std::move(count_)) {}
};

struct InsertStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<Value>> vals;

    InsertStmt(std::string tab_name_, std::vector<std::shared_ptr<Value>> vals_)
        : tab_name(std::move(tab_name_)), vals(std::move(vals_)) {}
};

struct DeleteStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<BinaryExpr>> conds;

    DeleteStmt(std::string tab_name_, std::vector<std::shared_ptr<BinaryExpr>> conds_)
        : tab_name(std::move(tab_name_)), conds(std::move(conds_)) {}
};

struct UpdateStmt : public TreeNode {
    std::shared_ptr<TableRef> tab_name;  // 使用TableRef以支持别名
    std::vector<std::shared_ptr<SetClause>> set_clauses;
    std::vector<std::shared_ptr<BinaryExpr>> conds;

    UpdateStmt(std::shared_ptr<TableRef> tab_name_, std::vector<std::shared_ptr<SetClause>> set_clauses_,
               std::vector<std::shared_ptr<BinaryExpr>> conds_)
        : tab_name(std::move(tab_name_)), set_clauses(std::move(set_clauses_)), conds(std::move(conds_)) {}
};

struct JoinExpr : public TreeNode {
    std::shared_ptr<TableRef> right;
    std::vector<std::shared_ptr<BinaryExpr>> conds;
    JoinType type;

    JoinExpr(std::shared_ptr<TableRef> right_, std::vector<std::shared_ptr<BinaryExpr>> conds_, JoinType type_)
        : right(std::move(right_)), conds(std::move(conds_)), type(type_) {}
};

struct SelectStmt : public TreeNode {
    std::vector<std::shared_ptr<Col>> cols;
    std::vector<std::shared_ptr<TableRef>> tabs;  // 改为TableRef以支持别名
    std::vector<std::shared_ptr<BinaryExpr>> conds;
    std::vector<std::shared_ptr<JoinExpr>> jointree;

    bool has_sort;
    std::vector<std::shared_ptr<GroupBy>> group;            // 添加group by支持
    std::vector<std::shared_ptr<BinaryExpr>> having_conds;  // 添加having支持
    std::vector<std::shared_ptr<OrderBy>> orders;
    std::shared_ptr<Limit> limit;

    SelectStmt(std::vector<std::shared_ptr<Col>> cols_, std::vector<std::shared_ptr<TableRef>> tabs_,
               std::vector<std::shared_ptr<JoinExpr>> jointree_, std::vector<std::shared_ptr<BinaryExpr>> conds_,
               std::vector<std::shared_ptr<GroupBy>> group_, std::vector<std::shared_ptr<BinaryExpr>> having_conds_,
               std::vector<std::shared_ptr<OrderBy>> orders_, std::shared_ptr<Limit> limit_ = nullptr)
        : cols(std::move(cols_)),
          tabs(std::move(tabs_)),
          conds(std::move(conds_)),
          jointree(std::move(jointree_)),
          group(std::move(group_)),
          having_conds(std::move(having_conds_)),
          orders(std::move(orders_)),
          limit(std::move(limit_)) {
        has_sort = !orders.empty();
    }
};

struct ExplainStmt : public SelectStmt {
    ExplainStmt(std::vector<std::shared_ptr<Col>> cols_, std::vector<std::shared_ptr<TableRef>> tabs_,
                std::vector<std::shared_ptr<JoinExpr>> jointree_, std::vector<std::shared_ptr<BinaryExpr>> conds_,
                std::vector<std::shared_ptr<GroupBy>> group_, std::vector<std::shared_ptr<BinaryExpr>> having_conds_,
                std::vector<std::shared_ptr<OrderBy>> orders_, std::shared_ptr<Limit> limit_ = nullptr)
        : SelectStmt(std::move(cols_), std::move(tabs_), std::move(jointree_), std::move(conds_), std::move(group_),
                     std::move(having_conds_), std::move(orders_), std::move(limit_)) {}
};

// set enable_nestloop
struct SetStmt : public TreeNode {
    SetKnobType set_knob_type_;
    bool bool_val_;

    SetStmt(SetKnobType &type, bool bool_value) : set_knob_type_(type), bool_val_(bool_value) {}
};

// Semantic value
struct SemValue {
    int sv_int;
    float sv_float;
    std::string sv_str;
    bool sv_bool;
    OrderByDir sv_orderby_dir;
    std::vector<std::string> sv_strs;

    std::shared_ptr<TreeNode> sv_node;

    SvCompOp sv_comp_op;

    std::shared_ptr<TypeLen> sv_type_len;

    std::shared_ptr<Field> sv_field;
    std::vector<std::shared_ptr<Field>> sv_fields;

    std::shared_ptr<Expr> sv_expr;

    std::shared_ptr<Value> sv_val;
    std::vector<std::shared_ptr<Value>> sv_vals;

    std::shared_ptr<Col> sv_col;
    std::vector<std::shared_ptr<Col>> sv_cols;

    std::shared_ptr<TableRef> sv_table_ref;                // 添加表引用支持
    std::vector<std::shared_ptr<TableRef>> sv_table_refs;  // 添加表引用列表支持

    std::shared_ptr<SetClause> sv_set_clause;
    std::vector<std::shared_ptr<SetClause>> sv_set_clauses;

    std::shared_ptr<BinaryExpr> sv_cond;
    std::vector<std::shared_ptr<BinaryExpr>> sv_conds;

    std::shared_ptr<OrderBy> sv_orderby;
    std::vector<std::shared_ptr<OrderBy>> sv_orderbys;

    std::shared_ptr<GroupBy> sv_groupby;
    std::vector<std::shared_ptr<GroupBy>> sv_groupbys;

    std::vector<std::shared_ptr<BinaryExpr>> sv_having_conds;

    std::shared_ptr<JoinExpr> sv_join_expr;
    std::vector<std::shared_ptr<JoinExpr>> sv_join_exprs;
    JoinType sv_join_type;

    SetKnobType sv_setKnobType;

    std::shared_ptr<Limit> sv_limit;
};

extern thread_local std::shared_ptr<ast::TreeNode> parse_tree;

}  // namespace ast

#define YYSTYPE ast::SemValue
