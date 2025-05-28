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

#include "parser/parser.h"
#include "system/sm.h"
#include "common/common.h"

class ColCheck {
public:
    std::map<std::string, std::vector<std::string>> mp;
    std::map<std::pair<std::string, std::string>, int> cols;
    ColCheck(const std::vector<ColMeta> &all_cols) {
        for (const auto &col : all_cols) {
            cols[{col.tab_name, col.name}]++;
            auto it = mp.find(col.name);
            if (it == mp.end()) {
                mp[col.name] = std::vector<std::string>({col.tab_name});
            }else{
                if(it->second.size() < 2){
                    it->second.push_back(col.tab_name);
                }
            }
        }
    }
    TabCol check(TabCol target_col) {
        if (target_col.tab_name.empty()) {
            auto it = mp.find(target_col.col_name);
            if (it == mp.end()) {
                throw ColumnNotFoundError(target_col.col_name);
            }
            if (it->second.size() > 1) {
                throw AmbiguousColumnError(target_col.col_name);
            }
            target_col.tab_name = it->second[0];
        } else {
            auto it = cols.find({target_col.tab_name, target_col.col_name});
            if (it == cols.end()) {
                throw ColumnNotFoundError(target_col.col_name);
            }
            if (it->second > 1) {
                throw AmbiguousColumnError(target_col.col_name);
            }
        }
        return target_col;
    }
};

class Query{
    public:
    std::shared_ptr<ast::TreeNode> parse;
    // TODO jointree
    // where条件
    std::vector<Condition> conds;
    // 投影列
    std::vector<TabCol> cols;
    // 表名
    std::vector<std::string> tables;
    // update 的set 值
    std::vector<SetClause> set_clauses;
    //insert 的values值
    std::vector<Value> values;

    Query(){}

};

class Analyze
{
private:
    SmManager *sm_manager_;
public:
    Analyze(SmManager *sm_manager) : sm_manager_(sm_manager){}
    ~Analyze(){}

    std::shared_ptr<Query> do_analyze(std::shared_ptr<ast::TreeNode> root);

private:
    TabCol check_column(const std::vector<ColMeta> &all_cols, TabCol target);
    void get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols);
    void get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds);
    void check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds);
    void check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds, ColCheck &col_check);
    void check_where_aggregates(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds);
    void check_group_by_semantics(const std::vector<TabCol> &select_cols,
                                 const std::vector<std::shared_ptr<ast::GroupBy>> &group_cols,
                                 ColCheck &col_check);
    Value convert_sv_value(const std::shared_ptr<ast::Value> &sv_val);
    CompOp convert_sv_comp_op(ast::SvCompOp op);
};

