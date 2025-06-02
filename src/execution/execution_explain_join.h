/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class ExplainJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;   // 左儿子节点
    std::unique_ptr<AbstractExecutor> right_;  // 右儿子节点
    std::vector<std::string> tables_;
    std::vector<Condition> conds_;
    int offset_;

   public:
    ExplainJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                        std::vector<std::string> tables, std::vector<Condition> conds, int offset) {
        left_ = std::move(left);
        right_ = std::move(right);
        tables_ = std::move(tables);
        conds_ = std::move(conds);
        offset_ = offset;
        std::sort(tables_.begin(), tables_.end());
        std::sort(conds_.begin(), conds_.end(),
                  [](const Condition &a, const Condition &b) { return a.to_string() < b.to_string(); });
    }

    std::unique_ptr<RmRecord> Next() override {
        std::string res = std::string(offset_, '\t');
        res += "Join(tables=[";
        for (size_t i = 0; i < tables_.size(); ++i) {
            if (i != 0) {
                res += ",";
            }
            res += tables_[i];
        }
        res += "],condition=[";
        for (size_t i = 0; i < conds_.size(); ++i) {
            if (i != 0) {
                res += ",";
            }
            const auto &cond = conds_[i];
            res += cond.to_string();
        }
        res += "])\n";

        std::fstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << res;
        outfile.close();
        left_->Next();
        right_->Next();
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }

    std::string getType() override { return "ExplainJoinExecutor"; }

    std::string get_tables() const {
        std::string res;
        for (const auto &table : tables_) {
            if (!res.empty()) {
                res += ", ";
            }
            res += table;
        }
        return res;
    }
};