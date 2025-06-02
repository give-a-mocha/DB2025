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

#include "common/StackString.hpp"
#include "common/config.h"
#include "execution_common.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class ExplainProjectExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<TabCol> cols_;
    int offset_;
    bool isStar_;

   public:
    ExplainProjectExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<TabCol> sel_cols, int offset,
                           bool isStar = false) {
        prev_ = std::move(prev);
        cols_ = std::move(sel_cols);
        offset_ = offset;
        isStar_ = isStar;
        std::sort(cols_.begin(), cols_.end(), [&](const TabCol &a, const TabCol &b) {
            return std::make_pair(a.tab_name.empty() ? a.tab_alias : a.tab_name, a.col_name) <
                   std::make_pair(b.tab_name.empty() ? b.tab_alias : b.tab_name, b.col_name);
        });
    }

    std::unique_ptr<RmRecord> Next() override {
        std::string res = std::string(offset_, '\t');
        res += "Project(columns=[";
        if (isStar_) {
            res += "*";
        } else {
            for (size_t i = 0; i < cols_.size(); ++i) {
                if (i != 0) {
                    res += ",";
                }
                res += cols_[i].to_string();
            }
        }
        res += "])\n";
        std::fstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << res;
        outfile.close();
        prev_->Next();
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }

    std::string getType() override { return "ExplainProjectExecutor"; }

    std::string get_cols() const {
        std::string res;
        for (const auto &col : cols_) {
            if (!res.empty()) {
                res += ", ";
            }
            res += col.to_string();
        }
        return res;
    }
};