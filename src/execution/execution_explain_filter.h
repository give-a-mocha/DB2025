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

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include "common/config.h"
#include "common/StackString.hpp"

class ExplainFilterExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<Condition> conds_;
    Context *context_;
    int offset_;

   public:
    ExplainFilterExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<Condition> conds, int offset, Context *context) {
        prev_ = std::move(prev);
        conds_ = std::move(conds);
        offset_ = offset;
        context_ = context;
    }
    
    std::unique_ptr<RmRecord> Next() override {
        // ! warning 未带越界检查
        StackString<2048, true> output(context_->data_send_ + *context_->offset_);
        output.append(offset_, '\t');
        // 多个条件按字典序排序
        std::sort(conds_.begin(), conds_.end(), [](const Condition &a, const Condition &b) {
            return a.to_string() < b.to_string();
        });
        output += "Filter(condition=[";
        for (size_t i = 0; i < conds_.size(); ++i) {
            if(i != 0) {
                output += ",";
            }
            const auto &cond = conds_[i];
            output += cond.to_string();
        }
        output += "])\n";
        *context_->offset_ += output.size();
        prev_->Next();
        return nullptr;
    }

    
    Rid &rid() override { return _abstract_rid; }

    std::string getType() override { return "ExplainFilterExecutor"; }
};