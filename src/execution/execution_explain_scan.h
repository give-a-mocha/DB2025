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
#include "execution_common.h"
#include "index/ix.h"
#include "system/sm.h"
#include "common/config.h"
#include "common/StackString.hpp"

class ExplainScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;
    Context *context_;
    int offset_;

   public:
    ExplainScanExecutor(std::string tab_name, int offset, Context *context) {
        tab_name_ = std::move(tab_name);
        context_ = context;
        offset_ = offset;
    }

    std::unique_ptr<RmRecord> Next() override {
        StackString<2048, true> output(context_->data_send_ + *context_->offset_);
        output.append(offset_, '\t');
        output += "Scan(table=";
        output += tab_name_;
        output += ")\n";

        *context_->offset_ += output.size();
        return nullptr;
    }
    
    Rid &rid() override { return _abstract_rid; }

    std::string getType() override { return "ExplainScanExecutor"; }
};