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

class ExplainScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;
    int offset_;

   public:
    ExplainScanExecutor(std::string tab_name, int offset) {
        tab_name_ = std::move(tab_name);
        offset_ = offset;
    }

    std::unique_ptr<RmRecord> Next() override {
        std::string res = std::string(offset_, '\t');
        res += "Scan(table=";
        res += tab_name_;
        res += ")\n";

        std::fstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << res;
        outfile.close();
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }

    std::string getType() override { return "ExplainScanExecutor"; }

    std::string get_tab_name() const { return tab_name_; }
};