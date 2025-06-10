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

/**
 * @brief 表扫描操作执行计划的解释器
 */
class ExplainScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;  // 被扫描的表名
    int offset_;            // 输出的缩进偏移量

   public:
    /**
     * @brief 构造函数
     * @param tab_name 要扫描的表名
     * @param offset 输出的缩进偏移量
     */
    ExplainScanExecutor(std::string tab_name, int offset) {
        tab_name_ = std::move(tab_name);
        offset_ = offset;
    }

    /**
     * @brief 生成当前节点的执行计划说明
     * @return nullptr,因为解释器不实际生成记录
     */
    std::unique_ptr<RmRecord> Next() override {
        // 按指定缩进生成输出
        std::string res = std::string(offset_, '\t');
        res += "Scan(table=";
        res += tab_name_;
        res += ")\n";

        // 将解释写入输出文件
        std::fstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << res;
        outfile.close();
        return nullptr;
    }

    /**
     * @brief 获取当前记录的RID
     * @return 抽象执行器的RID引用
     */
    Rid &rid() override { return _abstract_rid; }

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    std::string getType() override { return "ExplainScanExecutor"; }

    /**
     * @brief 获取被扫描的表名
     * @return 表名字符串
     *
     * @details 用于调试和错误报告
     */
    std::string get_tab_name() const { return tab_name_; }
};