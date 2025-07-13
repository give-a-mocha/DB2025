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
#include "common/BatchArray.hpp"
#include "execution/execution_defs.h"
#include "execution/execution_manager.h"
#include "execution/executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 过滤条件执行计划的解释器
 *
 * @details 负责解释和展示过滤操作的执行计划,包括:
 * 1. WHERE子句中的过滤条件
 * 2. 条件的逻辑排列
 * 3. 计划的缩进层次
 */
class ExplainFilterExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;  // 前序执行器
    std::vector<Condition> conds_;            // 过滤条件列表
    int offset_;                              // 输出的缩进偏移量

   public:
    /**
     * @brief 构造函数
     * @param prev 前序执行器
     * @param conds 过滤条件列表
     * @param offset 输出的缩进偏移量
     */
    ExplainFilterExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<Condition> conds, int offset) {
        prev_ = std::move(prev);
        conds_ = std::move(conds);
        // 按字典序排序所有条件,保证解释计划输出的一致性
        std::sort(conds_.begin(), conds_.end(),
                  [](const Condition &a, const Condition &b) { return a.to_string() < b.to_string(); });
        offset_ = offset;
    }

    /**
     * @brief 生成当前节点的执行计划说明
     * @return nullptr,因为解释器不实际生成记录
     */
    std::unique_ptr<BatchRecord> Next() override {
        // 按指定缩进生成输出
        std::string res = std::string(offset_, '\t');
        res += "Filter(condition=[";

        // 输出所有过滤条件
        for (size_t i = 0; i < conds_.size(); ++i) {
            if (i != 0) {
                res += ",";
            }
            const auto &cond = conds_[i];
            res += cond.to_string();
        }
        res += "])\n";

        // 将解释写入输出文件
        std::fstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << res;
        outfile.close();

        // 递归解释前序节点
        prev_->Next();
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
    std::string getType() override { return "ExplainFilterExecutor"; }

    /**
     * @brief 获取所有过滤条件的字符串表示
     * @return 以逗号分隔的条件列表
     *
     * @details 将条件列表转换为字符串形式,用于调试和错误报告
     */
    std::string get_conds() const {
        std::string res;
        for (const auto &cond : conds_) {
            if (!res.empty()) {
                res += ", ";
            }
            res += cond.to_string();
        }
        return res;
    }
};