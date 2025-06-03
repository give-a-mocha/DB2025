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
 * @brief 投影操作执行计划的解释器
 *
 * @details 负责解释和展示SELECT语句的投影操作,包括:
 * 1. 投影列的列表
 * 2. 是否为SELECT *
 * 3. 计划的缩进层次
 */
class ExplainProjectExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;  // 前序执行器
    std::vector<TabCol> cols_;                // 投影的列列表
    int offset_;                              // 输出的缩进偏移量
    bool isStar_;                             // 是否为SELECT *查询

   public:
    /**
     * @brief 构造函数
     * @param prev 前序执行器
     * @param sel_cols 需要投影的列
     * @param offset 输出的缩进偏移量
     * @param isStar 是否为SELECT *查询
     *
     * @details 初始化投影操作的解释器:
     * 1. 保存前序执行器
     * 2. 记录投影列
     * 3. 对列按表名和列名排序,确保输出的一致性
     */
    ExplainProjectExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<TabCol> sel_cols, int offset,
                           bool isStar = false) {
        prev_ = std::move(prev);
        cols_ = std::move(sel_cols);
        offset_ = offset;
        isStar_ = isStar;
        // 按表名(如果为空则用别名)和列名排序,保证解释计划输出的一致性
        std::sort(cols_.begin(), cols_.end(), [&](const TabCol &a, const TabCol &b) {
            return std::make_pair(a.tab_name.empty() ? a.tab_alias : a.tab_name, a.col_name) <
                   std::make_pair(b.tab_name.empty() ? b.tab_alias : b.tab_name, b.col_name);
        });
    }

    /**
     * @brief 生成当前节点的执行计划说明
     * @return nullptr,因为解释器不实际生成记录
     *
     * @details 输出格式:
     * Project(columns=[col1,col2,...]) 或 Project(columns=[*])
     * 其中:
     * 1. 输出开始按offset_进行缩进
     * 2. 如果是SELECT *则输出*
     * 3. 否则列出所有投影列
     * 4. 递归解释前序节点的执行计划
     */
    std::unique_ptr<RmRecord> Next() override {
        // 按指定缩进生成输出
        std::string res = std::string(offset_, '\t');
        res += "Project(columns=[";

        // 根据查询类型输出投影列
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
    std::string getType() override { return "ExplainProjectExecutor"; }

    /**
     * @brief 获取所有投影列的字符串表示
     * @return 以逗号分隔的列名列表
     *
     * @details 将投影列列表转换为字符串形式,用于调试和错误报告
     */
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