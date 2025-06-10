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

/**
 * @brief 连接执行计划的解释器
 */
class ExplainJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;   // 左子树执行器
    std::unique_ptr<AbstractExecutor> right_;  // 右子树执行器
    std::vector<std::string> tables_;          // 参与连接的表名列表
    std::vector<Condition> conds_;             // 连接条件列表
    int offset_;                               // 输出时的缩进偏移量

   public:
    /**
     * @brief 构造函数
     * @param left 左子树执行器
     * @param right 右子树执行器
     * @param tables 参与连接的表名列表
     * @param conds 连接条件列表
     * @param offset 输出的缩进偏移量
     *
     * @details 初始化连接执行计划的解释器:
     * 1. 保存左右子树执行器
     * 2. 记录连接涉及的表和条件
     * 3. 对表名和条件按字典序排序,保证输出的一致性
     */
    ExplainJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                        std::vector<std::string> tables, std::vector<Condition> conds, int offset) {
        left_ = std::move(left);
        right_ = std::move(right);
        tables_ = std::move(tables);
        conds_ = std::move(conds);
        offset_ = offset;
        // 对表名和条件排序,保证解释计划输出的一致性
        std::sort(tables_.begin(), tables_.end());
        std::sort(conds_.begin(), conds_.end(),
                  [](const Condition &a, const Condition &b) { return a.to_string() < b.to_string(); });
    }

    /**
     * @brief 生成当前节点的执行计划说明
     * @return nullptr,因为解释器不实际生成记录
     */
    std::unique_ptr<RmRecord> Next() override {
        // 按指定缩进生成输出
        std::string res = std::string(offset_, '\t');
        res += "Join(tables=[";

        // 输出参与连接的表名
        for (size_t i = 0; i < tables_.size(); ++i) {
            if (i != 0) {
                res += ",";
            }
            res += tables_[i];
        }
        res += "],condition=[";

        // 输出连接条件
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

        // 递归解释左右子树
        left_->Next();
        right_->Next();
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
    std::string getType() override { return "ExplainJoinExecutor"; }

    /**
     * @brief 获取所有参与连接的表名
     * @return 以逗号分隔的表名列表
     *
     * @details 将表名列表转换为字符串形式,用于调试和错误报告
     */
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
    std::vector<std::string> get_tables_vec() const { return tables_; }
};