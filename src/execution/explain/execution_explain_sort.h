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

#include "execution/execution_defs.h"
#include "execution/execution_manager.h"
#include "execution/executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include "common/BatchArray.hpp"

/**
 * @brief 排序执行器，负责实现ORDER BY的排序功能
 */

class ExplainSortExecutor : public AbstractExecutor {
   private:
    struct SortColumn {
        TabCol col;    // 列元数据
        bool is_desc;  // 是否降序

        SortColumn(const TabCol& c, bool desc) : col(c), is_desc(desc) {}
    };

    std::unique_ptr<AbstractExecutor> prev_;  // 前序执行器
    std::vector<SortColumn> sort_cols_;       // 排序列信息
    int offset_;                              // 输出的缩进偏移量
   public:
    /**
     * @brief 构造函数
     * @param prev 前序执行器
     * @param sel_col 排序列的表列引用
     * @param is_desc 是否降序排序
     */
    ExplainSortExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol>& sel_cols,
                        const std::vector<bool>& is_desc, int offset) {
        prev_ = std::move(prev);
        // 获取所有排序列的元数据
        sort_cols_.reserve(sel_cols.size());
        for (size_t i = 0; i < sel_cols.size(); i++) {
            sort_cols_.emplace_back(sel_cols[i], is_desc[i]);
        }
        offset_ = offset;
    }

    /**
     * @brief 返回当前排序位置的元组
     * @return 当前元组的智能指针
     */
    std::unique_ptr<BatchRecord> Next() override {
        // 按指定缩进生成输出
        std::string res = std::string(offset_, '\t');
        res += "Sort(columns=[";
        for (size_t i = 0; i < sort_cols_.size(); i++) {
            if (i > 0) res += ", ";
            res += sort_cols_[i].col.to_string();
            if (sort_cols_[i].is_desc) {
                res += " DESC";
            } else {
                res += " ASC";
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
    Rid& rid() override { return _abstract_rid; }

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    std::string getType() override { return "ExplainSortExecutor"; }
};