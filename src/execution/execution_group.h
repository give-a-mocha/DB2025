#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/Hash.h"
#include "executor_abstract.h"

class GroupExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;  ///< 指向上一个执行器的智能指针，用于获取输入元组
    std::vector<ColMeta> cols_;               ///< SELECT 子句中选择的列的元数据
    std::vector<ColMeta> group_cols_;         ///< GROUP BY 子句中用于分组的列的元数据
    std::vector<Condition> having_conds_;     ///< HAVING 子句中的过滤条件

    /**
     * @brief 用于存储分组后的记录。
     * 键是根据 group_cols_ 生成的字符串，值是属于该组的 RmRecord 列表。
     */
    std::unordered_map<std::string, std::vector<std::unique_ptr<RmRecord>>> grouped_records;

    /**
     * @brief 当前正在处理的组的迭代器。
     * 指向 group_iterators 中的一个元素。
     */
    std::vector<std::unordered_map<std::string, std::vector<std::unique_ptr<RmRecord>>>::iterator>::iterator
        current_group;

    std::unique_ptr<RmRecord> current_tuple;  ///< 当前 Next() 方法将返回的元组（通常是每个组的代表元组）

    std::vector<std::unordered_map<std::string, std::vector<std::unique_ptr<RmRecord>>>::iterator> group_iterators;

    GroupExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol>& sel_cols,
                  const std::vector<TabCol>& group_cols, std::vector<Condition> conds) {
        prev_ = std::move(prev);  // 移动上一个执行器的所有权
        // 处理 SELECT 列元数据
        for (const auto& sel_col : sel_cols) {
            // 特殊处理 COUNT(*)
            if (sel_col.col_name == "*" && sel_col.agg_type == AggregateType::COUNT) {
                cols_.push_back(
                    ColMeta{.tab_name = "", .name = "*", .type = ColType::TYPE_INT, .len = sizeof(int), .offset = 0});
            } else {
                // 从上一个执行器的列元数据中查找并添加选择的列
                cols_.push_back(*get_col(prev_->cols(), sel_col));
            }
        }
        // 处理 GROUP BY 列元数据
        for (const auto& group_col : group_cols) {
            // 从上一个执行器的列元数据中查找并添加分组列
            group_cols_.push_back(*get_col(prev_->cols(), group_col));
        }
        having_conds_ = std::move(conds);  // 移动 HAVING 条件的所有权
        current_tuple = nullptr;           // 初始化当前元组为空
    }
    // TODO
};