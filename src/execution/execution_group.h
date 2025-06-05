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
    //     void beginTuple() override {
    //         prev_->beginTuple();      // 初始化上一个执行器
    //         grouped_records.clear();  // 清空上一次执行的分组记录
    //         group_iterators.clear();  // 清空上一次执行的组迭代器

    //         // 从上一个执行器获取所有元组并进行分组
    //         while (!prev_->is_end()) {
    //             auto tuple = prev_->Next();                       // 获取下一个元组
    //             std::string group_key = generateGroupKey(tuple);  // 生成分组键
    //             // 将元组添加到对应的分组中
    //             grouped_records[group_key].emplace_back(std::move(tuple));
    //             prev_->nextTuple();  // 移动到上一个执行器的下一个元组
    //         }

    //         // 根据 HAVING 条件过滤分组
    //         // TODO: 优化过滤过程，避免创建临时 vector
    //         std::vector<std::string> group_keys_to_remove;  // 存储需要移除的分组键
    //         for (auto& [key, records] : grouped_records) {
    //             if (having_conds_.empty()) {  // 如果没有 HAVING 条件，则无需过滤
    //                 break;
    //             }
    //             // 评估当前分组是否满足 HAVING 条件
    //             bool should_remove = !eval_aggr_conds(cols_, having_conds_, records);
    //             if (should_remove) {
    //                 group_keys_to_remove.push_back(key);  // 记录不满足条件的分组键
    //             }
    //         }

    //         // 删除不满足 HAVING 条件的分组
    //         for (const auto& key : group_keys_to_remove) {
    //             grouped_records.erase(key);
    //         }

    //         // 将剩余分组的迭代器添加到 group_iterators 中
    //         for (auto it = grouped_records.begin(); it != grouped_records.end(); ++it) {
    //             group_iterators.push_back(it);
    //         }
    //         // 反转迭代器顺序，以便 Next() 可以按预期顺序弹出
    //         // TODO: 考虑是否需要反转，或者调整 Next() 的逻辑
    //         std::reverse(group_iterators.begin(), group_iterators.end());

    //         // 初始化当前处理的组
    //         current_group = group_iterators.begin();

    //         // 初始化 current_tuple 为第一个有效组的代表元组
    //         if (current_group != group_iterators.end()) {
    //             // 获取当前组的第一条记录作为代表元组（用于后续投影等操作）
    //             // 注意：这里只是复制了第一条记录，聚合结果需要在 ProjectionExecutor 中计算
    //             auto& front = current_group->operator->()->second.front();
    //             // 创建一个新的 RmRecord 来存储代表元组的数据
    //             auto temp_tuple = std::make_unique<RmRecord>(front->size, front->data);
    //             current_tuple = std::move(temp_tuple);  // 移动所有权
    //         } else {
    //             current_tuple = nullptr;  // 如果没有有效分组，则当前元组为空
    //         }
    //     }

    //     /**
    //      * @brief 移动到下一个分组。
    //      * 更新 current_group 和 current_tuple。
    //      */
    //     void nextTuple() override {
    //         if (current_group != group_iterators.end()) {
    //             current_group++;  // 移动到下一个分组迭代器
    //             if (current_group != group_iterators.end()) {
    //                 // 如果存在下一个分组，更新 current_tuple 为该组的代表元组
    //                 auto& front = current_group->operator->()->second.front();
    //                 auto temp_tuple = std::make_unique<RmRecord>(front->size, front->data);
    //                 current_tuple = std::move(temp_tuple);
    //             } else {
    //                 // 如果没有更多分组，将 current_tuple 置为空
    //                 current_tuple = nullptr;
    //             }
    //         }
    //     }

    //     /**
    //      * @brief 获取当前分组的代表元组。
    //      * @return 指向当前分组代表元组的智能指针。
    //      * 注意：返回的元组所有权被转移。
    //      */
    //     std::unique_ptr<RmRecord> Next() override {
    //         // 返回当前元组，并将 current_tuple 置空，表示所有权转移
    //         return std::move(current_tuple);
    //     }

    //     /**
    //      * @brief 获取输出列的元数据。
    //      * @return 包含输出列元数据的常量引用。
    //      * 注意：这里返回的是上一个执行器的列元数据，
    //      * 实际输出的列（包括聚合结果）由 ProjectionExecutor 处理。
    //      */
    //     const std::vector<ColMeta>& cols() const override { return prev_->cols(); }

    //     /**
    //      * @brief 获取记录标识符（RID）。
    //      * 对于 GroupExecutor，RID 通常没有明确意义，返回一个默认值。
    //      * @return 记录标识符的引用。
    //      */
    //     Rid& rid() override { return _abstract_rid; }  // 返回基类中的默认 RID

    //     /**
    //      * @brief 检查是否已处理完所有分组。
    //      * @return 如果已到达最后一个分组之后，则返回 true；否则返回 false。
    //      */
    //     bool is_end() const override {
    //         // 当 current_group 指向 group_iterators 的末尾时，表示处理完毕
    //         return current_group == group_iterators.end();
    //     }

    //     /**
    //      * @brief 获取执行器的类型。
    //      * @return 执行器类型枚举值 ExecutorType::GROUP。
    //      */
    //     virtual std::string getType() { return "GroupExecutor"; }

    //    private:
    //     /**
    //      * @brief 根据分组列为给定的记录生成分组键。
    //      * @param record 指向要生成键的记录的智能指针
    //      * @return 生成的分组键字符串。
    //      */
    //     std::string generateGroupKey(std::unique_ptr<RmRecord>& record) {
    //         std::string group_key = "";
    //         // 遍历所有分组列
    //         for (const auto& group_col : group_cols_) {
    //             // 获取列数据在记录中的起始地址
    //             const char* col_data = record->data + group_col.offset;
    //             // 将列数据追加到分组键字符串中
    //             // TODO: 对于不同数据类型，可能需要更健壮的序列化方式
    //             group_key += std::string(col_data, group_col.len);
    //         }
    //         // std::cerr << "Group key: " << group_key << std::endl; // 调试输出
    //         return group_key;
    //     }

    //     /**
    //      * @brief 评估单个聚合条件。
    //      * @param rec_cols SELECT 子句中的列元数据（用于查找聚合函数的目标列）
    //      * @param cond 要评估的条件
    //      * @param rec 当前分组的所有记录
    //      * @return 如果条件满足，则返回 true；否则返回 false。
    //      */
    //     bool eval_aggr_cond(const std::vector<ColMeta>& rec_cols, const Condition& cond,
    //                         std::vector<std::unique_ptr<RmRecord>>& rec) {
    //         auto copy_cond = cond;  // 创建条件的副本以防修改原始条件
    //         // 计算条件的左侧聚合值
    //         Value lhs_val = get_aggr_value(rec_cols, rec, copy_cond.lhs_col, cond.lhs_col.aggregate);
    //         Value rhs_val;  // 条件的右侧值
    //         if (cond.is_rhs_val) {
    //             // 如果右侧是常量值
    //             rhs_val = cond.rhs_val;
    //         } else {
    //             // 如果右侧也是聚合值
    //             // TODO: 检查 rec_cols 是否为空以及索引是否有效
    //             if (copy_cond.rhs_col.col_name == "") {
    //                 // 如果右侧列名为空，可能表示使用第一个列（需要确认逻辑）
    //                 copy_cond.rhs_col.col_name = rec_cols[0].name;
    //             }
    //             // 计算条件的右侧聚合值
    //             rhs_val = get_aggr_value(rec_cols, rec, copy_cond.rhs_col, cond.rhs_col.aggregate);
    //         }
    //         // 比较左右两侧的值
    //         return check_cond(lhs_val, rhs_val, cond.op);
    //     }

    //     /**
    //      * @brief 评估一个分组是否满足所有 HAVING 条件。
    //      * @param rec_cols SELECT 子句中的列元数据
    //      * @param conds HAVING 条件列表
    //      * @param records 当前分组的所有记录
    //      * @return 如果所有条件都满足，则返回 true；否则返回 false。
    //      */
    //     bool eval_aggr_conds(const std::vector<ColMeta>& rec_cols, const std::vector<Condition>& conds,
    //                          std::vector<std::unique_ptr<RmRecord>>& records) {
    //         // 使用 std::all_of 检查是否所有条件都满足
    //         return std::all_of(conds.begin(), conds.end(), [&](const Condition& cond) {
    //             // 对每个条件调用 eval_aggr_cond
    //             return eval_aggr_cond(rec_cols, cond, records);
    //         });
    //     }
};