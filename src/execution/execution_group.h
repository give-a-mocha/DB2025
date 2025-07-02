#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/Hash.h"
#include "common/TraceStack.hpp"
#include "executor_abstract.h"

struct ListHash {
    size_t operator()(const std::list<std::string_view>& v) const {
        size_t hash = 5381;
        for (auto& i : v) {
            hash = getHashCode(i, hash);
        }
        return hash;
    }
};

class GroupExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;  ///< 指向上一个执行器的智能指针，用于获取输入元组
    std::vector<ColMeta> cols_;               ///< SELECT 子句中选择的列的元数据
    std::vector<ColMeta> group_cols_;         ///< GROUP BY 子句中用于分组的列的元数据
    std::vector<Condition> having_conds_;     ///< HAVING 子句中的过滤条件

   public:
    /**
     * @brief 用于存储分组后的记录。
     * 键是根据 group_cols_ 生成的字符串，值是属于该组的 RmRecord 列表。
     */
    std::unordered_map<std::list<std::string_view>, std::vector<std::unique_ptr<RmRecord>>, ListHash> grouped_records;
    std::unordered_map<std::list<std::string_view>, std::vector<std::unique_ptr<RmRecord>>, ListHash>::iterator
        now_iter;

    GroupExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol>& sel_cols,
                  const std::vector<TabCol>& group_cols, std::vector<Condition> conds) {
        TRACE_FUNCTION
        prev_ = std::move(prev);

        std::for_each(sel_cols.begin(), sel_cols.end(), [this](const auto& sel_col) {
            if (sel_col.col_name == "*" && sel_col.agg_type == AggregateType::COUNT) {
                cols_.push_back(
                    ColMeta{.tab_name = "", .name = "*", .type = ColType::TYPE_INT, .len = sizeof(int), .offset = 0});
            } else {
                cols_.push_back(*get_col(prev_->cols(), sel_col));
            }
            cols_.back().agg_type = sel_col.agg_type;  // 设置聚合类型
        });
        // 处理 GROUP BY 列元数据
        std::for_each(group_cols.begin(), group_cols.end(), [this](const auto& group_col) {
            this->group_cols_.push_back(*get_col(this->prev_->cols(), group_col));
        });
        having_conds_ = std::move(conds);
    }
    /**
     * @brief 初始化分组执行过程。
     * 从上一个执行器获取所有元组，进行分组，并根据 HAVING 条件过滤组。
     */
    void beginTuple() override {
        TRACE_FUNCTION
        prev_->beginTuple();      // 初始化上一个执行器
        grouped_records.clear();  // 清空上一次执行的分组记录
        while (!prev_->is_end()) {
            auto tuple = prev_->Next();
            std::list<std::string_view> group_key = generateGroupKey(tuple);
            grouped_records[group_key].emplace_back(std::move(tuple));
            prev_->nextTuple();  // 移动到上一个执行器的下一个元组
        }

        // 根据 HAVING 条件过滤分组
        if (!having_conds_.empty()) {
            for (auto it = grouped_records.begin(); it != grouped_records.end();) {
                if (!eval_aggr_conds(cols_, having_conds_, it->second)) {
                    it = grouped_records.erase(it);
                } else {
                    ++it;
                }
            }
        }
        now_iter = grouped_records.begin();
    }

    /**
     * @brief 移动到下一个分组。
     * 更新 current_group 和 current_tuple。
     */
    void nextTuple() override {
        TRACE_FUNCTION
        if (now_iter != grouped_records.end()) {
            now_iter++;
        }
    }

    /**
     * @brief 获取当前分组的代表元组。
     * @return 指向当前分组代表元组的智能指针。
     * 注意：返回的元组所有权被转移。
     */
    std::unique_ptr<RmRecord> Next() override {
        const auto& front = now_iter->second.front();
        return std::make_unique<RmRecord>(front->size, front->data);
    }

    /**
     * @brief 获取输出列的元数据。
     * @return 包含输出列元数据的常量引用。
     * 注意：这里返回的是上一个执行器的列元数据，
     * 实际输出的列（包括聚合结果）由 ProjectionExecutor 处理。
     */
    const std::vector<ColMeta>& cols() const override { return prev_->cols(); }

    /**
     * @brief 获取记录标识符（RID）。
     * 对于 GroupExecutor，RID 通常没有明确意义，返回一个默认值。
     * @return 记录标识符的引用。
     */
    Rid& rid() override { return _abstract_rid; }  // 返回基类中的默认 RID

    /**
     * @brief 检查是否已处理完所有分组。
     * @return 如果已到达最后一个分组之后，则返回 true；否则返回 false。
     */
    bool is_end() const override { return now_iter == grouped_records.end(); }

    /**
     * @brief 获取执行器的类型。
     * @return 执行器类型枚举值 ExecutorType::GROUP。
     */
    std::string getType() override { return "GroupExecutor"; };

   private:
    /**
     * @brief 根据分组列为给定的记录生成分组键。
     * @param record 指向要生成键的记录的智能指针
     * @return 生成的分组键字符串。
     */
    std::list<std::string_view> generateGroupKey(std::unique_ptr<RmRecord>& record) {
        TRACE_FUNCTION
        std::list<std::string_view> group_key_list;
        // 遍历所有分组列
        for (const auto& group_col : group_cols_) {
            // 获取列数据在记录中的起始地址
            const char* col_data = record->data + group_col.offset;
            group_key_list.emplace_back(col_data, group_col.len);
        }
        return group_key_list;
    }

    /**
     * @brief 评估一个分组是否满足所有 HAVING 条件。
     * @param rec_cols SELECT 子句中的列元数据
     * @param conds HAVING 条件列表
     * @param records 当前分组的所有记录
     * @return 如果所有条件都满足，则返回 true；否则返回 false。
     */
    bool eval_aggr_conds(const std::vector<ColMeta>& rec_cols, const std::vector<Condition>& conds,
                         const std::vector<std::unique_ptr<RmRecord>>& records) {
        TRACE_FUNCTION
        // return std::all_of(conds.begin(), conds.end(), [&](const Condition& cond) {
        //     // 对每个条件调用 eval_aggr_cond
        //     return eval_aggr_cond(rec_cols, cond, records);
        // });
        std::vector<TabCol> lhs_cols;
        std::vector<AggregateType> lhs_agg_types;
        lhs_cols.reserve(conds.size());
        lhs_agg_types.reserve(conds.size());
        std::for_each(conds.begin(), conds.end(), [&](const Condition& cond) {
            lhs_cols.emplace_back(cond.lhs_col);
            lhs_agg_types.emplace_back(cond.lhs_col.agg_type);
        });
        std::vector<Value> lhs_vals = get_aggr_values(rec_cols, records, lhs_cols, lhs_agg_types);
        std::vector<Value> rhs_vals(conds.size());
        std::vector<TabCol> rhs_cols;
        std::vector<AggregateType> rhs_agg_types;
        rhs_cols.reserve(conds.size());
        rhs_agg_types.reserve(conds.size());
        std::for_each(conds.begin(), conds.end(), [&](const Condition& cond) {
            switch (cond.rhs_type) {
                case ConditionRhsType::RHS_VALUE:
                    break;
                case ConditionRhsType::RHS_COLUMN:
                    rhs_cols.emplace_back(cond.rhs_col);
                    rhs_agg_types.emplace_back(cond.rhs_col.agg_type);
                    break;
                case ConditionRhsType::RHS_EXPR:
                    // TODO: 实现 EvaluateAggrExpr 来处理包含聚合的表达式
                    // rhs_val = EvaluateAggrExpr(ExprTerm(cond.rhs_expr), rec, rec_cols);
                    // 暂时抛出错误，因为 EvaluateExpr 不适用于聚合上下文
                    throw RMDBError(
                        "Arithmetic expressions involving aggregates in HAVING clause RHS not yet fully supported.");
                    break;
                default:
                    throw RMDBError("Unsupported ConditionRhsType in HAVING clause");
            }
        });
        std::vector<Value> rhs_aggr_vals = get_aggr_values(rec_cols, records, rhs_cols, rhs_agg_types);
        for (size_t i = 0, j = 0; i < conds.size(); i++) {
            switch (conds[i].rhs_type) {
                case ConditionRhsType::RHS_VALUE:
                    rhs_vals[i] = conds[i].rhs_val;  // 直接使用 RHS_VALUE
                    break;
                case ConditionRhsType::RHS_COLUMN:
                    rhs_vals[i] = rhs_aggr_vals[j];  // 使用聚合计算的值
                    j++;
                    break;
                default:
                    throw RMDBError("Unsupported ConditionRhsType in HAVING clause");
            }
        }
        // 遍历所有条件，检查是否满足
        for (size_t i = 0; i < conds.size(); i++) {
            if (!Value::compare(lhs_vals[i], rhs_vals[i], conds[i].op)) {
                return false;  // 如果有一个条件不满足，则返回 false
            }
        }
        return true;
    }

    /**
     * @brief 评估单个聚合条件。
     * @param rec_cols SELECT 子句中的列元数据（用于查找聚合函数的目标列）
     * @param cond 要评估的条件
     * @param rec 当前分组的所有记录
     * @return 如果条件满足，则返回 true；否则返回 false。
     */
    [[maybe_unused]] bool eval_aggr_cond(const std::vector<ColMeta>& rec_cols, const Condition& cond,
                                         const std::vector<std::unique_ptr<RmRecord>>& rec) {
        // 计算条件的左侧聚合值
        Value lhs_val = get_aggr_value(rec_cols, rec, cond.lhs_col, cond.lhs_col.agg_type);
        Value rhs_val;  // 条件的右侧值
        // 根据 rhs_type 获取右侧值
        switch (cond.rhs_type) {
            case ConditionRhsType::RHS_VALUE:
                rhs_val = cond.rhs_val;
                break;
            case ConditionRhsType::RHS_COLUMN:  // 假设 RHS_COLUMN 在 HAVING 中意味着聚合
                if (cond.rhs_col.col_name.empty()) {
                    throw InternalError("Aggregate column name cannot be empty in HAVING clause RHS");
                }
                // 计算条件的右侧聚合值
                rhs_val = get_aggr_value(rec_cols, rec, cond.rhs_col, cond.rhs_col.agg_type);
                break;
            case ConditionRhsType::RHS_EXPR:
                // TODO: 实现 EvaluateAggrExpr 来处理包含聚合的表达式
                // rhs_val = EvaluateAggrExpr(ExprTerm(cond.rhs_expr), rec, rec_cols);
                // 暂时抛出错误，因为 EvaluateExpr 不适用于聚合上下文
                throw RMDBError(
                    "Arithmetic expressions involving aggregates in HAVING clause RHS not yet fully supported.");
                break;
            default:
                throw RMDBError("Unsupported ConditionRhsType in HAVING clause");
        }
        // 比较左右两侧的值
        return Value::compare(lhs_val, rhs_val, cond.op);
    }
};