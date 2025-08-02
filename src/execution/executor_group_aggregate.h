#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>

#include "common/utils/Hash.h"
#include "common/TraceStack.hpp"
#include "executor_abstract.h"

// 为 std::vector<Value> 提供哈希函数
namespace std {
    template<>
    struct hash<std::vector<Value>> {
        size_t operator()(const std::vector<Value>& v) const {
            size_t hash_val = 0;
            for (const auto& value : v) {
                // 根据值的类型计算哈希值
                size_t elem_hash = 0;
                switch (value.type) {
                    case ColType::TYPE_INT:
                        elem_hash = std::hash<int>{}(value.int_val);
                        break;
                    case ColType::TYPE_FLOAT:
                        elem_hash = std::hash<float>{}(value.float_val);
                        break;
                    case ColType::TYPE_STRING:
                        elem_hash = std::hash<std::string>{}(value.str_val);
                        break;
                    default:
                        // 对于其他类型，使用默认哈希
                        elem_hash = 0;
                }
                // 组合哈希值
                hash_val ^= elem_hash + 0x9e3779b9 + (hash_val << 6) + (hash_val >> 2);
            }
            return hash_val;
        }
    };
}


class GroupAggregateExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;    // 指向上一个执行器的智能指针，用于获取输入元组
    std::vector<ColMeta> cols_;               
    std::vector<ColMeta> group_cols_;           // GROUP BY 子句中用于分组的列的元数据
    std::vector<ColMeta> aggr_cols_;            // 聚合列的元数据
    std::vector<Value> aggr_values;             // 存储聚合结果的值
    std::vector<Condition> having_conds_;       // HAVING 子句中的过滤条件
    int offset;                                 // 记录偏移量，用于计算每个列在记录中的位置
    bool is_end_ = false;                       // 标记是否已经结束

    std::unordered_map<std::vector<Value>, std::vector<Value>> grouped_values;
    std::unordered_map<std::vector<Value>, int> grouped_count;  // 存储每个分组的计数
    std::unordered_map<std::vector<Value>, std::vector<Value>>::iterator now_iter;
   public:

    GroupAggregateExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<TabCol> group_cols, std::vector<TabCol> aggr_cols, std::vector<Condition> conds) {
        TRACE_FUNCTION
        prev_ = std::move(prev);

        offset = 0;
        group_cols_.reserve(group_cols.size());
        aggr_cols_.reserve(aggr_cols.size());
        std::for_each(group_cols.begin(), group_cols.end(), [this](const auto& group_col) {
            this->group_cols_.push_back(*get_col(this->prev_->cols(), group_col));
            this->cols_.push_back(this->group_cols_.back());
            this->cols_.back().offset = offset;  // 设置偏移量
            offset += this->cols_.back().len;  // 更新偏移量
        });

        std::for_each(aggr_cols.begin(), aggr_cols.end(), [this](const auto& aggr_col) {
            if (aggr_col.col_name == "*" && aggr_col.agg_type == AggregateType::COUNT) {
                this->aggr_cols_.push_back(ColMeta{"", "*", ColType::TYPE_INT, sizeof(int), 0, false, AggregateType::COUNT});
            } else {
                this->aggr_cols_.push_back(*prev_->get_col(prev_->cols(), aggr_col));
                this->aggr_cols_.back().agg_type = aggr_col.agg_type;
            }
            this->cols_.push_back(this->aggr_cols_.back());
            this->cols_.back().offset = offset;  // 设置偏移量
            if (this->cols_.back().agg_type == AggregateType::AVG) {
                // AVG 聚合类型的列类型为 FLOAT
                this->cols_.back().type = ColType::TYPE_FLOAT;
                this->cols_.back().len = sizeof(float); 
            } else if (this->cols_.back().agg_type == AggregateType::COUNT) {
                // COUNT 聚合类型的列类型为 INT
                this->cols_.back().type = ColType::TYPE_INT;
                this->cols_.back().len = sizeof(int);
            }
            offset += this->cols_.back().len;  // 更新偏移量
        });
        having_conds_ = std::move(conds);
        get_grouped_values();  // 初始化分组聚合结果
    }

    void get_grouped_values() {
        if (group_cols_.empty()) {
            // 没有分组，那么直接统计数量
            aggr_values = init_aggr_values();
            int count_ = 0;
            for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
                auto record = prev_->Next();
                count_++;
                for (size_t i = 0; i < aggr_cols_.size(); i++) {
                    Value val;
                    val.set_col_data(aggr_cols_[i].type, record->data + aggr_cols_[i].offset, aggr_cols_[i].len);
                    add(i, cols_[i].agg_type, aggr_values[i], val);
                }
            }
            if (count_ != 0) {
                for (size_t i = 0; i < aggr_cols_.size(); i++) {
                    if (cols_[i].agg_type == AggregateType::AVG) {
                        aggr_values[i].float_val = aggr_values[i].float_val / count_;
                    }
                }
            }
            std::vector<Value> group_values;
            if (eval_having_conds(group_values, aggr_values) == false) {
                aggr_values.clear();  // 如果 HAVING 条件不满足，则清空聚合结果
            }
        } else {
            for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
                auto record = prev_->Next();
                std::vector<Value> group_;
                group_.reserve(group_cols_.size());
                for (const auto &col : group_cols_) {
                    Value val;
                    val.set_col_data(col.type, record->data + col.offset, col.len);
                    group_.push_back(val);  // 获取分组列的值
                }
                auto it = grouped_values.find(group_);
                if (it == grouped_values.end()) {
                    grouped_values[group_] = init_aggr_values();  // 如果分组不存在，则初始化聚合结果
                    grouped_count[group_] = 0;  // 初始化分组计数
                    it = grouped_values.find(group_);
                }
                grouped_count[group_]++;  // 更新分组计数
                for (size_t i = 0; i < aggr_cols_.size(); i++) {
                    Value val;
                    val.set_col_data(aggr_cols_[i].type, record->data + aggr_cols_[i].offset, aggr_cols_[i].len);
                    add(i, cols_[i + group_cols_.size()].agg_type, (it->second)[i], val);
                }
            }

            for (auto &[group, aggr] : grouped_values) {
                int count_ = grouped_count[group];
                for (size_t i = 0; i < aggr_cols_.size(); i++) {
                    if (cols_[i + group_cols_.size()].agg_type == AggregateType::AVG) {
                        aggr[i].float_val = aggr[i].float_val / count_;  // 计算平均值
                    }
                }
            }

            for (auto it = grouped_values.begin(); it != grouped_values.end();) {
                if (eval_having_conds(it->first, it->second) == false) {
                    it = grouped_values.erase(it);  // 如果 HAVING 条件不满足，则删除该分组
                } else {
                    ++it;  // 否则继续下一个分组
                }
            }
        }
    }

    /**
     * @brief 初始化分组执行过程。
     * 从上一个执行器获取所有元组，进行分组，并根据 HAVING 条件过滤组。
     */
    void beginTuple() override {
        TRACE_FUNCTION
        is_end_ = false;  // 重置结束标志
        if (!group_cols_.empty()) {
            now_iter = grouped_values.begin();  // 初始化迭代器
            if (now_iter == grouped_values.end()) {
                is_end_ = true;  // 如果没有分组，则标记为结束
            }
        } else {
            if (aggr_values.empty()) {
                is_end_ = true;  // 如果没有分组列且没有聚合结果，则标记为结束
            }
        }
        
    }

    /**
     * @brief 移动到下一个分组。
     * 更新 current_group 和 current_tuple。
     */
    void nextTuple() override {
        if (is_end_) {
            return;  // 如果已经结束，则不进行任何操作
        }
        if (group_cols_.empty()) {
            is_end_ = true;  // 如果没有分组，则标记为结束
        } else {
            if (now_iter == grouped_values.end()) {
                is_end_ = true;  // 如果没有更多分组，则标记为结束
            } else {
                now_iter++;
                if (now_iter == grouped_values.end()) {
                    is_end_ = true;  // 如果已经到达最后一个分组之后，则标记为结束
                }
            }
        }
    }

    /**
     * @brief 获取当前分组的代表元组。
     * @return 指向当前分组代表元组的智能指针。
     * 注意：返回的元组所有权被转移。
     */
    std::unique_ptr<RmRecord> Next() override {
        std::unique_ptr<RmRecord> record = std::make_unique<RmRecord>(offset);
        
        auto set_value = [&](const ColMeta &col, const Value &val) {
            if (col.type == ColType::TYPE_INT) {
                memcpy(record->data + col.offset, &(val.int_val), col.len);
            } else if (col.type == ColType::TYPE_FLOAT) {
                memcpy(record->data + col.offset, &(val.float_val), col.len);
            } else if (col.type == ColType::TYPE_STRING) {
                memset(record->data + col.offset, 0, col.len);  // 对于字符串类型，清空数据
                memcpy(record->data + col.offset, val.str_val.c_str(), val.str_val.size());
            } else {
                throw InternalError("Unsupported type in GroupAggregateExecutor::Next");
            }
        };
        
        if (group_cols_.empty()) {
            for (size_t i = 0; i < aggr_cols_.size(); i++) {
                set_value(cols_[i], aggr_values[i]);
            }
        } else {
            // 有分组的情况，需要处理分组列和聚合列
            // 首先处理分组列
            for (size_t i = 0; i < group_cols_.size(); i++) {
                const auto& col = cols_[i];
                const auto& val = now_iter->first[i];
                set_value(col, val);  // 设置分组列的值
            }
            
            // 再处理聚合列
            for (size_t i = 0; i < aggr_cols_.size(); i++) {
                const auto& col = cols_[i + group_cols_.size()];
                const auto& val = now_iter->second[i];
                set_value(col, val);  // 设置聚合列的值
            }
        }
        return record;
    }

    const std::vector<ColMeta>& cols() const override { return cols_; }

    Rid& rid() override { return _abstract_rid; }  // 返回基类中的默认 RID

    bool is_end() const override { return is_end_; }

    size_t tupleLen() const override { return offset; };

    std::string getType() override { return "GroupAggregateExecutor"; };

   private:
    bool eval_having_conds(const std::vector<Value> &group_, const std::vector<Value> &aggr_) {
        auto get_val = [&](const TabCol &col) -> Value {
            auto pos = get_col(cols_, col, true);
            int index = pos - cols_.begin();
            if (index < static_cast<int>(group_cols_.size())) {
                return group_[index];  // 返回分组列的值
            } else {
                index -= (int)group_cols_.size();
                return aggr_[index];  // 返回聚合列的值
            }
        };
        
        for (const auto &cond : having_conds_) {
            Value lhs_val = get_val(cond.lhs_col);  // 获取左侧列的值
            Value rhs_val;

            // 根据 rhs_type 获取右侧操作数信息
            switch (cond.rhs_type) {
                case ConditionRhsType::RHS_VALUE:
                    rhs_val = cond.rhs_val;
                    break;
                case ConditionRhsType::RHS_COLUMN: {
                    rhs_val = get_val(cond.rhs_col);  // 获取右侧列的值
                    break;
                }
                default:
                    throw RMDBError("Unsupported ConditionRhsType");
            }
            if (Value::compare(lhs_val, rhs_val, cond.op) == false) {
                return false;  // 如果有一个条件不满足，则返回 false
            }
        }
        
        return true;  // 所有条件都满足，返回 true
    }

    void add(int i, AggregateType type, Value &aggr_value, const Value& val) {
        switch (type) {
            case AggregateType::COUNT:
                aggr_value.int_val += 1;  // COUNT 聚合类型直接计数
                break;
            case AggregateType::SUM:
                if (aggr_value.type == ColType::TYPE_INT) {
                    aggr_value.int_val += val.int_val;  // 累加整数值
                } else if (aggr_value.type == ColType::TYPE_FLOAT) {
                    aggr_value.float_val += val.float_val;  // 累加浮点值
                } else {
                    throw InternalError("Unsupported type for SUM in GroupAggregateExecutor::add");
                }
                break;
            case AggregateType::AVG:
                if (val.type == ColType::TYPE_INT) {
                    aggr_value.float_val += val.int_val;  // 累加整数值
                } else if (val.type == ColType::TYPE_FLOAT) {
                    aggr_value.float_val += val.float_val;  // 累加浮点值
                } else {
                    throw InternalError("Unsupported type for AVG in GroupAggregateExecutor::add");
                }
                break;
            case AggregateType::MAX:
                if (aggr_value.type == ColType::TYPE_INT) {
                    aggr_value.int_val = std::max(aggr_value.int_val, val.int_val);  // 更新最大值
                } else if (aggr_value.type == ColType::TYPE_FLOAT) {
                    aggr_value.float_val = std::max(aggr_value.float_val, val.float_val);  // 更新最大值
                } else if (aggr_value.type == ColType::TYPE_STRING) {
                    aggr_value.str_val = std::max(aggr_value.str_val, val.str_val);  // 更新最大字符串
                } else {
                    throw InternalError("Unsupported type for MAX in GroupAggregateExecutor::add");
                }
                break;
            case AggregateType::MIN:
                if (aggr_value.type == ColType::TYPE_INT) {
                    aggr_value.int_val = std::min(aggr_value.int_val, val.int_val);  // 更新最小值
                } else if (aggr_value.type == ColType::TYPE_FLOAT) {
                    aggr_value.float_val = std::min(aggr_value.float_val, val.float_val);  // 更新最小值
                } else if (aggr_value.type == ColType::TYPE_STRING) {
                    aggr_value.str_val = std::min(aggr_value.str_val, val.str_val);  // 更新最小字符串
                } else {
                    throw InternalError("Unsupported type for MIN in GroupAggregateExecutor::add");
                }
                break;
            default:
                throw InternalError("Unsupported aggregate type in GroupAggregateExecutor::add");
        }
    }

    std::vector<Value> init_aggr_values() {
        std::vector<Value> values;
        values.reserve(aggr_cols_.size());
        // 聚合列从 group_cols_ 之后开始
        for (auto it = cols_.begin() + group_cols_.size(); it != cols_.end(); ++it) {
            Value val;
            if (it->type == ColType::TYPE_INT) {
                if (it->agg_type == AggregateType::MAX) {
                    val.set_int(std::numeric_limits<int>::min());  // MAX 初始化为最小值
                } else if (it->agg_type == AggregateType::MIN) {
                    val.set_int(std::numeric_limits<int>::max());  // MIN 初始化为最大值
                } else {
                    val.set_int(0);  // COUNT 和 SUM 初始化为 0
                }
            } else if (it->type == ColType::TYPE_FLOAT) {
                if (it->agg_type == AggregateType::MAX) {
                    val.set_float(std::numeric_limits<float>::lowest());  // MAX 初始化为最小浮点数
                } else if (it->agg_type == AggregateType::MIN) {
                    val.set_float(std::numeric_limits<float>::max());  // MIN 初始化为最大浮点数
                } else {
                    val.set_float(0.0f);  // COUNT 和 SUM 初始化为 0.0
                }
            } else if (it->type == ColType::TYPE_STRING) {
                if (it->agg_type == AggregateType::MAX) {
                    val.set_str(std::string(it->len, 0));
                } else if (it->agg_type == AggregateType::MIN) {
                    val.set_str(std::string(it->len, 255));
                } else if (it->agg_type == AggregateType::COUNT) {
                    val.set_int(0);  // COUNT 初始化为 0
                } else {
                    throw InternalError("String aggregate unsupported in GroupAggregateExecutor::init_aggr_values");
                }
            } else {
                throw InternalError("Unsupported aggregate column type in GroupAggregateExecutor::init_aggr_values");
            }
            values.emplace_back(std::move(val));  // 将初始化的值添加到结果向量中
        }
        return values;
    }
};