#pragma once

#include "common/TraceStack.hpp"
#include "common/print.hpp"
#include "execution/execution_group.h"
#include "execution/executor_abstract.h"

/**
 * @brief 聚合执行器类，用于执行SQL中的聚合操作（如COUNT、SUM、AVG等）
 * 继承自AbstractExecutor抽象执行器类
 */
class AggregateExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;                           // 前一个执行器
    std::vector<ColMeta> cols_;                                        // 输入列的元数据
    std::vector<ColMeta> output_cols_;                                 // 输出列的元数据
    std::vector<AggregateType> agg_types_;                             // 聚合类型列表
    std::vector<std::unique_ptr<RmRecord>> aggregated_records_;        // 聚合后的记录
    std::vector<std::unique_ptr<RmRecord>>::iterator current_record_;  // 当前记录迭代器

   public:
    /**
     * @brief 构造函数
     * @param prev 前一个执行器
     * @param sel_cols 选择的列
     * @param agg_types 聚合类型
     */
    AggregateExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol>& sel_cols,
                      const std::vector<AggregateType>& agg_types)
        : prev_(std::move(prev)), agg_types_(agg_types) {
        TRACE_FUNCTION
        for (const auto& sel_col : sel_cols) {
            // 处理COUNT(*)的特殊情况
            if (sel_col.col_name == "*" && sel_col.agg_type == AggregateType::COUNT) {
                cols_.push_back(
                    ColMeta{.tab_name = "", .name = "*", .type = ColType::TYPE_INT, .len = sizeof(int), .offset = 0});
            } else {
                // 从前一个执行器获取列元数据
                cols_.push_back(*prev_->get_col(prev_->cols(), sel_col));
            }
            output_cols_.push_back(cols_.back());
        }

        size_t beginIndex = 0;
        // 设置第一列的偏移量为0
        output_cols_.front().offset = 0;
        // 如果第一个聚合类型是COUNT，设置其类型为整数
        if (agg_types.front() == AggregateType::COUNT) {
            output_cols_.front().agg_type = agg_types.front();
            output_cols_.front().type = ColType::TYPE_INT;
            output_cols_.front().len = sizeof(int);
            beginIndex = 1;
        }

        // 设置其他列的类型和偏移量
        for (size_t i = beginIndex; i < output_cols_.size(); ++i) {
            // 根据聚合类型设置输出类型
            switch (agg_types[i]) {
                // 保持原始列类型
                case AggregateType::SUM:
                case AggregateType::MIN:
                case AggregateType::MAX:
                case AggregateType::NONE:
                    output_cols_[i].type = cols_[i].type;
                    output_cols_[i].len = cols_[i].len;
                    break;
                case AggregateType::COUNT:
                    output_cols_[i].type = ColType::TYPE_INT;
                    output_cols_[i].len = sizeof(int);
                    break;
                case AggregateType::AVG:
                    // AVG结果总是FLOAT类型
                    output_cols_[i].type = ColType::TYPE_FLOAT;
                    output_cols_[i].len = sizeof(float);
                    break;
                default:
                    throw InternalError("Unknown aggregate type");
            }
            output_cols_[i].agg_type = agg_types[i];  // 设置聚合类型
            // 计算列的偏移量（基于前一列的偏移量和长度）
            if (i != 0) output_cols_[i].offset = output_cols_[i - 1].offset + output_cols_[i - 1].len;
        }
    }

    /**
     * @brief 开始元组遍历，执行聚合操作
     */
    void beginTuple() override {
        prev_->beginTuple();

        // 检查前一个执行器是否为分组执行器
        if (auto group_executor = dynamic_cast<GroupExecutor*>(prev_.get())) {
            // 对每个分组执行聚合操作
            for (const auto& group : group_executor->grouped_records) {
                auto record = aggregateGroup(group.second);
                if (record) {
                    aggregated_records_.push_back(std::move(record));
                }
            }
        } else {
            // 没有分组，对所有记录执行聚合操作
            std::vector<std::unique_ptr<RmRecord>> records;
            while (!prev_->is_end()) {
                records.push_back(prev_->Next());
                prev_->nextTuple();
            }
            auto record = aggregateGroup(records);
            if (record) {
                aggregated_records_.push_back(std::move(record));
            }
        }
        // 初始化当前记录迭代器
        current_record_ = aggregated_records_.begin();
    }

    /**
     * @brief 移动到下一个元组
     */
    void nextTuple() override {
        if (current_record_ != aggregated_records_.end()) {
            ++current_record_;
        }
    }

    /**
     * @brief 获取当前记录
     * @return 当前记录的副本
     */
    std::unique_ptr<RmRecord> Next() override { return std::make_unique<RmRecord>(**current_record_); }

    /**
     * @brief 检查是否已到达结尾
     * @return 如果已到达结尾返回true，否则返回false
     */
    bool is_end() const override { return current_record_ == aggregated_records_.end(); }

    /**
     * @brief 获取输出列信息
     * @return 输出列的元数据向量
     */
    const std::vector<ColMeta>& cols() const override { return output_cols_; }

    /**
     * @brief 获取记录ID
     * @return 抽象记录ID的引用
     */
    Rid& rid() override { return _abstract_rid; }

    /**
     * @brief 获取执行器类型
     * @return 聚合执行器类型
     */
    std::string getType() override { return "AggregateExecutor"; }

   private:
    /**
     * @brief 对一组记录执行聚合操作
     * @param records 待聚合的记录列表
     * @return 聚合后的记录，如果为空则返回nullptr
     */
    std::unique_ptr<RmRecord> aggregateGroup(const std::vector<std::unique_ptr<RmRecord>>& records) {
        // 有记录的情况，执行实际的聚合计算
        size_t size = 0;
        std::vector<Value> values(agg_types_.size());
        for (size_t i = 0; i < agg_types_.size(); ++i) {
            // 调用聚合函数计算结果
            Value res;
            if (!records.empty()) {
                res = get_aggr_value(cols_, records, TabCol(cols_[i].tab_name, cols_[i].name), agg_types_[i]);
            } else {
                // 处理NULL值情况
                switch (agg_types_[i]) {
                    case AggregateType::COUNT:
                        res.set_int(0);
                        break;
                    case AggregateType::MIN:
                    case AggregateType::MAX:
                    case AggregateType::SUM:
                        if (output_cols_[i].type == ColType::TYPE_INT) res.set_int(0);
                        else res.set_float(0.0);
                        break;
                    case AggregateType::AVG:
                        res.set_float(0.0);
                        break;
                    default:
                        throw InternalError("Unknown aggregate type");
                }
            }
            res.init_raw();
            size += res.raw->size;
            values[i] = std::move(res);
        }
        auto result = std::make_unique<RmRecord>(size);
        size_t offset = 0;
        for (size_t i = 0; i < values.size(); ++i) {
            memcpy(result->data + offset, values[i].raw->data, values[i].raw->size);
            offset += values[i].raw->size;
        }
        return result;
    }
};