#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include "common/BatchArray.hpp"

class LimitExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> child_;    // 子执行器
    int offset_;                                 // 跳过的记录数
    int count_;                                  // 返回的记录数
    int current_;                                // 当前处理的记录数
    std::unique_ptr<BatchRecord> result_batch_;  // 结果批次

   public:
    LimitExecutor(std::unique_ptr<AbstractExecutor> child, int offset, int count)
        : child_(std::move(child)), offset_(offset), count_(count), current_(0) {}

    void beginTuple() override {
        current_ = 0;
        child_->beginTuple();
        // 跳过offset条记录
        for (int i = 0; i < offset_ && !child_->is_end(); i++) {
            child_->nextTuple();
        }
        // 获取第一批数据
        nextTuple();
    }

    void nextTuple() override {
        result_batch_ = std::make_unique<BatchRecord>();
        if (is_end()) return;

        auto child_batch = child_->Next();
        if (child_batch) {
            for (auto &rec : *child_batch) {
                if (current_ < count_) {
                    result_batch_->push_back(std::move(rec));
                    current_++;
                } else {
                    break;
                }
            }
        }
        child_->nextTuple();
    }

    std::unique_ptr<BatchRecord> Next() override {
        if (is_end() && (!result_batch_ || result_batch_->empty())) {
            return nullptr;
        }
        return std::move(result_batch_);
    }

    bool is_end() const override { return child_->is_end() || (current_ >= count_); }

    Rid &rid() override { return child_->rid(); }

    const std::vector<ColMeta> &cols() const override { return child_->cols(); }

    std::string getType() override { return "LimitExecutor"; }
};