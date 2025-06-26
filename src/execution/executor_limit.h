#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class LimitExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> child_;  // 子执行器
    int offset_;                               // 跳过的记录数
    int count_;                                // 返回的记录数
    int current_;                              // 当前处理的记录数

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
    }

    void nextTuple() override {
        child_->nextTuple();
        current_++;
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return child_->Next();
    }

    bool is_end() const override { return child_->is_end() || (current_ >= count_); }

    Rid &rid() override { return child_->rid(); }

    const std::vector<ColMeta> &cols() const override { return child_->cols(); }

    std::string getType() override { return "LimitExecutor"; }
};