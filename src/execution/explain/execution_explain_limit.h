#pragma once

#include "execution/execution_defs.h"
#include "execution/execution_manager.h"
#include "execution/executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class ExplainLimitExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> child_;  // 子执行器
    int offset_;                               // 跳过的记录数
    int count_;                                // 返回的记录数
    int output_offset_;                        // 输出的缩进偏移量

   public:
    ExplainLimitExecutor(std::unique_ptr<AbstractExecutor> child, int offset, int count, int output_offset)
        : child_(std::move(child)), offset_(offset), count_(count), output_offset_(output_offset) {}

    std::unique_ptr<RmRecord> Next() override {
        // 按指定缩进生成输出
        std::string res = std::string(output_offset_, '\t');
        res += "Limit(offset=" + std::to_string(offset_) + ", count=" + std::to_string(count_) + ")\n";
        // 将解释写入输出文件
        std::fstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << res;
        outfile.close();

        // 递归解释前序节点
        child_->Next();
        return nullptr;
    }

    Rid &rid() override { return child_->rid(); }

    std::string getType() override { return "ExplainLimitExecutor"; }
};