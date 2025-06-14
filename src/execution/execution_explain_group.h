#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "executor_abstract.h"


class ExplainGroupExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;  ///< 指向上一个执行器的智能指针，用于获取输入元组
    std::vector<TabCol> group_cols_;         ///< GROUP BY 子句中用于分组的列的元数据
    std::vector<Condition> having_conds_;     ///< HAVING 子句中的过滤条件
    int offset_;
    public:

    ExplainGroupExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<TabCol> group_cols, std::vector<Condition> conds, int offset) {
        prev_ = std::move(prev);
        offset_ = offset;
        group_cols_ = std::move(group_cols);
        having_conds_ = std::move(conds);

    }
    

    std::unique_ptr<RmRecord> Next() override {
        std::string res = std::string(offset_, '\t');
        res += "GroupBy(columns=[";
        for (size_t i = 0; i < group_cols_.size(); i++) {
            if (i > 0) res += ", ";
            res += group_cols_[i].to_string();
        }
        res += "], having=[";
        for (size_t i = 0; i < having_conds_.size(); i++) {
            if (i > 0) res += ", ";
            res += having_conds_[i].to_string();
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
     * @brief 获取记录标识符（RID）。
     * 对于 GroupExecutor，RID 通常没有明确意义，返回一个默认值。
     * @return 记录标识符的引用。
     */
    Rid& rid() override { return _abstract_rid; }  // 返回基类中的默认 RID

    
    /**
     * @brief 获取执行器的类型。
     * @return 执行器类型枚举值 ExecutorType::GROUP。
     */
    std::string getType() override { return "ExplainGroupExecutor"; };

};