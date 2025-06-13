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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 嵌套循环连接执行器，负责实现两个表的连接操作
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;   // 外表执行器
    std::unique_ptr<AbstractExecutor> right_;  // 内表执行器
    size_t len_;                               // 连接结果记录长度
    std::vector<ColMeta> cols_;                // 结果集列元数据
    std::vector<Condition> fed_conds_;         // 连接条件列表
    bool _is_end;                              // 扫描结束标志

   public:
    /**
     * @brief 构造函数
     *
     * 初始化连接执行器：
     * 1. 设置左右表的执行器
     * 2. 计算连接结果的记录长度
     * 3. 合并左右表的列元数据
     * 4. 调整右表列的偏移量
     *
     * @param left 左表执行器
     * @param right 右表执行器
     * @param conds 连接条件
     */
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        _is_end = false;
        fed_conds_ = std::move(conds);
    }

    /**
     * @brief 开始连接操作
     *
     * 初始化左右表的扫描并找到第一对满足条件的记录：
     * 1. 开始扫描左右表
     * 2. 检查是否有记录可用
     * 3. 查找第一对满足连接条件的记录
     */
    void beginTuple() override {
        left_->beginTuple();
        right_->beginTuple();
        if (left_->is_end() || right_->is_end()) {
            _is_end = true;
            return;
        }
        find_record();
    }

    /**
     * @brief 移动到下一对满足连接条件的记录
     *
     * 使用嵌套循环策略查找下一组记录：
     * 1. 移动左表记录
     * 2. 如果左表到达末尾，移动右表记录并重置左表
     * 3. 继续查找满足条件的记录对
     */
    void nextTuple() override {
        if (is_end()) return;
        left_->nextTuple();
        if (left_->is_end()) {
            right_->nextTuple();
            left_->beginTuple();
        }
        find_record();
    }

    /**
     * @brief 检查连接操作是否完成
     * @return 如果所有记录都已处理完返回true，否则返回false
     */
    bool is_end() const override { return _is_end; }

    /**
     * @brief 获取当前连接结果记录
     *
     * 将左右表的当前记录合并成一条新记录：
     * 1. 分别获取左右表的记录
     * 2. 创建新记录并分配空间
     * 3. 将左右表的记录数据复制到新记录中
     *
     * @return 合并后的记录指针，如果任一表的记录为空则返回nullptr
     */
    std::unique_ptr<RmRecord> Next() override {
        auto rec = std::make_unique<RmRecord>(len_);
        auto left_rec = left_->Next();
        auto right_rec = right_->Next();

        // 检查空指针，如果任一记录为空则返回空指针
        if (!left_rec || !right_rec) {
            std::cerr << "Error: One of the records is null at " + getType() << std::endl;
            return nullptr;
        }

        memcpy(rec->data, left_rec->data, left_->tupleLen());
        memcpy(rec->data + left_->tupleLen(), right_rec->data, right_->tupleLen());
        return rec;
    }

    /**
     * @brief 获取连接结果记录的长度
     * @return 连接后记录的总字节数（左表记录长度 + 右表记录长度）
     */
    size_t tupleLen() const override { return len_; }

    /**
     * @brief 获取连接结果的列元数据
     * @return 合并后的列元数据向量引用（包含左右表的所有列）
     */
    const std::vector<ColMeta> &cols() const override { return cols_; }

    /**
     * @brief 获取当前记录的RID
     * @return 抽象RID的引用（连接结果没有实际的RID）
     */
    Rid &rid() override { return _abstract_rid; }

   private:
    /**
     * @brief 查找下一对满足连接条件的记录
     */
    void find_record() {
        while (!is_end()) {
            if (left_->is_end()) {
                right_->nextTuple();
                if (right_->is_end()) {
                    _is_end = true;
                    return;
                }
                left_->beginTuple();
                continue;
            }

            auto left_rec = left_->Next();
            auto right_rec = right_->Next();
            if (!left_rec || !right_rec) {
                _is_end = true;
                return;
            }

            auto rec = std::make_unique<RmRecord>(len_);
            memcpy(rec->data, left_rec->data, left_->tupleLen());
            memcpy(rec->data + left_->tupleLen(), right_rec->data, right_->tupleLen());
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                return;
            }
            left_->nextTuple();
        }
        _is_end = true;
    }

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    std::string getType() override { return "NestedLoopJoinExecutor"; }
};