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
#include "common/BatchArray.hpp"

/**
 * @brief 嵌套循环连接执行器，负责实现两个表的连接操作
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;
    std::unique_ptr<AbstractExecutor> right_;
    size_t len_;                           // 连接结果记录长度
    std::vector<ColMeta> cols_;            // 结果集列元数据
    std::vector<Condition> fed_conds_;     // 连接条件列表
    bool _is_end;                          // 扫描结束标志
    
    // 批处理相关状态
    std::unique_ptr<BatchRecord> left_batch_;     // 当前左表批次
    std::unique_ptr<BatchRecord> right_batch_;    // 当前右表批次
    std::unique_ptr<BatchRecord> result_batch_;   // 结果批次
    
    // 批次内索引
    size_t left_idx_;           // 当前处理的左表记录索引
    size_t right_idx_;          // 当前处理的右表记录索引
    bool left_exhausted_;       // 左表是否已用完
    bool right_exhausted_;      // 右表是否已用完

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
        
        // 初始化批处理状态
        left_idx_ = 0;
        right_idx_ = 0;
        left_exhausted_ = false;
        right_exhausted_ = false;
    }

    /**
     * @brief 开始连接操作
     *
     * 初始化批处理连接：
     * 1. 开始扫描左右表
     * 2. 获取第一批记录
     * 3. 生成第一批连接结果
     */
    void beginTuple() override {
        TRACE_FUNCTION
        left_->beginTuple();
        right_->beginTuple();
        
        // 初始化批处理状态
        left_idx_ = 0;
        right_idx_ = 0;
        left_exhausted_ = false;
        right_exhausted_ = false;
        
        // 获取第一批记录
        fetch_left_batch();
        fetch_right_batch();
        
        // 生成第一批结果
        generate_result_batch();
    }

    /**
     * @brief 移动到下一批连接结果
     *
     * 批处理嵌套循环策略：
     * 1. 继续从当前位置处理批次
     * 2. 管理左右表批次的切换
     * 3. 生成下一批连接结果
     */
    void nextTuple() override {
        TRACE_FUNCTION
        if (is_end()) return;
        
        // 生成下一批结果
        generate_result_batch();
    }

    /**
     * @brief 检查连接操作是否完成
     * @return 如果所有记录都已处理完返回true，否则返回false
     */
    bool is_end() const override { return _is_end; }

    /**
     * @brief 获取当前批次的连接结果记录
     *
     * @return 当前批次的连接结果
     */
    std::unique_ptr<BatchRecord> Next() override {
        TRACE_FUNCTION
        return std::move(result_batch_);
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
     * @brief 获取左表下一批记录
     */
    void fetch_left_batch() {
        TRACE_FUNCTION
        if (left_->is_end()) {
            left_exhausted_ = true;
            return;
        }
        left_batch_ = left_->Next();
        if (!left_batch_ || left_batch_->empty()) {
            left_exhausted_ = true;
        } else {
            left_idx_ = 0;
        }
    }
    
    /**
     * @brief 获取右表下一批记录
     */
    void fetch_right_batch() {
        TRACE_FUNCTION
        if (right_->is_end()) {
            right_exhausted_ = true;
            return;
        }
        right_batch_ = right_->Next();
        if (!right_batch_ || right_batch_->empty()) {
            right_exhausted_ = true;
        } else {
            right_idx_ = 0;
        }
    }
    
    /**
     * @brief 重置右表扫描
     */
    void reset_right_scan() {
        TRACE_FUNCTION
        right_->beginTuple();
        right_exhausted_ = false;
        fetch_right_batch();
    }
    
    /**
     * @brief 生成结果批次
     */
    void generate_result_batch() {
        TRACE_FUNCTION
        result_batch_ = std::make_unique<BatchRecord>();
        
        while (!left_exhausted_ && !result_batch_->full()) {
            // 如果右表批次用完，重置右表
            if (right_exhausted_ || right_idx_ >= right_batch_->size()) {
                left_idx_++;
                if (left_idx_ >= left_batch_->size()) {
                    // 左表当前批次用完，获取下一批
                    left_->nextTuple();
                    fetch_left_batch();
                    if (left_exhausted_) break;
                }
                reset_right_scan();
                continue;
            }
            
            // 处理当前左右表记录对
            if (left_idx_ < left_batch_->size() && right_idx_ < right_batch_->size()) {
                auto& left_rec = *(left_batch_->begin() + left_idx_);
                auto& right_rec = *(right_batch_->begin() + right_idx_);
                
                // 创建连接记录
                auto joined_rec = std::make_unique<RmRecord>(len_);
                memcpy(joined_rec->data, left_rec->data, left_->tupleLen());
                memcpy(joined_rec->data + left_->tupleLen(), right_rec->data, right_->tupleLen());
                
                // 检查连接条件
                if (eval_conds(cols_, fed_conds_, joined_rec)) {
                    result_batch_->push_back(std::move(joined_rec));
                }
            }
            
            // 移动到右表下一条记录
            right_idx_++;
            if (right_idx_ >= right_batch_->size()) {
                right_->nextTuple();
                fetch_right_batch();
            }
        }
        
        // 检查是否结束
        if (left_exhausted_ && (right_exhausted_ || result_batch_->empty())) {
            _is_end = true;
        }
    }

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    std::string getType() override { return "NestedLoopJoinExecutor"; }
};