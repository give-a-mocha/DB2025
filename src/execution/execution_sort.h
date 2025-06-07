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
#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 排序执行器，负责实现ORDER BY的排序功能
 *
 * @details 主要功能和特点：
 * 1. 排序功能:
 *    - 支持按指定列排序
 *    - 支持升序(ASC)和降序(DESC)
 *    - 处理多种数据类型(INT,FLOAT,STRING)
 *
 * 2. 实现策略:
 *    - 采用选择排序算法
 *    - 使用索引记录已处理的元组
 *    - 支持按需获取下一个元组
 *
 * 3. 资源管理:
 *    - 动态分配排序空间
 *    - 避免重复处理数据
 *    - 优化内存使用效率
 */
class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;  // 前序执行器
    ColMeta col_;                             // 排序列的元数据
    size_t tuple_num;                         // 已处理的元组数量
    bool is_desc_;                            // 是否为降序排序
    std::vector<size_t> used_tuple;           // 已选择元组的索引表
    std::unique_ptr<RmRecord> current_tuple;  // 当前处理的元组

   public:
    /**
     * @brief 构造函数
     * @param prev 前序执行器
     * @param sel_col 排序列的表列引用
     * @param is_desc 是否降序排序
     *
     * @details 初始化排序执行器:
     * 1. 设置前序执行器
     * 2. 获取排序列的元数据
     * 3. 初始化排序状态
     */
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const TabCol& sel_col, bool is_desc) {
        prev_ = std::move(prev);
        // 查找排序列的元数据
        auto pos = get_col(prev_->cols(), sel_col);
        col_ = *pos;
        is_desc_ = is_desc;
        tuple_num = 0;
        used_tuple.clear();
        current_tuple = nullptr;
    }

    /**
     * @brief 初始化排序过程并找到第一个元组
     *
     * @details 处理步骤:
     * 1. 扫描所有输入元组
     * 2. 找出第一个极值(最大或最小)
     * 3. 记录选中元组的位置
     *
     * 排序策略:
     * - 实现类似选择排序的过程
     * - 每次选择未处理元组中的最值
     * - 通过used_tuple记录已选择的位置
     */
    void beginTuple() override {
        prev_->beginTuple();
        int cnt = 0;
        int now = -1;
        current_tuple = nullptr;
        // 扫描找出第一个最值
        while (!prev_->is_end()) {
            if (cmp(prev_->Next(), current_tuple)) {
                current_tuple = prev_->Next();
                now = cnt;
            }
            prev_->nextTuple();
            cnt++;
        }
        tuple_num++;
        used_tuple.push_back(now);
    }

    /**
     * @brief 获取下一个排序后的元组
     *
     * @details 实现步骤:
     * 1. 重新扫描所有输入元组
     * 2. 排除已经选择过的元组
     * 3. 在剩余元组中找出极值
     * 4. 更新排序状态
     *
     * 注意事项:
     * - 每次都需要完整扫描剩余元组
     * - 通过used_tuple跳过已处理的元组
     * - 正确维护排序状态和计数
     */
    void nextTuple() override {
        prev_->beginTuple();
        int cnt = 0;
        int now = -1;
        current_tuple = nullptr;
        while (!prev_->is_end()) {
            // 跳过已经选择过的元组
            if (std::find(used_tuple.begin(), used_tuple.end(), cnt) == used_tuple.end() &&
                cmp(prev_->Next(), current_tuple)) {
                current_tuple = prev_->Next();
                now = cnt;
            }
            prev_->nextTuple();
            cnt++;
        }
        tuple_num++;
        used_tuple.push_back(now);
    }

    /**
     * @brief 返回当前排序位置的元组
     * @return 当前元组的智能指针
     */
    std::unique_ptr<RmRecord> Next() override { return std::move(current_tuple); }

    /**
     * @brief 获取输出列的元数据
     * @return 列元数据向量的常量引用
     */
    const std::vector<ColMeta>& cols() const override { return prev_->cols(); }

    /**
     * @brief 获取当前记录的RID
     * @return 抽象执行器的RID引用
     */
    Rid& rid() override { return _abstract_rid; }

    /**
     * @brief 比较两个元组在排序列上的大小
     * @param a 第一个元组
     * @param b 第二个元组
     * @return true表示a应该在b之前
     *
     * @details 比较策略:
     * 1. 处理空值情况
     * 2. 根据列类型选择比较方法:
     *    - 整数: 直接比较
     *    - 浮点数: 考虑精度的比较
     *    - 字符串: 使用strncmp比较
     * 3. 根据排序方向(升序/降序)返回结果
     */
    bool cmp(std::unique_ptr<RmRecord> a, std::unique_ptr<RmRecord>& b) {
        // 处理空值情况
        if (b == nullptr) {
            return true;
        }

        // 获取要比较的字段值
        char* rec_buf_a = a->data + col_.offset;
        char* rec_buf_b = b->data + col_.offset;

        // 根据字段类型进行比较
        if (col_.type == ColType::TYPE_INT) {
            int value_a = *reinterpret_cast<int*>(rec_buf_a);
            int value_b = *reinterpret_cast<int*>(rec_buf_b);
            if (is_desc_) return value_a > value_b;
            else return value_a < value_b;
        } else if (col_.type == ColType::TYPE_FLOAT) {
            float value_a = *reinterpret_cast<float*>(rec_buf_a);
            float value_b = *reinterpret_cast<float*>(rec_buf_b);
            if (is_desc_) return value_a > value_b;
            else return value_a < value_b;
        } else if (col_.type == ColType::TYPE_STRING) {
            int comparison_result = strncmp(rec_buf_a, rec_buf_b, static_cast<size_t>(col_.len));
            if (is_desc_) {
                return comparison_result > 0;
            } else {
                return comparison_result < 0;
            }
        }
        return false;
    }

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    std::string getType() override { return "SortExecutor"; }
};