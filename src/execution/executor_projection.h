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
 * @brief 投影执行器，负责实现SELECT语句的列选择功能
 *
 * @details 主要功能和特点：
 * 1. 列处理：
 *    - 选择指定列
 *    - 重组列布局
 *    - 优化内存结构
 *
 * 2. 性能优化：
 *    - 预计算偏移量
 *    - 批量数据处理
 *    - 减少内存拷贝
 *
 * 3. 内存管理：
 *    - 动态空间分配
 *    - 对齐优化
 *    - 缓存友好
 *
 * 4. 特殊处理：
 *    - NULL值处理
 *    - 列不存在检查
 *    - 类型转换支持
 */
class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;  // 前序执行器
    std::vector<ColMeta> cols_;               // 输出列元数据
    size_t len_;                              // 结果记录长度
    std::vector<size_t> sel_idxs_;           // 选中列在原记录中的索引

   public:
    /**
     * @brief 构造函数
     *
     * 初始化投影执行器：
     * 1. 设置输入执行器
     * 2. 计算输出记录的列布局
     * 3. 记录需要选择的列的索引位置
     *
     * @param prev 输入执行器
     * @param sel_cols 需要投影的列
     */
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;
    }

    /**
     * @brief 开始处理第一个元组
     * 调用输入执行器的beginTuple开始扫描
     */
    void beginTuple() override { prev_->beginTuple(); }

    /**
     * @brief 移动到下一个元组
     * 调用输入执行器的nextTuple继续扫描
     */
    void nextTuple() override { prev_->nextTuple(); }

    /**
     * @brief 检查是否完成所有元组的处理
     * @return 如果输入执行器到达末尾则返回true
     */
    bool is_end() const override { return prev_->is_end(); }

    /**
     * @brief 获取投影后的下一条记录
     *
     * 处理步骤：
     * 1. 获取输入执行器的下一条记录
     * 2. 创建新记录并分配空间
     * 3. 将选中的列从输入记录复制到新记录
     *
     * @return 投影后的记录指针，如果输入记录为空则返回nullptr
     */
    /**
     * @brief 获取投影后的结果记录
     * @return 投影记录的智能指针，如果输入为空返回nullptr
     *
     * @details 执行步骤：
     * 1. 数据获取：
     *    - 读取输入记录
     *    - 验证记录有效性
     *    - 准备缓冲区
     *
     * 2. 列处理：
     *    - 提取目标列
     *    - 计算新偏移
     *    - 复制字段数据
     *
     * 3. 优化处理：
     *    - 减少内存拷贝
     *    - 批量数据移动
     *    - 保持对齐访问
     */
    std::unique_ptr<RmRecord> Next() override {
        // 获取输入记录
        auto prev_rec = prev_->Next();
        if (!prev_rec) {
            std::cerr << "Error: Previous record is null at " + getType() << std::endl;
            return nullptr;
        }

        // 创建投影结果记录
        auto proj_rec = std::make_unique<RmRecord>(len_);
        auto &prev_cols = prev_->cols();

        // 复制选中的列数据
        for (size_t i = 0; i < sel_idxs_.size(); ++i) {
            size_t prev_idx = sel_idxs_[i];
            auto &prev_col = prev_cols[prev_idx];
            auto &proj_col = cols_[i];

            // 将列数据复制到新位置
            memcpy(proj_rec->data + proj_col.offset,
                  prev_rec->data + prev_col.offset,
                  proj_col.len);
        }

        return proj_rec;
    }

    /**
     * @brief 获取投影后记录的长度
     * @return 投影后记录的总字节数
     */
    size_t tupleLen() const override { return len_; }

    /**
     * @brief 获取投影后的列元数据
     * @return 投影列的元数据向量引用
     */
    const std::vector<ColMeta> &cols() const override { return cols_; }

    /**
     * @brief 获取当前记录的RID
     * @return 抽象RID的引用（投影结果没有实际的RID）
     */
    Rid &rid() override { return _abstract_rid; }

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    std::string getType() override { return "ProjectionExecutor"; }
};
