/* Copyright (c) 2023 Renmin University of China
 * RMDB is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details. */

#pragma once

#include <vector>
#include "ix_defs.h"
#include "ix_index_handle.h"
#include "ix.h"

/**
 * @brief B+树索引扫描器类 (优化的版本)
 * @details 构造时预加载所有匹配的RID到内存中，提高扫描性能
 */
class IxScanFinal : public RecScan {
    const IxIndexHandle *ih_;  // 索引句柄指针
    BufferPoolManager *bpm_;   // 缓冲池管理器

   private:
    std::vector<Rid> rids_;  // 用于缓存扫描结果的RID
    size_t cursor_{0};       // 当前扫描在rids_中的位置

   public:
    /**
     * @brief 构造索引扫描器
     * @param ih 索引句柄
     * @param lower 扫描起始位置
     * @param upper 扫描终止位置
     * @param bpm 缓冲池管理器
     */
    IxScanFinal(const IxIndexHandle *ih, const Iid &lower, const Iid &upper, BufferPoolManager *bpm);

    /**
     * @brief 析构函数，确保正确释放页面资源
     */
    ~IxScanFinal() = default;

    /**
     * @brief 移动到下一条记录
     */
    void next() override { cursor_++; }

    /**
     * @brief 检查是否到达扫描终点
     * @return bool 是否扫描完成
     */
    bool is_end() const override { return cursor_ == rids_.size(); }

    /**
     * @brief 获取当前记录的RID
     * @return Rid 当前记录的标识符
     */
    Rid rid() const override { return rids_[cursor_]; }
};