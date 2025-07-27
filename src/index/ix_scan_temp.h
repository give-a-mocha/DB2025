/* Copyright (c) 2023 Renmin University of China
| RMDB is licensed under Mulan PSL v2.
| You can use this software according to the terms and conditions of the Mulan PSL v2.
| You may obtain a copy of Mulan PSL v2 at:
|         http://license.coscl.org.cn/MulanPSL2
| THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
| EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
| MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
| See the Mulan PSL v2 for more details. */

#pragma once

#include "ix_defs.h"
#include "ix_index_handle.h"
#include "record/rm_scan.h"
#include <vector>

/**
 * @brief B+树索引扫描器类 (预取版本)
 * @details 在构造时预取所有符合条件的Rid，以提高扫描性能。
 *
 * 设计特点：
 * 1. 构造时一次性加载所有Rid到内存中。
 * 2. next()操作仅移动游标，无磁盘I/O和锁操作。
 * 3. 适用于读多写少的场景。
 */
class IxScanTemp : public RecScan {
    std::vector<Rid> rids_;
    int curr_rid_idx_ = 0;

   public:
    /**
     * @brief 构造索引扫描器
     * @param ih 索引句柄
     * @param lower 扫描起始位置
     * @param upper 扫描终止位置
     * @param bpm 缓冲池管理器
     */
    IxScanTemp(IxIndexHandle *ih, const Iid &lower, const Iid &upper, BufferPoolManager *bpm);

    /**
     * @brief 析构函数
     */
    ~IxScanTemp() override = default;

    /**
     * @brief 移动到下一条记录
     */
    void next() override { curr_rid_idx_++; }

    /**
     * @brief 检查是否到达扫描终点
     * @return bool 是否扫描完成
     */
    bool is_end() const override { return curr_rid_idx_ >= (int)rids_.size(); }

    /**
     * @brief 获取当前记录的RID
     * @return Rid 当前记录的标识符
     */
    Rid rid() const override {
        if (is_end()) {
            return Rid{INVALID_PAGE_ID, -1};
        }
        return rids_[curr_rid_idx_];
    }
};