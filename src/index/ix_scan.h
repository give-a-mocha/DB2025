/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "ix_defs.h"
#include "ix_index_handle.h"

/**
 * @brief B+树索引扫描器类
 * @details 用于遍历B+树的叶子节点链表
 *
 * 设计特点：
 * 1. 直接遍历叶子节点链表，避免重复查找
 * 2. 支持范围扫描操作
 * 3. 提供顺序访问接口
 *
 * 关键功能：
 * - 使用双指针维护扫描范围
 * - 通过叶子节点链表高效遍历
 * - 支持范围查询的边界控制
 *
 * @note TODO：
 * - 需要为页面遍历添加读锁
 * - 完善并发控制机制
 */
class IxScan : public RecScan {
    const IxIndexHandle *ih_;  // 索引句柄指针
    Iid iid_;                  // 当前扫描位置(初始为lower)
    Iid end_;                  // 扫描终止位置(初始为upper)
    BufferPoolManager *bpm_;   // 缓冲池管理器
    Page *now;

   public:
    /**
     * @brief 构造索引扫描器
     * @param ih 索引句柄
     * @param lower 扫描起始位置
     * @param upper 扫描终止位置
     * @param bpm 缓冲池管理器
     */
    IxScan(const IxIndexHandle *ih, const Iid &lower, const Iid &upper, BufferPoolManager *bpm)
        : ih_(ih), iid_(lower), end_(upper), bpm_(bpm) {
        if (!is_end()) {
            now = bpm_->fetch_page({ih_->fd_, iid_.page_no});
            now->rlatch();  // 获取读锁，确保扫描期间页面不被修改
        }
    }

    /**
     * @brief 移动到下一条记录
     * @note
     * - 在叶子节点内移动或跨节点移动
     * - 需要正确处理跨节点边界情况
     */
    void next() override;

    /**
     * @brief 检查是否到达扫描终点
     * @return bool 是否扫描完成
     * @note 通过比较当前位置和终止位置判断
     */
    bool is_end() const override { return iid_ == end_; }

    /**
     * @brief 获取当前记录的RID
     * @return Rid 当前记录的标识符
     * @note 将索引项的位置转换为记录位置
     */
    Rid rid() const override;

    /**
     * @brief 获取当前索引项标识符
     * @return Iid 当前索引项的标识符
     * @note 用于外部获取扫描器位置信息
     */
    const Iid &iid() const { return iid_; }

    void unlatch() {
        if (now != nullptr) {
            now->runlatch();                              // 释放读锁
            bpm_->unpin_page(now->get_page_id(), false);  // 解除页面固定状态
            now = nullptr;                                // 清空当前页面指针
        }
    }
};