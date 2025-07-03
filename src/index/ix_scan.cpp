/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_scan.h"

/**
 * @brief 移动扫描器到下一个位置
 * @details 优化版本：如果当前键有多个RID（溢出页），先遍历缓存的RID，
 *          只有当缓存用完后才移动到下一个键值位置
 *
 * @warning
 * - 必须正确处理节点边界情况
 * - 需要及时释放缓冲池资源
 */
void IxScan::next() {
    assert(!is_end());

    if (has_overflow_cache_ && rid_index_ < current_rids_.size() - 1) {
        // 当前键还有未返回的RID，直接移动到下一个RID
        rid_index_++;
        return;
    }

    // 当前键的所有RID都已遍历，移动到下一个键值位置
    next_key_position();

    // 加载新位置的RID缓存
    if (!is_end()) {
        load_current_rids();
    }
}

/**
 * @brief 移动到下一个键值位置（跳过当前键的所有RID）
 * @note 内部使用，用于跳转到下一个不同的键值
 */
void IxScan::next_key_position() {
    auto node = new IxNodeHandle(ih_->file_hdr_, now);
    assert(node->is_leaf_page());
    assert(iid_.slot_no < node->get_size());

    // increment slot no
    iid_.slot_no++;
    if (iid_.page_no != ih_->file_hdr_->last_leaf_ && iid_.slot_no == node->get_size()) {
        // go to next leaf
        iid_.slot_no = 0;
        iid_.page_no = node->get_next_leaf();
        Page* next_page = bpm_->fetch_page({ih_->fd_, iid_.page_no});
        next_page->rlatch();
        unlatch();        // 释放当前页面
        now = next_page;  // 更新当前页面为下一个叶节点
    }
    delete node;  // 释放内存
}

/**
 * @brief 加载当前位置对应的所有RID（处理溢出页）
 * @note 如果当前键值有溢出页，会一次性加载所有RID到缓存中
 */
void IxScan::load_current_rids() {
    current_rids_.clear();
    rid_index_ = 0;
    has_overflow_cache_ = false;

    if (is_end()) {
        return;
    }

    auto node = new IxNodeHandle(ih_->file_hdr_, now);
    auto rid = node->get_rid(iid_.slot_no);

    if (rid->slot_no == IX_NO_SLOT && rid->page_no != IX_NO_PAGE) {
        // 这是一个溢出页引用，获取所有RID
        ih_->get_all_rids_from_overflow_page(rid->page_no, &current_rids_);
        has_overflow_cache_ = true;
    } else {
        // 普通RID，直接添加到缓存
        current_rids_.push_back(*rid);
        has_overflow_cache_ = true;
    }

    delete node;
}

/**
 * @brief 获取当前记录的标识符
 * @details 从RID缓存中返回当前索引对应的RID，支持溢出页
 * @return Rid 当前位置对应的记录标识符
 */
Rid IxScan::rid() const {
    if (has_overflow_cache_ && rid_index_ < current_rids_.size()) {
        return current_rids_[rid_index_];
    }
    // 如果没有缓存，使用原来的方法（向后兼容）
    return ih_->get_rid(iid_);
}