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
 * @details 在叶子节点中顺序移动，必要时跨越节点边界
 *
 * @warning
 * - 必须正确处理节点边界情况
 * - 需要及时释放缓冲池资源
 */
void IxScan::next() {
    assert(!is_end());
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
        now->runlatch();
        bpm_->unpin_page(node->get_page_id(), false);
        now = next_page;  // 更新当前页面为下一个叶节点
    }
    
    delete node;  // 释放内存
}

/**
 * @brief 获取当前记录的标识符
 * @details 将当前索引项ID转换为记录ID
 * @return Rid 当前位置对应的记录标识符
 */
Rid IxScan::rid() const { return ih_->get_rid(iid_); }