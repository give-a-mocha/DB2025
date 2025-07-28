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

void IxScan::next() {
    auto node = new IxNodeHandle(ih_->file_hdr_, now);
    assert(node->is_leaf_page());
    assert(iid_.slot_no < node->get_size());

    // increment slot no
    iid_.slot_no++;
    if (iid_.page_no != ih_->file_hdr_->last_leaf_ && iid_.slot_no == node->get_size()) {
        // go to next leaf
        iid_.slot_no = 0;
        iid_.page_no = node->get_next_leaf();
        Page* next_page = buffer_pool_manager.fetch_page({ih_->fd_, iid_.page_no});
        next_page->RLatch();
        unlatch();        // 释放当前页面
        now = next_page;  // 更新当前页面为下一个叶节点
    }
    delete node;  // 释放内存
}

/**
 * @brief 获取当前记录的标识符
 * @details 从RID缓存中返回当前索引对应的RID，支持溢出页
 * @return Rid 当前位置对应的记录标识符
 */
Rid IxScan::rid() const { return ih_->get_rid(iid_); }