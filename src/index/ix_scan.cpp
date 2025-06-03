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
 * 实现逻辑：
 * 1. 在当前节点内移动
 * 2. 如果达到节点末尾且不是最后叶子节点：
 *    - 移动到下一个叶子节点的开始位置
 * 3. 释放旧节点的资源
 *
 * @todo
 * - 添加读锁保护页面访问
 * - 使用缓冲池管理页面生命周期
 *
 * @warning
 * - 必须正确处理节点边界情况
 * - 需要及时释放缓冲池资源
 */
void IxScan::next() {
    assert(!is_end());
    IxNodeHandle *node = ih_->fetch_node(iid_.page_no);
    assert(node->is_leaf_page());
    assert(iid_.slot_no < node->get_size());
    // increment slot no
    iid_.slot_no++;
    if (iid_.page_no != ih_->file_hdr_->last_leaf_ && iid_.slot_no == node->get_size()) {
        // go to next leaf
        iid_.slot_no = 0;
        iid_.page_no = node->get_next_leaf();
    }
    bpm_->unpin_page(node->get_page_id(), false);
    delete node;  // 释放内存
}

/**
 * @brief 获取当前记录的标识符
 * @details 将当前索引项ID转换为记录ID
 * @return Rid 当前位置对应的记录标识符
 * @note
 * - 通过索引句柄将iid转换为rid
 * - iid标识索引内的位置
 * - rid标识实际记录的位置
 */
Rid IxScan::rid() const { return ih_->get_rid(iid_); }