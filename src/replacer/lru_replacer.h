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

#include <list>
#include <mutex>
#include <vector>

#include "common/config.h"
#include "replacer/replacer.h"

// 前向声明，避免与 storage/page.h 产生循环依赖
class Page;

/**
 * @brief LRU-C（优先淘汰干净页）替换策略
 *
 * 维护两条独立的 LRU 链表：
 *   - clean_list_：干净页（无需写回磁盘）
 *   - dirty_list_：脏页（淘汰时需写回磁盘）
 *
 * victim 时优先从 clean_list_ 尾部取，无干净页再从 dirty_list_ 尾部取，
 * 全程 O(1)，无需扫描。
 *
 * 注意：必须在首次 unpin() 前调用 set_pages()，否则所有帧均视为干净页。
 */
class LRUReplacer : public Replacer {
   public:
    explicit LRUReplacer();

    ~LRUReplacer();

    /**
     * @brief 绑定缓冲池的页面数组，供 unpin 时查询脏页状态
     * @param pages 缓冲池 pages_ 数组指针
     */
    void set_pages(Page *pages);

    bool victim(frame_id_t *frame_id);

    void pin(frame_id_t frame_id);

    void unpin(frame_id_t frame_id);

    size_t Size();

   private:
    Page *pages_;  // 缓冲池页面数组，由 set_pages 注入

    /**
     * @brief 干净页 LRU 链表（头=最近使用，尾=最久未使用）
     * victim 优先从此链表尾部取，避免触发磁盘写回
     */
    std::list<frame_id_t> clean_list_;

    /**
     * @brief 脏页 LRU 链表（头=最近使用，尾=最久未使用）
     * 仅在 clean_list_ 为空时才从此链表尾部取
     */
    std::list<frame_id_t> dirty_list_;

    static constexpr size_t max_size_ = BUFFER_POOL_SIZE;

    /**
     * @brief frame_id 到所在链表节点的映射（O(1) 定位，供 pin 时删除）
     * clean_list_ 和 dirty_list_ 的迭代器类型相同，共用此数组
     */
    std::list<frame_id_t>::iterator LRUhash_[max_size_];

    bool is_pinned_[max_size_];   // true = 已固定，不在任何链表中
    bool in_dirty_[max_size_];    // true = 位于 dirty_list_（仅 !is_pinned_ 时有意义）
};
