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
#include "unordered_map"

/**
 * @brief LRU(最近最少使用)页面替换策略的实现
 *
 * @details 实现原理：
 * 1. 使用双向链表(LRUlist_)记录页面访问顺序
 *    - 最近使用的页面在链表头部
 *    - 最久未使用的页面在链表尾部
 *
 * 2. 使用哈希表(LRUhash_)实现O(1)的页面查找
 *    - 存储frame_id到链表节点的映射
 *    - 支持快速的页面状态更新
 *
 * @note 特性：
 * 1. 线程安全：使用互斥锁保护并发访问
 * 2. 空间效率：额外空间复杂度O(n)
 * 3. 时间效率：所有操作时间复杂度O(1)
 */
class LRUReplacer : public Replacer {
   public:
    /**
     * @description: 创建一个新的LRUReplacer
     * @param {size_t} num_pages LRUReplacer最多需要存储的page数量
     */
    explicit LRUReplacer(size_t num_pages);

    ~LRUReplacer();

    bool victim(frame_id_t *frame_id);

    void pin(frame_id_t frame_id);

    void unpin(frame_id_t frame_id);

    size_t Size();

   private:
    std::mutex latch_;  // 互斥锁，保护并发访问
    
    /**
     * @brief 存储未固定页面的双向链表
     *
     * 维护页面的访问顺序：
     * - 链表头：最近访问的页面
     * - 链表尾：最久未访问的页面(将被淘汰)
     */
    std::list<frame_id_t> LRUlist_;

    /**
     * @brief frame_id到链表节点的映射
     *
     * 用途：
     * 1. O(1)时间定位页面在链表中的位置
     * 2. 支持快速的页面状态更新
     * 3. 避免在链表中的线性查找
     */
    std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> LRUhash_;
    
    size_t max_size_;  // 最大容量，与缓冲池容量相同
};
