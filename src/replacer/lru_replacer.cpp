/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lru_replacer.h"

#include "storage/page.h"

LRUReplacer::LRUReplacer() : pages_(nullptr) {
    std::fill(std::begin(is_pinned_), std::end(is_pinned_), true);
    std::fill(std::begin(in_dirty_), std::end(in_dirty_), false);
}

LRUReplacer::~LRUReplacer() = default;

void LRUReplacer::set_pages(Page *pages) { pages_ = pages; }

/**
 * @brief 根据 LRU-C 策略选择并移除一个受害帧（O(1)）
 *
 * 优先从 clean_list_ 尾部取干净页（无需写回磁盘）；
 * 若无干净页，则从 dirty_list_ 尾部取最久未使用的脏页。
 */
bool LRUReplacer::victim(frame_id_t *frame_id) {
    // 优先淘汰干净页
    if (!clean_list_.empty()) {
        *frame_id = clean_list_.back();
        is_pinned_[*frame_id] = true;
        clean_list_.pop_back();
        return true;
    }
    // 无干净页，退化为标准 LRU 淘汰脏页
    if (!dirty_list_.empty()) {
        *frame_id = dirty_list_.back();
        is_pinned_[*frame_id] = true;
        dirty_list_.pop_back();
        return true;
    }
    return false;
}

/**
 * @brief 固定指定帧，将其从所在链表中移除（O(1)）
 */
void LRUReplacer::pin(frame_id_t frame_id) {
    if (is_pinned_[frame_id]) {
        return;
    }
    is_pinned_[frame_id] = true;
    // 根据记录的位置，从对应链表中删除
    if (in_dirty_[frame_id]) {
        dirty_list_.erase(LRUhash_[frame_id]);
    } else {
        clean_list_.erase(LRUhash_[frame_id]);
    }
}

/**
 * @brief 解除固定，将帧插入对应链表头部（O(1)）
 *
 * 依据 pages_[frame_id].is_dirty() 决定放入 clean_list_ 还是 dirty_list_。
 * 调用前须确保脏页标记已更新（buffer_pool_instance 中先 mark_dirty 再 unpin）。
 */
void LRUReplacer::unpin(frame_id_t frame_id) {
    if (!is_pinned_[frame_id]) {
        return;
    }
    is_pinned_[frame_id] = false;

    // 查询当前脏页状态，决定放入哪条链表
    bool dirty = (pages_ != nullptr) && pages_[frame_id].is_dirty();
    in_dirty_[frame_id] = dirty;

    if (dirty) {
        dirty_list_.push_front(frame_id);
        LRUhash_[frame_id] = dirty_list_.begin();
    } else {
        clean_list_.push_front(frame_id);
        LRUhash_[frame_id] = clean_list_.begin();
    }
}

size_t LRUReplacer::Size() { return clean_list_.size() + dirty_list_.size(); }
