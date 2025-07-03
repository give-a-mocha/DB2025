/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

/**
 * @brief 根据LRU策略选择并移除一个受害帧
 *
 * @param frame_id 被选中移除的帧ID的指针
 * @return true 成功找到并移除了一个帧
 * @return false 没有可移除的帧
 * @thread_safety 线程安全
 */
bool LRUReplacer::victim(frame_id_t* frame_id) {
    // Todo:
    // !利用lru_replacer中的LRUlist_,LRUHash_实现LRU策略
    // !选择合适的frame指定为淘汰页面,赋值给*frame_id

    std::scoped_lock lock{latch_};
    if (LRUlist_.empty()) {
        return false;
    }

    *frame_id = LRUlist_.back();  // 选择最久未使用的页面(链表尾部)
    LRUhash_.erase(*frame_id);    // 从哈希表中删除
    LRUlist_.pop_back();          // 从链表中删除

    return true;
}

/**
 * @brief 固定指定的帧，防止其被淘汰
 *
 * @param frame_id 要固定的帧ID
 * @thread_safety 线程安全
 */
void LRUReplacer::pin(frame_id_t frame_id) {
    // Todo:
    // !固定指定id的frame
    // !在数据结构中移除该frame

    std::scoped_lock lock{latch_};
    auto iter = LRUhash_.find(frame_id);
    if (iter != LRUhash_.end()) {
        LRUlist_.erase(iter->second);  // 从链表中删除
        LRUhash_.erase(iter);          // 从哈希表中删除
    }
}

/**
 * @brief 取消固定帧，使其可以被淘汰
 *
 * @param frame_id 要取消固定的帧ID
 * @thread_safety 线程安全
 */
void LRUReplacer::unpin(frame_id_t frame_id) {
    // 支持并发锁
    std::scoped_lock lock{latch_};

    // 满了
    if (LRUlist_.size() >= max_size_) {
        return;
    }

    // 选择一个frame取消固定
    if (LRUhash_.find(frame_id) != LRUhash_.end()) return;
    LRUlist_.push_front(frame_id);                 // 加入链表头部(最近使用)
    LRUhash_.emplace(frame_id, LRUlist_.begin());  // 加入哈希表
}

/**
 * @brief 获取当前可被淘汰的页面数量
 *
 * @return size_t 可淘汰页面的数量
 * @thread_safety 依赖STL容器的线程安全性
 */
size_t LRUReplacer::Size() { return LRUlist_.size(); }
