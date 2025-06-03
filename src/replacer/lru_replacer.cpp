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
 *
 * @details 实现步骤：
 * 1. 获取互斥锁，保证并发安全
 * 2. 检查是否有可淘汰的页面
 * 3. 选择链表尾部(最久未使用)的页面
 * 4. 同时从链表和哈希表中删除该页面
 *
 * @note 优化考虑：
 * 1. 使用双向链表保证O(1)删除
 * 2. 哈希表维护O(1)查找
 * 3. 并发控制使用RAII锁
 *
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
 *
 * @details 实现步骤：
 * 1. 获取互斥锁
 * 2. 在哈希表中查找指定帧
 * 3. 如果帧存在(未固定)：
 *    - 从链表中删除该帧
 *    - 从哈希表中删除映射
 *
 * @note 特殊情况：
 * 1. 如果帧已经被固定(不在替换器中)，无需操作
 * 2. 重复pin操作是安全的
 *
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
 *
 * @details 实现步骤：
 * 1. 获取互斥锁
 * 2. 检查帧是否已在替换器中
 * 3. 如果不在：
 *    - 将帧添加到链表头部(最近使用)
 *    - 在哈希表中添加对应映射
 *
 * @note 设计考虑：
 * 1. 新解固定的页面被视为最近使用
 * 2. 避免重复添加同一帧
 * 3. 维护LRU的时间顺序特性
 *
 * @thread_safety 线程安全
 */
void LRUReplacer::unpin(frame_id_t frame_id) {
    // 支持并发锁
    std::scoped_lock lock{latch_};

    // 选择一个frame取消固定
    if (LRUhash_.find(frame_id) != LRUhash_.end()) return;
    LRUlist_.push_front(frame_id);                 // 加入链表头部(最近使用)
    LRUhash_.emplace(frame_id, LRUlist_.begin());  // 加入哈希表
}

/**
 * @brief 获取当前可被淘汰的页面数量
 *
 * @return size_t 可淘汰页面的数量
 *
 * @details 实现说明：
 * 1. 直接返回LRU链表的大小
 * 2. 链表中的页面都是未固定的
 * 3. 表示当前可以被替换的页面总数
 *
 * @note 使用场景：
 * 1. 评估缓冲池使用状态
 * 2. 判断是否需要强制淘汰
 * 3. 性能监控和调优
 *
 * @thread_safety 依赖STL容器的线程安全性
 */
size_t LRUReplacer::Size() { return LRUlist_.size(); }
