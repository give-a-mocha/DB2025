/* Copyright (c) 2023 Renmin University of China
 RMDB is licensed under Mulan PSL v2.
 You can use this software according to the terms and conditions of the Mulan PSL
 v2. You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
 THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 See the Mulan PSL v2 for more details. */

#pragma once

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include "replacer/replacer.h"

/**
 * @brief ShardedLRUReplacer 是一种高并发的 LRU 替换策略，它通过分片来减少锁争用。
 *
 * @design
 * 设计核心在于与上层调用者（如 BufferPoolInstance）的锁协同工作，形成内外两层锁机制：
 * 1. 外部锁（宏观锁）：由 BufferPoolInstance 持有，保证缓冲池在宏观上的一致性。
 * 2. 内部锁（微观锁）：即 ShardedLRUReplacer 内部的分片锁，以细粒度的方式保护自身数据结构。
 *
 * 这种内外两层锁的设计，使得 Replacer 内部的操作（例如在不同分片上并发 pin/unpin）
 * 可以高效执行，从而显著缩短外部锁的持有时间，最终提升整个系统的并发吞吐量。
 */
class ShardedLRUReplacer : public Replacer {
public:
    /**
     * @brief Create a new ShardedLRUReplacer.
     * @param num_shards the number of shards to use.
     */
    explicit ShardedLRUReplacer(size_t capacity, size_t num_shards = 8);

    ~ShardedLRUReplacer() override;

    /**
     * @brief Choose a victim frame to evict, and remove it from the replacer.
     * @param[out] frame_id id of the frame chosen as victim.
     * @return true if a victim frame is found, false otherwise.
     */
    bool victim(frame_id_t* frame_id) override;

    /**
     * @brief Pin a frame, indicating that it should not be evicted.
     * @param frame_id id of the frame to pin.
     */
    void pin(frame_id_t frame_id) override;

    /**
     * @brief Unpin a frame, indicating that it can be evicted.
     * @param frame_id id of the frame to unpin.
     */
    void unpin(frame_id_t frame_id) override;

    /**
     * @brief Get the number of frames in the replacer that can be evicted.
     * @return size_t the number of evictable frames.
     */
    size_t Size() override;

private:
    struct LRUShard {
        std::list<frame_id_t> lru_list_;
        std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> lru_map_;
        std::mutex latch_;
    };

    LRUShard& get_shard(frame_id_t frame_id) const;

    size_t capacity_;
    size_t num_shards_;
    std::vector<std::unique_ptr<LRUShard>> shards_;
    std::atomic<size_t> clock_hand_{0};
};