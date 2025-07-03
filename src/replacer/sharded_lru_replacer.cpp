#include "replacer/sharded_lru_replacer.h"

ShardedLRUReplacer::ShardedLRUReplacer(size_t capacity, size_t num_shards)
    : capacity_(capacity), num_shards_(num_shards) {
    shards_.reserve(num_shards_);
    for (size_t i = 0; i < num_shards_; ++i) {
        shards_.emplace_back(std::make_unique<LRUShard>());
    }
}

ShardedLRUReplacer::~ShardedLRUReplacer() = default;

bool ShardedLRUReplacer::victim(frame_id_t* frame_id) {
    size_t start_shard = clock_hand_.fetch_add(1) % num_shards_;
    for (size_t i = 0; i < num_shards_; ++i) {
        size_t shard_idx = (start_shard + i) % num_shards_;
        auto& shard = *shards_[shard_idx];
        std::scoped_lock lock(shard.latch_);

        if (!shard.lru_list_.empty()) {
            *frame_id = shard.lru_list_.back();
            shard.lru_map_.erase(*frame_id);
            shard.lru_list_.pop_back();
            return true;
        }
    }
    return false;
}

void ShardedLRUReplacer::pin(frame_id_t frame_id) {
    auto& shard = get_shard(frame_id);
    std::scoped_lock lock(shard.latch_);

    auto it = shard.lru_map_.find(frame_id);
    if (it != shard.lru_map_.end()) {
        shard.lru_list_.erase(it->second);
        shard.lru_map_.erase(it);
    }
}

void ShardedLRUReplacer::unpin(frame_id_t frame_id) {
    auto& shard = get_shard(frame_id);
    std::scoped_lock lock(shard.latch_);

    if (shard.lru_map_.find(frame_id) == shard.lru_map_.end()) {
        shard.lru_list_.emplace_front(frame_id);
        shard.lru_map_[frame_id] = shard.lru_list_.begin();
    }
}

size_t ShardedLRUReplacer::Size() {
    size_t total_size = 0;
    for (const auto& shard_ptr : shards_) {
        std::scoped_lock lock(shard_ptr->latch_);
        total_size += shard_ptr->lru_map_.size();
    }
    return total_size;
}

ShardedLRUReplacer::LRUShard& ShardedLRUReplacer::get_shard(frame_id_t frame_id) const {
    return *shards_[frame_id % num_shards_];
}