#pragma once
#include "replacer/replacer.h"

class ClockReplacer : public Replacer {
   private:
    size_t max_size_;   // 最大容量，与缓冲池容量相同
    size_t point_ = 0;  // 指向当前时钟指针的位置
    int8_t *use_bits_;  // 使用位数组，标记每个帧的使用状态
    bool *pinned_;      // 固定状态数组，标记每个帧是否被固定
   public:
    ClockReplacer(size_t num_pages);
    ~ClockReplacer();

    bool victim(frame_id_t *frame_id);

    void pin(frame_id_t frame_id);

    void unpin(frame_id_t frame_id);

    [[maybe_unused]] size_t Size();
};