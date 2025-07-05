#include "replacer/clock_replacer.h"
#include <cstring>

ClockReplacer::ClockReplacer(size_t num_pages) : max_size_(num_pages), point_(0) {
    use_bits_ = new int8_t[num_pages];
    pinned_ = new bool[num_pages];
    memset(use_bits_, 0, num_pages * sizeof(int8_t));
    memset(pinned_, 0, num_pages * sizeof(bool));
}

ClockReplacer::~ClockReplacer() { delete[] use_bits_; }

bool ClockReplacer::victim(frame_id_t *frame_id) {
    int step = 0;
    do {
        point_ = (point_ + 1) % max_size_;
        if (use_bits_[point_] == 0 && !pinned_[point_]) {
            *frame_id = point_;
            return true;
        }
        if (use_bits_[point_] == 0) {
            pinned_[point_] = false;
        }
    } while (++step < max_size_ * 2);
    return false;
}

void ClockReplacer::pin(frame_id_t frame_id) {
    if (++use_bits_[frame_id] > 1) {
        use_bits_[frame_id] = 1;
    }
}

void ClockReplacer::unpin(frame_id_t frame_id) {
    if (use_bits_[frame_id] > 0) {
        --use_bits_[frame_id];
    }
}

size_t ClockReplacer::Size() { return max_size_; }
