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

#include <cinttypes>
#include <cstring>

// 定义位图的基本单位为字节(8位)
static constexpr int BITMAP_WIDTH = 8;
// 定义最高位的掩码(10000000)，用于位操作
static constexpr unsigned BITMAP_HIGHEST_BIT = 0x80u;  // 128 (2^7)

class Bitmap {
   public:
    // 从地址bm开始的size个字节全部置0
    static void init(char *bm, int size) { memset(bm, 0, size); }

    // pos位 置1
    static void set(char *bm, int pos) { bm[get_bucket(pos)] |= get_bit(pos); }

    // pos位 置0
    static void reset(char *bm, int pos) { bm[get_bucket(pos)] &= static_cast<char>(~get_bit(pos)); }

    // 如果pos位是1，则返回true
    static bool is_set(const char *bm, int pos) { return (bm[get_bucket(pos)] & get_bit(pos)) != 0; }

    /**
     * @brief 找下一个为0 or 1的位（优化版本）
     * @param bit false表示要找下一个为0的位，true表示要找下一个为1的位
     * @param bm 要找的起始地址为bm
     * @param max_n 要找的从起始地址开始的偏移为[curr+1,max_n)
     * @param curr 要找的从起始地址开始的偏移为[curr+1,max_n)
     * @return 找到了就返回偏移位置，没找到就返回max_n
     */
    static int next_bit(bool bit, const char *bm, int max_n, int curr) {
        int start_pos = curr + 1;
        if (start_pos >= max_n) return max_n;

        int start_bucket = get_bucket(start_pos);
        int end_bucket = get_bucket(max_n - 1);

        // 目标字节模式：查找0用0x00，查找1用0xFF
        const char target_byte = bit ? static_cast<char>(0xFF) : static_cast<char>(0x00);

        // 处理起始字节的剩余位
        int start_offset = start_pos % BITMAP_WIDTH;
        if (start_offset != 0) {
            // 检查当前字节的剩余位
            int bits_to_check = std::min(BITMAP_WIDTH - start_offset, max_n - start_pos);
            for (int i = 0; i < bits_to_check; i++) {
                if (is_set(bm, start_pos + i) == bit) {
                    return start_pos + i;
                }
            }
            start_bucket++;
            start_pos = start_bucket * BITMAP_WIDTH;
        }

        // 快速跳过整字节
        while (start_bucket <= end_bucket && start_pos < max_n) {
            if (start_bucket == end_bucket) {
                // 最后一个字节，只检查有效位
                int remaining_bits = max_n - start_pos;
                for (int i = 0; i < remaining_bits; i++) {
                    if (is_set(bm, start_pos + i) == bit) {
                        return start_pos + i;
                    }
                }
                break;
            } else {
                // 整字节检查
                if (bm[start_bucket] == target_byte) {
                    // 整字节都是目标值，返回第一个位
                    return start_pos;
                } else if ((bit && bm[start_bucket] != 0x00) || (!bit && bm[start_bucket] != static_cast<char>(0xFF))) {
                    // 字节中有目标位，逐位查找
                    for (int i = 0; i < BITMAP_WIDTH; i++) {
                        if (is_set(bm, start_pos + i) == bit) {
                            return start_pos + i;
                        }
                    }
                }
                // 否则整个字节都不是目标值，跳过
                start_bucket++;
                start_pos += BITMAP_WIDTH;
            }
        }

        return max_n;
    }

    /**
     * @brief 批量设置连续的位为0
     * @param bm 位图起始地址
     * @param start_pos 起始位置
     * @param count 要设置的位数
     */
    static void batch_reset(char *bm, int start_pos, int count) {
        if (count <= 0) return;

        int end_pos = start_pos + count - 1;
        int start_bucket = get_bucket(start_pos);
        int end_bucket = get_bucket(end_pos);

        if (start_bucket == end_bucket) {
            // 所有位都在同一个字节内
            for (int i = 0; i < count; i++) {
                reset(bm, start_pos + i);
            }
        } else {
            // 跨多个字节

            // 1. 处理起始字节的不对齐部分
            int start_offset = start_pos % BITMAP_WIDTH;
            if (start_offset != 0) {
                int bits_in_first_byte = BITMAP_WIDTH - start_offset;
                for (int i = 0; i < bits_in_first_byte; i++) {
                    reset(bm, start_pos + i);
                }
                start_bucket++;
            }

            // 2. 批量处理完整字节（设置为0x00）
            for (int bucket = start_bucket; bucket < end_bucket; bucket++) {
                bm[bucket] = 0x00;  // 所有8位都设置为0
            }

            // 3. 处理结尾字节的不对齐部分
            int end_offset = end_pos % BITMAP_WIDTH;
            if (end_bucket < get_bucket(start_pos + count)) {
                for (int i = 0; i <= end_offset; i++) {
                    reset(bm, end_bucket * BITMAP_WIDTH + i);
                }
            }
        }
    }

    /**
     * @brief 高性能批量设置连续位为1（使用位操作优化）
     * @param bm 位图起始地址
     * @param start_pos 起始位置
     * @param count 要设置的位数
     */
    static void batch_set_fast(char *bm, int start_pos, int count) {
        if (count <= 0) return;

        int end_pos = start_pos + count - 1;
        int start_bucket = get_bucket(start_pos);
        int end_bucket = get_bucket(end_pos);

        if (start_bucket == end_bucket) {
            // 在同一字节内，使用位掩码
            char mask = create_mask(start_pos % BITMAP_WIDTH, count);
            bm[start_bucket] |= mask;
        } else {
            // 跨多个字节

            // 1. 处理起始字节
            int start_offset = start_pos % BITMAP_WIDTH;
            if (start_offset != 0) {
                int bits_in_first_byte = BITMAP_WIDTH - start_offset;
                char mask = create_mask(start_offset, bits_in_first_byte);
                bm[start_bucket] |= mask;
                start_bucket++;
            }

            // 2. 批量处理完整字节
            memset(bm + start_bucket, 0xFF, end_bucket - start_bucket);

            // 3. 处理结尾字节
            int end_offset = end_pos % BITMAP_WIDTH;
            if (end_bucket > start_bucket - 1) {
                char mask = create_mask(0, end_offset + 1);
                bm[end_bucket] |= mask;
            }
        }
    }

    /**
     * @brief 高性能批量设置连续位为0（使用位操作优化）
     * @param bm 位图起始地址
     * @param start_pos 起始位置
     * @param count 要设置的位数
     */
    static void batch_reset_fast(char *bm, int start_pos, int count) {
        if (count <= 0) return;

        int end_pos = start_pos + count - 1;
        int start_bucket = get_bucket(start_pos);
        int end_bucket = get_bucket(end_pos);

        if (start_bucket == end_bucket) {
            // 在同一字节内，使用位掩码
            char mask = create_mask(start_pos % BITMAP_WIDTH, count);
            bm[start_bucket] &= static_cast<char>(~mask);  // 使用反掩码清零
        } else {
            // 跨多个字节

            // 1. 处理起始字节
            int start_offset = start_pos % BITMAP_WIDTH;
            if (start_offset != 0) {
                int bits_in_first_byte = BITMAP_WIDTH - start_offset;
                char mask = create_mask(start_offset, bits_in_first_byte);
                bm[start_bucket] &= static_cast<char>(~mask);  // 使用反掩码清零
                start_bucket++;
            }

            // 2. 批量处理完整字节（设置为0x00）
            memset(bm + start_bucket, 0x00, end_bucket - start_bucket);

            // 3. 处理结尾字节
            int end_offset = end_pos % BITMAP_WIDTH;
            if (end_bucket > start_bucket - 1) {
                char mask = create_mask(0, end_offset + 1);
                bm[end_bucket] &= static_cast<char>(~mask);  // 使用反掩码清零
            }
        }
    }

    // 找第一个为0 or 1的位
    static int first_bit(bool bit, const char *bm, int max_n) { return next_bit(bit, bm, max_n, -1); }

    // for example:
    // rid_.slot_no = Bitmap::next_bit(true, page_handle.bitmap, file_handle_->file_hdr_.num_records_per_page,
    // rid_.slot_no); int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);

   private:
    /**
     * @brief 计算给定位置所在的字节偏移
     * @param pos 位的绝对位置
     * @return 该位所在的字节偏移
     */
    static int get_bucket(int pos) { return pos / BITMAP_WIDTH; }

    /**
     * @brief 计算给定位置的位掩码
     * @param pos 位的绝对位置
     * @return 对应的位掩码，用于位操作
     *
     * @note 实现原理:
     * 1. pos % BITMAP_WIDTH 得到字节内的偏移(0-7)
     * 2. BITMAP_HIGHEST_BIT 右移该偏移得到对应位的掩码
     */
    static char get_bit(int pos) { return BITMAP_HIGHEST_BIT >> static_cast<char>(pos % BITMAP_WIDTH); }

    /**
     * @brief 创建连续位的掩码
     * @param start_bit 字节内的起始位位置(0-7)
     * @param count 连续位数
     * @return 位掩码
     */
    static char create_mask(int start_bit, int count) {
        char mask = 0;
        for (int i = 0; i < count && (start_bit + i) < BITMAP_WIDTH; i++) {
            mask |= (BITMAP_HIGHEST_BIT >> (start_bit + i));
        }
        return mask;
    }
};
