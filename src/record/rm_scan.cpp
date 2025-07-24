/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"

#include "rm_file_handle.h"

/**
 * @brief 构造函数，初始化记录扫描器
 *
 * 该构造函数完成以下初始化工作：
 * 1. 保存要扫描的文件句柄
 * 2. 将rid初始化为第一个记录页的起始位置
 * 3. 通过调用next()移动到第一个有效记录
 *
 * @param file_handle 要扫描的表文件句柄
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // 扫描器初始化步骤：
    // 1. 保存要扫描的文件句柄指针
    // 2. 将rid初始化指向第一个记录页
    // 3. 调用next()移动到第一个有效记录位置
    rid_.page_no = RM_FIRST_RECORD_PAGE - 1;
    rid_.slot_no = RM_NO_PAGE;
    current_slot_idx_ = -1;
    next();
}

/**
 * @brief 移动到下一个有效记录的位置
 */
void RmScan::next() {
    // 查找下一条记录的步骤：
    // 1. 首先检查page_slots_缓存中是否还有记录
    // 2. 如果缓存中还有记录，则直接更新current_slot_idx_并更新rid_
    // 3. 如果缓存已用完，则扫描下一个包含有效记录的页面
    // 4. 将新页面中所有记录的槽位号填充到page_slots_中，并重置current_slot_idx_
    // 5. 如果所有页面都已扫描完毕，则将扫描器状态设为结束
    current_slot_idx_++;
    if (current_slot_idx_ < (int)page_slots_.size()) {
        // 缓存中还有记录
        rid_.slot_no = page_slots_[current_slot_idx_];
        return;
    } else {
        rid_.page_no++;  // 移动到下一页
    }
    // 缓存已用完，需要扫描下一页
    while (rid_.page_no < file_handle_->file_hdr_.num_pages) {
        auto page_guard = file_handle_->AcquirePageReadLock(rid_);
        RmPageHandle page_handle = RmPageHandle(&file_handle_->file_hdr_, const_cast<char *>(page_guard.GetData()));

        page_slots_.clear();
        page_slots_.reserve(file_handle_->file_hdr_.num_records_per_page);
        int slot = RM_NO_PAGE;
        while ((slot = Bitmap::next_bit(true, page_handle.bitmap, file_handle_->file_hdr_.num_records_per_page, slot)) <
               file_handle_->file_hdr_.num_records_per_page) {
            page_slots_.push_back(slot);
        }

        if (!page_slots_.empty()) {
            current_slot_idx_ = 0;
            rid_.slot_no = page_slots_[current_slot_idx_];
            return;
        }

        // 当前页面没有有效记录，转到下一页
        rid_.page_no++;
    }

    // 所有页面都已扫描完毕
    rid_.page_no = RM_NO_PAGE;
    rid_.slot_no = RM_NO_PAGE;
}

/**
 * @brief 判断扫描是否结束
 *
 * 通过检查当前rid_的page_no是否为RM_NO_PAGE来判断
 * 扫描是否已经遍历完所有有效记录
 *
 * @return true 如果已经扫描完所有记录
 * @return false 如果还有记录未扫描
 */
bool RmScan::is_end() const { return rid_.page_no == RM_NO_PAGE; }

/**
 * @brief 获取当前记录的标识符
 *
 * 返回扫描器当前指向的记录的RID，
 * 包含页面号和槽位号
 *
 * @return Rid 当前记录的标识符
 */
Rid RmScan::rid() const { return rid_; }
