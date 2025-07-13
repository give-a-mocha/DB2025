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

    rid_.page_no = RM_FIRST_RECORD_PAGE;  // 从第一个记录页开始
    rid_.slot_no = RM_NO_PAGE;            // 初始化为-1，next()会移动到第一个有效位置
    next();                               // 移动到第一个有效记录
}

/**
 * @brief 移动到下一个有效记录的位置
 */
void RmScan::next() {
    // 查找下一条记录的步骤：
    // 1. 遍历文件中的所有页面
    // 2. 在每个页面中使用位图查找已使用的槽位
    // 3. 找到后更新rid_指向该位置
    // 4. 如果当前页面搜索完毕则转到下一页
    // 5. 如果所有页面都搜索完则将page_no设为无效值

    while (rid_.page_no < file_handle_->file_hdr_.num_pages) {
        // 获取当前页面句柄
        auto page_guard = file_handle_->AcquirePageReadLock(rid_);

        RmPageHandle page_handle = RmPageHandle(&file_handle_->file_hdr_, const_cast<char*>(page_guard.GetData()));

        // 在当前页面寻找下一个非空slot
        rid_.slot_no =
            Bitmap::next_bit(true, page_handle.bitmap, file_handle_->file_hdr_.num_records_per_page, rid_.slot_no);
        if (rid_.slot_no < file_handle_->file_hdr_.num_records_per_page) {
            // 在当前页面找到了非空slot
            return;
        }

        // 当前页面搜索完毕,转到下一页
        rid_.page_no++;
        rid_.slot_no = RM_NO_PAGE;  // 重置slot_no为-1
    }
    rid_.page_no = RM_NO_PAGE;
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
