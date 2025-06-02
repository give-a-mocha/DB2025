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

#include <assert.h>

#include <memory>

#include "bitmap.h"
#include "common/context.h"
#include "rm_defs.h"

class RmManager;

/**
 * @brief 表数据文件中单个页面的句柄类
 *
 * 对表数据文件中的页面进行封装，提供对页面内容的访问和管理。
 * 页面内部结构分为三个部分：
 * 1. 页面元信息（page_hdr）：存储页面的基本信息
 * 2. 位图（bitmap）：记录每个槽位是否被使用
 * 3. 数据槽位（slots）：存储实际的记录数据
 */
struct RmPageHandle {
    const RmFileHdr *file_hdr;  // 当前页面所在文件的文件头指针，包含表的元数据信息
    Page *page;                 // 底层页面对象，负责实际的数据存储
    RmPageHdr *page_hdr;        // 页面头部信息，指向页面数据的第一部分，存储页面级别的元数据
    char *bitmap;               // 页面的位图区域，用于跟踪槽位的使用情况
    char *slots;                // 实际记录存储区域，每个槽位存储一条记录

    RmPageHandle(const RmFileHdr *fhdr_, Page *page_) : file_hdr(fhdr_), page(page_) {
        page_hdr = reinterpret_cast<RmPageHdr *>(page->get_data() + page->OFFSET_PAGE_HDR);
        bitmap = page->get_data() + sizeof(RmPageHdr) + page->OFFSET_PAGE_HDR;
        slots = bitmap + file_hdr->bitmap_size;
    }

    // 返回指定slot_no的slot存储收地址
    char* get_slot(int slot_no) const {
        return slots + slot_no * file_hdr->record_size;  // slots的首地址 + slot个数 * 每个slot的大小(每个record的大小)
    }
};

/**
 * @brief 表数据文件的管理器类
 *
 * 负责管理单个表的数据文件，提供记录的增删改查操作。
 * 主要功能：
 * 1. 管理文件中的所有数据页面
 * 2. 提供记录级别的操作接口
 * 3. 维护文件的元数据信息
 * 4. 协调磁盘管理器和缓冲池管理器
 */
class RmFileHandle {
    friend class RmScan;    // 允许记录扫描器访问内部成员
    friend class RmManager; // 允许记录管理器访问内部成员

   private:
    DiskManager *disk_manager_;
    BufferPoolManager *buffer_pool_manager_;
    int fd_;        // 打开文件后产生的文件句柄
    RmFileHdr file_hdr_;    // 文件头，维护当前表文件的元数据

   public:
    RmFileHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
        : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
        // 注意：这里从磁盘中读出文件描述符为fd的文件的file_hdr，读到内存中
        // 这里实际就是初始化file_hdr，只不过是从磁盘中读出进行初始化
        // init file_hdr_
        disk_manager_->read_page(fd, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
        // disk_manager管理的fd对应的文件中，设置从file_hdr_.num_pages开始分配page_no
        disk_manager_->set_fd2pageno(fd, file_hdr_.num_pages);
    }

    RmFileHdr get_file_hdr() { return file_hdr_; }
    int GetFd() { return fd_; }

    /* 判断指定位置上是否已经存在一条记录，通过Bitmap来判断 */
    bool is_record(const Rid &rid) const {
        RmPageHandle page_handle = fetch_page_handle(rid.page_no);
        return Bitmap::is_set(page_handle.bitmap, rid.slot_no);  // page的slot_no位置上是否有record
    }

    /**
     * @brief 获取指定记录
     * @param rid 记录ID
     * @param context 事务上下文
     * @return 返回记录对象的智能指针
     */
    std::unique_ptr<RmRecord> get_record(const Rid &rid, Context *context) const;

    /**
     * @brief 插入新记录
     * @param buf 记录数据
     * @param context 事务上下文
     * @return 新记录的RID
     */
    Rid insert_record(char *buf, Context *context);

    /**
     * @brief 在指定位置插入记录
     * @param rid 指定的记录ID
     * @param buf 记录数据
     */
    void insert_record(const Rid &rid, char *buf);

    /**
     * @brief 删除记录
     * @param rid 要删除的记录ID
     * @param context 事务上下文
     */
    void delete_record(const Rid &rid, Context *context);

    /**
     * @brief 更新记录
     * @param rid 要更新的记录ID
     * @param buf 新的记录数据
     * @param context 事务上下文
     */
    void update_record(const Rid &rid, char *buf, Context *context);

    /**
     * @brief 创建新的页面句柄
     * @return 新页面的句柄
     */
    RmPageHandle create_new_page_handle();

    /**
     * @brief 获取指定页面的句柄
     * @param page_no 页面号
     * @return 页面句柄
     */
    RmPageHandle fetch_page_handle(int page_no) const;

   private:
    /**
     * @brief 创建页面句柄的内部方法
     * @return 新创建的页面句柄
     */
    RmPageHandle create_page_handle();

    /**
     * @brief 释放页面句柄
     * @param page_handle 要释放的页面句柄
     */
    void release_page_handle(RmPageHandle &page_handle);
};