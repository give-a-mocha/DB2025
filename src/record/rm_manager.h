/**
 * @file rm_manager.h
 * @author RMDB Development Team
 * @brief 记录管理器头文件
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * 记录管理器提供表记录的物理存储管理功能，主要包括：
 * 1. 文件管理
 *    - 创建/删除表的数据文件
 *    - 打开/关闭表的数据文件
 * 2. 记录组织
 *    - 支持变长记录的存储
 *    - 利用位图管理空闲空间
 *    - 自动分配和回收页面
 * 3. 页面缓冲
 *    - 与缓冲池管理器协作
 *    - 支持页面的固定和解固定
 * 4. 文件格式
 *    - 文件头页面：记录表的元数据
 *    - 数据页面：存储实际记录
 *    - 每页包含页头和位图
 */

#pragma once

#include <assert.h>

#include "bitmap.h"
#include "rm_defs.h"
#include "rm_file_handle.h"

extern DiskManager disk_manager;
extern BufferPoolManager buffer_pool_manager;

/**
 * @brief 记录管理器类
 */
class RmManager {
   public:
    RmManager() = default;

    /**
     * @brief 创建表的数据文件并初始化文件头信息
     *
     * @param filename 要创建的文件名称
     * @param record_size 表中记录的大小(字节数)
     * @throw InvalidRecordSizeError 如果记录大小超出限制
     */
    void create_file(const std::string &filename, int record_size) {
        if (record_size < 1 || record_size > RM_MAX_RECORD_SIZE) {
            throw InvalidRecordSizeError(record_size);
        }
        disk_manager.create_file(filename);
        int fd = disk_manager.open_file(filename);

        // 初始化file header
        RmFileHdr file_hdr{};
        file_hdr.record_size = record_size;
        file_hdr.num_pages = 1;
        file_hdr.record_num = 0;
        file_hdr.first_free_page_no = RM_NO_PAGE;
        // [RmPageHdr] [Bitmap] [Record1] [Record2] ... [RecordN]
        // sizeof(lsn_t) + sizeof(RmPageHdr) + bitmap_size + (记录数 × record_size) ≤ PAGE_SIZE
        // sizeof(lsn_t) + sizeof(hdr) + (n + BITMAP_WIDTH - 1) / BITMAP_WIDTH + n * record_size <= PAGE_SIZE
        file_hdr.num_records_per_page =
            (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmPageHdr) - (int)sizeof(lsn_t)) + 1) /
            (1 + (record_size + TUPLE_META_SIZE) * BITMAP_WIDTH);
        file_hdr.bitmap_size = (file_hdr.num_records_per_page + BITMAP_WIDTH - 1) / BITMAP_WIDTH;

        // 将file header写入磁盘文件（名为file name，文件描述符为fd）中的第0页
        // head page直接写入磁盘，没有经过缓冲区的NewPage，那么也就不需要FlushPage
        disk_manager.write_page(fd, RM_FILE_HDR_PAGE, (char *)&file_hdr, sizeof(file_hdr));
        disk_manager.close_file(fd);
    }

    /**
     * @brief 删除表的数据文件
     *
     * @param filename 要删除的文件名称
     * @note 该操作将永久删除文件，请谨慎使用
     * @warning 删除前应确保文件已关闭，否则可能导致资源泄漏
     */
    void destroy_file(const std::string &filename) { disk_manager.destroy_file(filename); }

    /**
     * @brief 打开表的数据文件，并创建文件句柄
     *
     * @param filename 要打开的文件名称
     * @return unique_ptr<RmFileHandle> 文件句柄的智能指针
     */
    std::unique_ptr<RmFileHandle> open_file(const std::string &filename) {
        int fd = disk_manager.open_file(filename);
        return std::make_unique<RmFileHandle>(fd);
    }
    /**
     * @brief 关闭表的数据文件
     *
     * @param file_handle 要关闭文件的句柄
     */
    void close_file(const RmFileHandle *file_handle) {
        disk_manager.write_page(file_handle->fd_, RM_FILE_HDR_PAGE, (char *)&file_handle->file_hdr_,
                                sizeof(file_handle->file_hdr_));
        // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
        buffer_pool_manager.flush_all_pages(file_handle->fd_);
        disk_manager.close_file(file_handle->fd_);
    }

    /**
     * @brief 关闭数据文件并清理缓冲池
     *
     * @param file_handle 要关闭文件的句柄
     */
    void close_file_and_clear_buffer(const RmFileHandle *file_handle) {
        disk_manager.write_page(file_handle->fd_, RM_FILE_HDR_PAGE, (char *)&file_handle->file_hdr_,
                                sizeof(file_handle->file_hdr_));
        // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
        buffer_pool_manager.flush_all_pages(file_handle->fd_);
        // 删除缓冲池中该文件的所有页面
        buffer_pool_manager.delete_all_pages(file_handle->fd_);
        disk_manager.close_file(file_handle->fd_);
    }
};
