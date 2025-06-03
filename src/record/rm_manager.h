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

/**
 * @brief 记录管理器类
 *
 * 记录管理器负责管理表的数据文件，维护数据在磁盘上的组织方式，主要功能：
 * 1. 文件操作
 *    - 创建新的数据文件并初始化文件头
 *    - 删除已存在的数据文件
 *    - 打开和关闭数据文件
 * 2. 空间管理
 *    - 使用位图管理记录的空闲空间
 *    - 计算每页可存储的记录数
 * 3. 页面管理
 *    - 维护空闲页面链表
 *    - 自动扩展文件大小
 * 4. 缓冲管理
 *    - 协调文件数据在内存和磁盘间的交换
 *    - 确保数据的持久化
 */
class RmManager {
   private:
    DiskManager *disk_manager_;           // 磁盘管理器，负责文件操作
    BufferPoolManager *buffer_pool_manager_;  // 缓冲池管理器，负责页面缓存

   public:
    /**
     * @brief 构造函数
     * @param disk_manager 磁盘管理器
     * @param buffer_pool_manager 缓冲池管理器
     * @note 初始化记录管理器，建立与磁盘和缓冲池管理器的关联
     */
    RmManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager)
        : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager) {}

    /**
     * @brief 创建表的数据文件并初始化文件头信息
     *
     * @param filename 要创建的文件名称
     * @param record_size 表中记录的大小(字节数)
     * @throw InvalidRecordSizeError 如果记录大小超出限制
     *
     * @note 文件头页面(page_no = 0)保存元数据：
     * - record_size: 记录大小
     * - num_pages: 文件中的页面数
     * - first_free_page_no: 第一个空闲页面号
     * - num_records_per_page: 每页可存储的记录数
     * - bitmap_size: 每页位图的大小(字节数)
     */
    void create_file(const std::string &filename, int record_size) {
        if (record_size < 1 || record_size > RM_MAX_RECORD_SIZE) {
            throw InvalidRecordSizeError(record_size);
        }
        disk_manager_->create_file(filename);
        int fd = disk_manager_->open_file(filename);

        // 初始化file header
        RmFileHdr file_hdr{};
        file_hdr.record_size = record_size;
        file_hdr.num_pages = 1;
        file_hdr.record_num = 0;
        file_hdr.first_free_page_no = RM_NO_PAGE;
        // We have: sizeof(hdr) + (n + 7) / 8 + n * record_size <= PAGE_SIZE
        file_hdr.num_records_per_page =
            (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmPageHdr)) + 1) / (1 + record_size * BITMAP_WIDTH);
        file_hdr.bitmap_size = (file_hdr.num_records_per_page + BITMAP_WIDTH - 1) / BITMAP_WIDTH;

        // 将file header写入磁盘文件（名为file name，文件描述符为fd）中的第0页
        // head page直接写入磁盘，没有经过缓冲区的NewPage，那么也就不需要FlushPage
        disk_manager_->write_page(fd, RM_FILE_HDR_PAGE, (char *)&file_hdr, sizeof(file_hdr));
        disk_manager_->close_file(fd);
    }

    /**
     * @brief 删除表的数据文件
     *
     * @param filename 要删除的文件名称
     * @note 该操作将永久删除文件，请谨慎使用
     * @warning 删除前应确保文件已关闭，否则可能导致资源泄漏
     */
    void destroy_file(const std::string &filename) { disk_manager_->destroy_file(filename); }

    /**
     * @brief 打开表的数据文件，并创建文件句柄
     *
     * @param filename 要打开的文件名称
     * @return unique_ptr<RmFileHandle> 文件句柄的智能指针
     * @note 文件句柄封装了对文件的所有操作：
     * 1. 记录的插入、删除和更新
     * 2. 页面的分配和回收
     * 3. 文件头信息的维护
     */
    std::unique_ptr<RmFileHandle> open_file(const std::string &filename) {
        int fd = disk_manager_->open_file(filename);
        return std::make_unique<RmFileHandle>(disk_manager_, buffer_pool_manager_, fd);
    }
    /**
     * @brief 关闭表的数据文件
     *
     * @param file_handle 要关闭文件的句柄
     * @note 关闭文件时的操作顺序：
     * 1. 将文件头信息写回磁盘
     * 2. 将所有脏页刷新到磁盘
     * 3. 关闭文件描述符
     */
    void close_file(const RmFileHandle *file_handle) {
        disk_manager_->write_page(file_handle->fd_, RM_FILE_HDR_PAGE, (char *)&file_handle->file_hdr_,
                                  sizeof(file_handle->file_hdr_));
        // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
        buffer_pool_manager_->flush_all_pages(file_handle->fd_);
        disk_manager_->close_file(file_handle->fd_);
    }

    /**
     * @brief 关闭数据文件并清理缓冲池
     *
     * @param file_handle 要关闭文件的句柄
     * @note 相比普通关闭，该方法还会：
     * 1. 删除缓冲池中该文件的所有页面
     * 2. 释放相关的内存资源
     * 3. 强制持久化所有修改
     * @warning 该操作会导致缓冲池中的页面失效，可能影响性能
     */
    void close_file_and_clear_buffer(const RmFileHandle *file_handle) {
        disk_manager_->write_page(file_handle->fd_, RM_FILE_HDR_PAGE, (char *)&file_handle->file_hdr_,
                                  sizeof(file_handle->file_hdr_));
        // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
        buffer_pool_manager_->flush_all_pages(file_handle->fd_);
        // 删除缓冲池中该文件的所有页面
        buffer_pool_manager_->delete_all_pages(file_handle->fd_);
        disk_manager_->close_file(file_handle->fd_);
    }
};
