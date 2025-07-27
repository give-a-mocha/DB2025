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
#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <list>
#include <unordered_map>
#include <vector>

#include "disk_manager.h"
#include "errors.h"
#include "page.h"
#include "buffer_pool_instance.h"
#include "page_guard.h"


extern DiskManager disk_manager;

/**
 * @brief 缓冲池管理器类
 */
class BufferPoolManager {
   private:
    size_t pool_size_;                                                      // 缓冲池大小（帧数）
    BufferPoolInstance* buffer_pool_instances_[BUFFER_POOL_INSTANCE_SIZE];  // 缓冲池实例数组
    std::hash<PageId> hasher_;  // 用于计算PageId的哈希值

   public:
    BufferPoolManager(size_t pool_size)
        : pool_size_(pool_size) {
        // 为buffer pool分配一块连续的内存空间
        for (size_t i = 0; i < BUFFER_POOL_INSTANCE_SIZE; ++i) {
            buffer_pool_instances_[i] = new BufferPoolInstance(pool_size / BUFFER_POOL_INSTANCE_SIZE);
        }
    }

    ~BufferPoolManager() {
        for (size_t i = 0; i < BUFFER_POOL_INSTANCE_SIZE; ++i) {
            delete buffer_pool_instances_[i];
        }
    }

    /**
     * @description: 将目标页面标记为脏页
     * @param {Page*} page 脏页
     */
    // static void mark_dirty(Page* page) { page->is_dirty_ = true; }

   public:
    /**
     * @brief 获取指定页面
     *
     * 如果页面在缓冲池中，直接返回；
     * 否则从磁盘读取页面到缓冲池，可能触发页面替换
     *
     * @param page_id 页面标识符
     * @return Page* 页面指针
     */
    Page* fetch_page(PageId page_id);

    /**
     * @brief 解除页面的固定状态
     *
     * 减少页面的引用计数，如果设置了is_dirty，
     * 标记页面为脏页
     *
     * @param page_id 页面标识符
     * @param is_dirty 是否标记为脏页
     * @return true 成功解除固定
     * @return false 页面不存在或已经解除固定
     */
    bool unpin_page(PageId page_id, bool is_dirty);

    /**
     * @brief 将指定页面刷新到磁盘
     *
     * @param page_id 页面标识符
     * @return true 刷新成功
     * @return false 页面不存在
     */
    bool flush_page(PageId page_id);

    /**
     * @brief 创建新页面
     *
     * 在缓冲池中分配一个新的页面，可能触发页面替换
     *
     * @param page_id 输出参数，返回新页面的标识符
     * @return Page* 新页面的指针
     */
    Page* new_page(PageId* page_id);

    /**
     * @brief 删除指定页面
     *
     * 从缓冲池和磁盘中删除页面
     *
     * @param page_id 页面标识符
     * @return true 删除成功
     * @return false 页面不存在或无法删除
     */
    bool delete_page(PageId page_id);

    /**
     * @brief 刷新指定文件的所有页面
     *
     * @param fd 文件描述符
     */
    void flush_all_pages(int fd);

    /**
     * @brief 删除指定文件的所有页面
     *
     * @param fd 文件描述符
     */
    void delete_all_pages(int fd);

    size_t get_instance_no(const PageId& page_id) const;

    auto new_page_guarded(PageId* page_id) -> BasicPageGuard;

    auto fetch_basic_page(PageId page_id) -> BasicPageGuard;

    auto fetch_read_page(PageId page_id) -> ReadPageGuard;

    auto fetch_write_page(PageId page_id) -> WritePageGuard;
};