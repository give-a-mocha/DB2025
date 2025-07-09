/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"
#include "common/TraceStack.hpp"

/**
 * @description: 获取指定的页面
 *
 * @param {PageId} page_id 需要获取的页面ID
 * @return {Page*} 获取的页面指针，如果无法获取则返回nullptr
 * @note 该函数使用互斥锁保护并发访问
 */
Page* BufferPoolManager::fetch_page(PageId page_id) {
    TRACE_FUNCTION
    return buffer_pool_instances_[get_instance_no(page_id)]->fetch_page(page_id);
}

/**
 * @description: 取消固定一个页面
 *
 * 该函数完成以下操作：
 * 1. 在页表中查找目标页面
 * 2. 如果页面不存在或已经完全解除固定(pin_count=0)，返回false
 * 3. 减少页面的引用计数：
 *    - 如果引用计数降为0，在替换器中标记为可替换
 * 4. 根据is_dirty参数更新页面的脏页标记
 *
 * @param {PageId} page_id 目标页面的ID
 * @param {bool} is_dirty 是否将页面标记为脏页
 * @return {bool} true表示成功解除固定，false表示操作失败
 * @note 该函数使用互斥锁保护并发访问
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    TRACE_FUNCTION
    return buffer_pool_instances_[get_instance_no(page_id)]->unpin_page(page_id, is_dirty);
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    TRACE_FUNCTION
    return buffer_pool_instances_[get_instance_no(page_id)]->flush_page(page_id);
}

/**
 * @description: 创建新页面

 * @param {PageId*} page_id 输出参数，存储新创建页面的ID
 * @return {Page*} 新创建的页面指针，如果创建失败则返回nullptr
 * @note 该函数使用互斥锁保护并发访问
 */
Page* BufferPoolManager::new_page(PageId* page_id) {
    TRACE_FUNCTION
    page_id->page_no = disk_manager_->allocate_page(page_id->fd);
    return buffer_pool_instances_[get_instance_no(*page_id)]->new_page(page_id);
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool}
 * 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    TRACE_FUNCTION
    return buffer_pool_instances_[get_instance_no(page_id)]->delete_page(page_id);
}

/**
 * @description: 将指定文件的所有缓冲页刷新到磁盘
 *
 * 该函数执行以下操作：
 * 1. 遍历页表查找属于指定文件的所有页面
 * 2. 将找到的页面写回磁盘，不论是否为脏页
 * 3. 重置页面的脏页标记
 *
 * @param {int} fd 要刷新的文件描述符
 * @note 该函数使用互斥锁保护并发访问
 */
void BufferPoolManager::flush_all_pages(int fd) {
    TRACE_FUNCTION
    for (size_t i = 0; i < BUFFER_POOL_INSTANCE_SIZE; ++i) {
        buffer_pool_instances_[i]->flush_all_pages(fd);
    }
}

/**
 * @description: 从缓冲池中删除指定文件的所有页面
 * @param {int} fd 要删除的文件描述符
 * @note 该函数使用互斥锁保护并发访问
 */
void BufferPoolManager::delete_all_pages(int fd) {
    TRACE_FUNCTION
    for (size_t i = 0; i < BUFFER_POOL_INSTANCE_SIZE; ++i) {
        buffer_pool_instances_[i]->delete_all_pages(fd);
    }
}

auto BufferPoolManager::new_page_guarded(PageId* page_id) -> BasicPageGuard {
    TRACE_FUNCTION
    page_id->page_no = disk_manager_->allocate_page(page_id->fd);
    return buffer_pool_instances_[get_instance_no(*page_id)]->new_page_guarded(page_id);
}

auto BufferPoolManager::fetch_basic_page(PageId page_id) -> BasicPageGuard {
    TRACE_FUNCTION
    return buffer_pool_instances_[get_instance_no(page_id)]->fetch_basic_page(page_id);
}

auto BufferPoolManager::fetch_read_page(PageId page_id) -> ReadPageGuard {
    TRACE_FUNCTION
    return buffer_pool_instances_[get_instance_no(page_id)]->fetch_read_page(page_id);
}

auto BufferPoolManager::fetch_write_page(PageId page_id) -> WritePageGuard {
    TRACE_FUNCTION
    return buffer_pool_instances_[get_instance_no(page_id)]->fetch_write_page(page_id);
}