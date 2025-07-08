/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中指定记录号的记录
 *
 * 该函数执行以下操作：
 * 1. 获取共享锁（如果在事务中）
 * 2. 从缓冲池获取对应页面
 * 3. 定位并复制记录数据
 * 4. 释放页面（unpin）
 *
 * @param {Rid&} rid 记录的标识符，包含页面号和槽位号
 * @param {Context*} context 事务上下文，用于并发控制
 * @return {unique_ptr<RmRecord>} 记录对象的智能指针
 * @throw RecordNotFoundError 如果记录不存在
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // Todo:
    // !1. 获取指定记录所在的page handle
    // !2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
    // if (context != nullptr) {
    //     context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);
    // }
    // 获取页面句柄
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    // 创建RmRecord并复制数据
    char* slot = page_handle.get_slot(rid.slot_no);
    auto record = std::make_unique<RmRecord>(file_hdr_.record_size, slot);

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);

    return record;
}

/**
 * @description: 在当前表中插入一条新记录
 *
 * 该函数执行以下操作：
 * 1. 获取表的排他锁（如果在事务中）
 * 2. 获取或创建有空闲空间的页面
 * 3. 在页面中找到第一个空闲槽位
 * 4. 复制记录数据到槽位
 * 5. 更新位图和记录计数
 * 6. 必要时更新空闲页面链表
 *
 * @param {char*} buf 要插入的记录数据
 * @param {Context*} context 事务上下文，用于并发控制
 * @return {Rid} 新插入记录的标识符
 */
// 修改后的 insert_record 函数，支持 MVCC (基于 Context 和 UndoLog 将被更新的假设)
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // 插入记录的步骤：
    // 1. 获取或创建一个有空闲空间的页面句柄
    // 2. 使用位图找到页面中第一个空闲槽位
    // 3. 将记录数据复制到找到的槽位
    // 4. 更新页面头部信息(记录数等)
    // 5. 如果页面已满，更新文件头的空闲页面链表
    // 6. 返回新记录的RID标识符

    // if (context != nullptr) {
    //     context->lock_mgr_->lock_exclusive_on_table(context->txn_, fd_);
    // }

    // 获取空闲页面
    RmPageHandle page_handle = create_page_handle();
    page_handle.page->WLatch();

    // 找到空闲slot
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);
    // 设置bitmap和更新记录数
    Bitmap::set(page_handle.bitmap, slot_no);
    page_handle.page_hdr->num_records++;
    file_hdr_.record_num++;
    // 如果页面已满,更新空闲页面链表
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }
    page_handle.page->WUnlatch();
    // 复制数据到slot
    char* slot = page_handle.get_slot(slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);

    return Rid{page_handle.page->get_page_id().page_no, slot_no};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    // 获取页面句柄
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    // 检查slot是否已被占用
    if (Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    // 设置bitmap和更新记录数
    Bitmap::set(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records++;
    file_hdr_.record_num++;
    if (page_handle.page_hdr->num_records == page_handle.file_hdr->num_records_per_page) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }

    // 复制数据到指定slot
    char* slot = page_handle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

void RmFileHandle::insert_record_force(const Rid& rid, char* buf) {
    // 获取页面句柄
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    // 设置bitmap和更新记录数
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        Bitmap::set(page_handle.bitmap, rid.slot_no);
        page_handle.page_hdr->num_records++;
    }
    file_hdr_.record_num++;
    if (page_handle.page_hdr->num_records == page_handle.file_hdr->num_records_per_page) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }
    // 复制数据到指定slot
    char* slot = page_handle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 删除指定的记录
 *
 * 该函数执行以下操作：
 * 1. 获取记录的排他锁（如果在事务中）
 * 2. 检查记录是否存在
 * 3. 更新位图标记槽位为空闲
 * 4. 更新记录计数
 * 5. 如果页面从满变为非满，将其加入空闲页面链表
 *
 * @param {Rid&} rid 要删除的记录标识符
 * @param {Context*} context 事务上下文
 * @throw RecordNotFoundError 如果记录不存在
 */
// 修改后的 delete_record 函数，支持 MVCC (基于 Context 和 UndoLog 将被更新的假设)
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // 删除记录的步骤：
    // 1. 获取记录所在页面的句柄
    // 2. 将对应槽位在位图中标记为空闲
    // 3. 更新页面头部信息(减少记录数)
    // 4. 如果页面从满变为非满，需要将其加入空闲页面链表
    // 5. 更新文件头的记录总数

    // if (context != nullptr) {
    //     context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
    // }

    // 获取页面句柄
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    // 检查record是否存在
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }

    // 复位bitmap
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records--;
    file_hdr_.record_num--;

    // 如果页面从满变为未满,加入空闲页面链表
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page - 1) {
        release_page_handle(page_handle);
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 更新指定记录的内容
 *
 * 该函数执行以下操作：
 * 1. 获取记录的排他锁（如果在事务中）
 * 2. 检查记录是否存在
 * 3. 用新数据覆盖原记录数据
 * 4. 标记页面为脏页
 *
 * @param {Rid&} rid 要更新的记录标识符
 * @param {char*} buf 新记录数据
 * @param {Context*} context 事务上下文
 * @throw RecordNotFoundError 如果记录不存在
 */
// 修改后的 update_record 函数，支持 MVCC (基于 Context 和 UndoLog 将被更新的假设)
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // 更新记录的步骤：
    // 1. 获取记录所在页面的句柄
    // 2. 验证记录是否存在
    // 3. 用新数据覆盖原记录
    // 4. 标记页面为脏页以便后续写回磁盘

    // if (context != nullptr) {
    //     context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
    // }

    // 获取页面句柄
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    // 检查record是否存在
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    // 更新记录
    char* slot = page_handle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
 */
/**
 * @description: 获取指定页面的句柄
 *
 * @param {int} page_no 目标页面号
 * @return {RmPageHandle} 页面句柄对象
 * @throw PageNotExistError 如果页面不存在或无法访问
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    if (page_no >= file_hdr_.num_pages) {
        throw PageNotExistError(disk_manager_->get_file_name(fd_), page_no);
    }
    // 使用缓冲池获取指定页面
    Page* page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    if (page == nullptr) {
        throw PageNotExistError(disk_manager_->get_file_name(fd_), page_no);
    }
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建新的页面句柄
 *
 * @return {RmPageHandle} 新创建的页面句柄
 * @throw PageNotExistError 如果无法创建新页面
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // 使用缓冲池创建新页面
    PageId new_page_id = {fd_, INVALID_PAGE_ID};
    Page* page = buffer_pool_manager_->new_page(&new_page_id);
    if (page == nullptr) {
        throw PageNotExistError(disk_manager_->get_file_name(fd_), new_page_id.page_no);
    }

    // 初始化页面头信息
    RmPageHandle page_handle(&file_hdr_, page);
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    page_handle.page_hdr->num_records = 0;

    // 初始化bitmap
    Bitmap::init(page_handle.bitmap, file_hdr_.bitmap_size);

    // 更新文件头信息
    file_hdr_.num_pages++;
    file_hdr_.first_free_page_no = page->get_page_id().page_no;
    return page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
        // 没有空闲页面,创建新页面
        RmPageHandle page_handle = create_new_page_handle();
        return page_handle;
    } else {
        // 有空闲页面,获取第一个空闲页
        return fetch_page_handle(file_hdr_.first_free_page_no);
    }
}

/**
 * @description: 释放页面句柄并更新空闲页面链表
 *
 * 当页面状态从满变为非满时，需要：
 * 1. 将该页面添加到空闲页面链表的头部
 * 2. 更新文件头中的第一个空闲页面指针
 * 3. 更新页面头部的下一个空闲页面指针
 *
 * @param {RmPageHandle&} page_handle 要释放的页面句柄
 */
void RmFileHandle::release_page_handle(RmPageHandle& page_handle) {
    // 将当前页面加入空闲页面链表
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
}