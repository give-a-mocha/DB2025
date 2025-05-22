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
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid,
                                                   Context* context) const {
    // Todo:
    // !1. 获取指定记录所在的page handle
    // !2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）

    if (context != nullptr) {
        context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);
    }

    // 获取页面句柄
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);

    // 创建RmRecord并复制数据
    char* slot = page_handle.get_slot(rid.slot_no);
    auto record = std::make_unique<RmRecord>(file_hdr_.record_size, slot);

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);

    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // Todo:
    // !1. 获取当前未满的page handle
    // !2. 在page handle中找到空闲slot位置
    // !3. 将buf复制到空闲slot位置
    // !4. 更新page_handle.page_hdr中的数据结构
    // !注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no

    if (context != nullptr) {
        context->lock_mgr_->lock_exclusive_on_table(context->txn_, fd_);
    }

    // 获取空闲页面
    RmPageHandle page_handle = create_page_handle();

    // 找到空闲slot
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap,
                                    file_hdr_.num_records_per_page);
    // 复制数据到slot
    char* slot = page_handle.get_slot(slot_no);
    memcpy(slot, buf, file_hdr_.record_size);

    // 设置bitmap和更新记录数
    Bitmap::set(page_handle.bitmap, slot_no);
    page_handle.page_hdr->num_records++;

    // 如果页面已满,更新空闲页面链表
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);

    return Rid{page_handle.page->get_page_id().page_no, slot_no};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    // Todo: 我上下文呢？

    // 获取页面句柄
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);

    // 检查slot是否已被占用
    if (Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    // 复制数据到指定slot
    char* slot = page_handle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);

    // 设置bitmap和更新记录数
    Bitmap::set(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records++;
    if (page_handle.page_hdr->num_records ==
        page_handle.file_hdr->num_records_per_page) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // Todo:
    // !1. 获取指定记录所在的page handle
    // !2. 更新page_handle.page_hdr中的数据结构
    // !注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()

    if (context != nullptr) {
        context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
    }

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

    // 如果页面从满变为未满,加入空闲页面链表
    if (page_handle.page_hdr->num_records ==
        file_hdr_.num_records_per_page - 1) {
        release_page_handle(page_handle);
    }

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // Todo:
    // !1. 获取指定记录所在的page handle
    // !2. 更新记录

    if (context != nullptr) {
        context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
    }

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
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
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
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // 使用缓冲池创建新页面
    PageId new_page_id = {fd_, INVALID_PAGE_ID};
    Page* page = buffer_pool_manager_->new_page(&new_page_id);
    if (page == nullptr) {
        throw PageNotExistError(disk_manager_->get_file_name(fd_),
                                new_page_id.page_no);
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
 * @description:
 * 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle& page_handle) {
    // 将当前页面加入空闲页面链表
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
}
