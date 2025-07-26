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

auto RmFileHandle::AcquirePageReadLock(const Rid& rid) const -> ReadPageGuard {
    if (rid.page_no >= file_hdr_.num_pages) {
        throw PageNotExistError(disk_manager_->get_file_name(fd_), rid.page_no);
    }
    return buffer_pool_manager_->fetch_read_page(PageId{fd_, rid.page_no});
}

auto RmFileHandle::AcquirePageWriteLock(const Rid& rid) -> WritePageGuard {
    if (rid.page_no >= file_hdr_.num_pages) {
        throw PageNotExistError(disk_manager_->get_file_name(fd_), rid.page_no);
    }
    return buffer_pool_manager_->fetch_write_page(PageId{fd_, rid.page_no});
}

auto RmFileHandle::GetTupleWithLockAcquired(const Rid& rid,
                                            const char* data) const -> std::pair<TupleMeta, std::unique_ptr<RmRecord>> {
    RmPageHandle page_handle(&file_hdr_, const_cast<char*>(data));
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    return page_handle.get_tuple(rid.slot_no);
}

auto RmFileHandle::GetTupleMetaWithLockAcquired(const Rid& rid, const char* data) const -> TupleMeta {
    RmPageHandle page_handle(&file_hdr_, const_cast<char*>(data));
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    return page_handle.get_tuple_meta(rid.slot_no);
}

auto RmFileHandle::UpdateTupleWithLockAcquired(const Rid& rid, TupleMeta& meta, const std::unique_ptr<RmRecord>& rec,
                                               char* data) -> void {
    RmPageHandle page_handle(&file_hdr_, data);
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        Bitmap::set(page_handle.bitmap, rid.slot_no);
        page_handle.page_hdr->num_records++;
        file_hdr_.record_num++;
    }
    char* slot_data = page_handle.get_slot(rid.slot_no);
    memcpy(slot_data, &meta, sizeof(TupleMeta));
    char* record_data = slot_data + TUPLE_META_SIZE;
    memcpy(record_data, rec->data, rec->size);
}

auto RmFileHandle::UpdateTupleMetaWithLockAcquired(const Rid& rid, const TupleMeta& new_meta, char* data_) -> void {
    RmPageHandle page_handle(&file_hdr_, data_);
    char* slot_data = page_handle.get_slot(rid.slot_no);
    memcpy(slot_data, &new_meta, sizeof(TupleMeta));
}

std::pair<TupleMeta, std::unique_ptr<RmRecord>> RmFileHandle::get_record(const Rid& rid) const {
    auto page_guard = AcquirePageReadLock(rid);
    return GetTupleWithLockAcquired(rid, page_guard.GetData());
}

auto RmFileHandle::GetNewWritePageGuard() -> WritePageGuard {
    PageId new_page_id = {fd_, INVALID_PAGE_ID};
    auto basic_guard = buffer_pool_manager_->new_page_guarded(&new_page_id);
    RmPageHandle page_handle(&file_hdr_, basic_guard.GetDataMut());
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    page_handle.page_hdr->num_records = 0;
    Bitmap::init(page_handle.bitmap, file_hdr_.bitmap_size);
    file_hdr_.num_pages++;
    file_hdr_.first_free_page_no = basic_guard.PageId().page_no;
    return basic_guard.UpgradeWrite();
}

auto RmFileHandle::GetNewRid() -> Rid {
    std::unique_lock lock(latch_);  // 确保获取新RID的操作是线程安全的
    WritePageGuard page_guard;
    if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
        page_guard = GetNewWritePageGuard();
    } else {
        page_guard = buffer_pool_manager_->fetch_write_page({fd_, file_hdr_.first_free_page_no});
    }

    RmPageHandle page_handle(&file_hdr_, page_guard.GetDataMut());
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);

    Bitmap::set(page_handle.bitmap, slot_no);
    page_handle.page_hdr->num_records++;
    file_hdr_.record_num++;

    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }
    lock.unlock();  // 释放锁以允许其他线程获取新RID

    TupleMeta new_meta(0, true);  // 默认时间戳为0，未删除
    char* slot_data = page_handle.get_slot(slot_no);
    memcpy(slot_data, &new_meta, sizeof(TupleMeta));
    return Rid{page_guard.PageId().page_no, slot_no};
}

Rid RmFileHandle::insert_record(TupleMeta& new_meta, char* buf) {
    std::unique_lock lock(latch_);  // 确保获取新RID的操作是线程安全的
    WritePageGuard page_guard;
    if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
        page_guard = GetNewWritePageGuard();
    } else {
        page_guard = buffer_pool_manager_->fetch_write_page({fd_, file_hdr_.first_free_page_no});
    }

    RmPageHandle page_handle(&file_hdr_, page_guard.GetDataMut());
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);

    Bitmap::set(page_handle.bitmap, slot_no);
    page_handle.page_hdr->num_records++;
    file_hdr_.record_num++;

    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }
    lock.unlock();  // 释放锁以允许其他线程获取新RID

    char* slot_data = page_handle.get_slot(slot_no);
    memcpy(slot_data, &new_meta, sizeof(TupleMeta));
    char* record_data = slot_data + TUPLE_META_SIZE;
    memcpy(record_data, buf, file_hdr_.record_size);

    return Rid{page_guard.PageId().page_no, slot_no};
}

void RmFileHandle::insert_record_force(const Rid& rid, TupleMeta& new_meta, char* buf) {
    auto page_guard = AcquirePageWriteLock(rid);
    RmPageHandle page_handle = RmPageHandle(&file_hdr_, page_guard.GetDataMut());

    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        Bitmap::set(page_handle.bitmap, rid.slot_no);
        page_handle.page_hdr->num_records++;
        file_hdr_.record_num++;
    }

    char* slot_data = page_handle.get_slot(rid.slot_no);
    memcpy(slot_data, &new_meta, sizeof(TupleMeta));
    char* record_data = slot_data + TUPLE_META_SIZE;
    memcpy(record_data, buf, file_hdr_.record_size);
}

void RmFileHandle::delete_record(const Rid& rid) {
    auto page_guard = AcquirePageWriteLock(rid);
    RmPageHandle page_handle = RmPageHandle(&file_hdr_, page_guard.GetDataMut());
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records--;
    file_hdr_.record_num--;
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page - 1) {
        page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
        file_hdr_.first_free_page_no = page_guard.PageId().page_no;
    }
}

void RmFileHandle::update_record(const Rid& rid, TupleMeta& new_meta, char* buf) {
    auto page_guard = AcquirePageWriteLock(rid);
    RmPageHandle page_handle = RmPageHandle(&file_hdr_, page_guard.GetDataMut());
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    char* slot_data = page_handle.get_slot(rid.slot_no);
    memcpy(slot_data, &new_meta, sizeof(TupleMeta));
    char* record_data = slot_data + TUPLE_META_SIZE;
    memcpy(record_data, buf, file_hdr_.record_size);
}

void RmFileHandle::update_tuple_meta(const Rid& rid, const TupleMeta& new_meta) {
    auto page_guard = AcquirePageWriteLock(rid);
    UpdateTupleMetaWithLockAcquired(rid, new_meta, page_guard.GetDataMut());
}