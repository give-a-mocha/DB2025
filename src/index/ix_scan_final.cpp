/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_scan_final.h"
#include "ix_index_handle.h"

IxScanFinal::IxScanFinal(const IxIndexHandle *ih, const Iid &lower, const Iid &upper, BufferPoolManager *bpm)
    : ih_(ih), bpm_(bpm) {
    if (lower == upper) return;

    Iid iid = lower;
    Page *page = bpm_->fetch_page({ih_->fd_, iid.page_no});
    page->RLatch();
    while (true) {
        auto node = IxNodeHandle(ih_->file_hdr_, page);

        while (iid != upper) {
            rids_.push_back(ih_->get_rid(iid));
            iid.slot_no++;
            if (iid.slot_no >= node.get_size()) {
                break;
            }
        }

        page_id_t next_page_no = node.get_next_leaf();

        if (iid == upper) {
            page->RUnlatch();
            bpm_->unpin_page(page->get_page_id(), false);
            break;
        }

        Page *next_page = bpm_->fetch_page({ih_->fd_, next_page_no});
        next_page->RLatch();

        page->RUnlatch();
        bpm_->unpin_page(page->get_page_id(), false);

        page = next_page;
        iid.page_no = next_page_no;
        iid.slot_no = 0;
    }
}