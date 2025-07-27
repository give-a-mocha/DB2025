/* Copyright (c) 2023 Renmin University of China
| RMDB is licensed under Mulan PSL v2.
| You can use this software according to the terms and conditions of the Mulan PSL v2.
| You may obtain a copy of Mulan PSL v2 at:
|         http://license.coscl.org.cn/MulanPSL2
| THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
| EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
| MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
| See the Mulan PSL v2 for more details. */

#include "ix_scan_temp.h"
#include "ix_index_handle.h"

IxScanTemp::IxScanTemp(IxIndexHandle *ih, const Iid &lower, const Iid &upper, BufferPoolManager *bpm) {
    // The buffer pool manager is not used in this implementation, but is kept for compatibility.
    (void)bpm;
    rids_ = ih->get_rids_in_range(lower, upper);
}