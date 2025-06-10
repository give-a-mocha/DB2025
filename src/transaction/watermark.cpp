/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction/watermark.h"

void Watermark::AddTxn(timestamp_t read_ts){
	current_reads_[read_ts]++;
}

void Watermark::RemoveTxn(timestamp_t read_ts){
	auto it = current_reads_.find(read_ts);
	if (it != current_reads_.end()) {
		it->second--;
		if (it->second <= 0) {
			current_reads_.erase(it);
		}
	}
}

void Watermark::UpdateCommitTs(timestamp_t commit_ts){
	commit_ts_ = commit_ts;
	if (watermark_ < commit_ts) {
		watermark_ = commit_ts;
	}
}
