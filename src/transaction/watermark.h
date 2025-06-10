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

#include <map>
#include <unordered_map>

#include "transaction/transaction.h"


class Watermark {
public:
	mutable timestamp_t commit_ts_;             // 当前的提交时间戳
	timestamp_t watermark_;                     // 当前的水位线值
	std::map<timestamp_t, int> current_reads_;  // 当前活跃的读操作时间戳计数

	explicit Watermark(timestamp_t commit_ts) : commit_ts_(commit_ts), watermark_(commit_ts) {}

	void AddTxn(timestamp_t read_ts);

	void RemoveTxn(timestamp_t read_ts);

	void UpdateCommitTs(timestamp_t commit_ts);

	timestamp_t GetWatermark(){
		return watermark_;
	}

};