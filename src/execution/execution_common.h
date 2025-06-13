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

#include <optional>
#include <vector>

#include "common/common.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"
#include "common/print.hpp"

auto ReconstructTuple(const std::vector<ColMeta> &cols, const RmRecord &base_tuple, const TupleMeta &base_meta,
const std::vector<UndoLog> &undo_logs) -> std::optional<RmRecord>;

auto IsWriteWriteConflict(timestamp_t tuple_ts, Transaction *txn) -> bool;

auto message_out(Context *context_, const std::string &output) -> void;

auto message_out(Context *context_, const char *output, size_t output_size) -> void;

std::vector<Value> convert_record_to_values(
	const std::unique_ptr<RmRecord> &record, 
	const std::vector<ColMeta> &cols_
);

std::unique_ptr<RmRecord> mvcc_get_record(
	const Rid &rid, 
	Context *context_,
	RmFileHandle *fh_,
	TransactionManager *txn_mgr_,
	const std::vector<ColMeta> &cols_
);

Rid mvcc_insert_record(
	char *buf, 
	Context *context_,
	RmFileHandle *fh_,
	TransactionManager *txn_mgr_,
	const std::vector<Value> &valus_
);

void mvcc_delete_record(
	const Rid &rid,
	Context *context_,
	RmFileHandle *fh_,
	TransactionManager *txn_mgr_,
	const std::vector<ColMeta> &cols_
);
void mvcc_update_record(
	const Rid &rid,
	std::unique_ptr<RmRecord> &new_rec,
	std::unique_ptr<RmRecord> &old_rec,
	Context *context_,
	TransactionManager *txn_mgr_,
	const std::vector<ColMeta> &cols_,
	std::vector<bool> is_modify
);

/**
 * @brief 递归地计算算术表达式的值
 * @param term 当前要求值的表达式项
 * @param record 当前记录/元组
 * @param cols 表的所有列元数据
 * @return 计算得到的 Value
 */
Value EvaluateExpr(const ExprTerm &term, const RmRecord &record, const std::vector<ColMeta> &cols);

Value GetColumnValue(const RmRecord &record, const ColMeta &col);