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
#include "common/print.hpp"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"

std::vector<Value> convert_record_to_values(const std::unique_ptr<RmRecord> &record, const std::vector<ColMeta> &cols_);

auto ReconstructTuple(std::unique_ptr<RmRecord> base_tuple, const TupleMeta& base_meta, const std::vector<const UndoLog*> &undo_logs) -> std::unique_ptr<RmRecord>;

auto IsWriteWriteConflict(Transaction *txn, TransactionManager *txn_mgr, UndoLink undolink) -> bool;

bool mvcc_insert_index(const TabMeta &tab_, std::unique_ptr<RmRecord> &rec, Rid rid, Context *context_,
                       TransactionManager *txn_mgr, SmManager *sm_manager);

/**
 * @brief 递归地计算算术表达式的值
 * @param term 当前要求值的表达式项
 * @param record 当前记录/元组
 * @param cols 表的所有列元数据
 * @return 计算得到的 Value
 */
Value EvaluateExpr(const ExprTerm &term, const std::unique_ptr<RmRecord> &record, const std::vector<ColMeta> &cols);

Value GetColumnValue(const std::unique_ptr<RmRecord> &record, const ColMeta &col);