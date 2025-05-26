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

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表名称
    TabMeta tab_;                       // 表的元数据
    std::vector<Condition> conds_;      // 扫描条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // 需要读取的字段
    size_t len_;                        // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;  // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;
    std::string index_name_;  // 索引名称
    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
                      std::vector<std::string> index_col_names, Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names;
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        index_name_ = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_);
        if (!sm_manager->ihs_.count(index_name_)) {
            // 如果没有打开则打开文件
            sm_manager->ihs_.emplace(index_name_,
                                     sm_manager_->get_ix_manager()->open_index(tab_name_, index_col_names));
        }

        std::function<CompOp(CompOp)> swap_op = [](CompOp op) {
            switch (op) {
                case CompOp::OP_EQ:
                    return CompOp::OP_EQ;
                case CompOp::OP_NE:
                    return CompOp::OP_NE;
                case CompOp::OP_LT:
                    return CompOp::OP_GT;
                case CompOp::OP_GT:
                    return CompOp::OP_LT;
                case CompOp::OP_LE:
                    return CompOp::OP_GE;
                case CompOp::OP_GE:
                    return CompOp::OP_LE;
                default:
                    throw InternalError("Unexpected comparison operator");
            }
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        // 构建索引查询范围
        auto ih = sm_manager_->ihs_.at(index_name_).get();
        // 从条件中提取索引键的范围
        RmRecord lower_record(index_meta_.col_tot_len), upper_record(index_meta_.col_tot_len);
        off_t offset = 0;

        for (const auto &col : index_meta_.cols) {
            Value max_val, min_val;
            switch (col.type) {
                case TYPE_INT: {
                    max_val.set_int(std::numeric_limits<int>::max());
                    min_val.set_int(std::numeric_limits<int>::min());
                    max_val.init_raw(sizeof(int)), min_val.init_raw(sizeof(int));
                    break;
                }
                case TYPE_FLOAT: {
                    max_val.set_float(std::numeric_limits<float>::max());
                    min_val.set_float(std::numeric_limits<float>::min());
                    max_val.init_raw(sizeof(float)), min_val.init_raw(sizeof(float));
                    break;
                }
                case TYPE_STRING: {
                    max_val.set_str(std::string(col.len, 255));
                    min_val.set_str(std::string(col.len, 0));
                    max_val.init_raw(col.len), min_val.init_raw(col.len);
                    break;
                }
                default:
                    throw InternalError("Unsupported column type in index scan");
            }
            for (const auto &cond : fed_conds_) {
                if (cond.lhs_col.col_name == col.name && cond.is_rhs_val) {
                    switch (cond.op) {
                        case CompOp::OP_EQ: {
                            if (compare(cond.rhs_val, min_val, CompOp::OP_GT)) {
                                min_val = cond.rhs_val;
                            }
                            if (compare(cond.rhs_val, max_val, CompOp::OP_LT)) {
                                max_val = cond.rhs_val;
                            }
                            break;
                        }

                        case CompOp::OP_LT:
                        case CompOp::OP_LE: {
                            if (compare(cond.rhs_val, max_val, CompOp::OP_LT)) {
                                max_val = cond.rhs_val;
                            }
                            break;
                        }

                        case CompOp::OP_GT:
                        case CompOp::OP_GE: {
                            if (compare(cond.rhs_val, min_val, CompOp::OP_GT)) {
                                min_val = cond.rhs_val;
                            }
                            break;
                        }

                        case CompOp::OP_NE: {
                            // 对于不等于的情况，忽略
                            // 这里不处理，因为索引扫描不支持不等于条件
                            break;
                        }

                        default:
                            throw InternalError("Unexpected comparison operator in index scan condition at " +
                                                getType());
                    }
                }
                memcpy(lower_record.data + offset, min_val.raw->data, col.len);
                memcpy(upper_record.data + offset, max_val.raw->data, col.len);
                offset += col.len;
            }
        }

        auto lower_iid = ih->lower_bound(lower_record.data);
        auto upper_iid = ih->upper_bound(upper_record.data);
        scan_ = std::make_unique<IxScan>(ih, lower_iid, upper_iid, sm_manager_->get_bpm());
        // 移动到第一个满足条件的记录
        while (is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                return;
            }
            scan_->next();
        }
    }

    void nextTuple() override {
        if (scan_ == nullptr) {
            throw InternalError("Scan not initialized at " + getType());
        }
        if (!scan_->is_end()) {
            scan_->next();
        }
        // 移动到下一个满足条件的记录
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                return;
            }
            scan_->next();
        }
    }

    bool is_end() const override { return scan_ == nullptr || scan_->is_end(); }

    std::unique_ptr<RmRecord> Next() override { return fh_->get_record(rid_, context_); }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    ColMeta get_col_offset(const TabCol &target) override {
        auto pos = get_col(cols_, target);
        return *pos;
    }

    Rid &rid() override { return rid_; }

    std::string getType() override { return "IndexScanExecutor"; }

   private:
    bool compare(Value lhs, Value rhs, CompOp op) {
        bool is_numeric = is_numeric_type(lhs.type) && is_numeric_type(rhs.type);
        if (lhs.type != rhs.type && !is_numeric) {
            throw IncompatibleTypeError(coltype2str(lhs.type), coltype2str(rhs.type));
        }
        int cmp;
        if (is_numeric) {
            // 整数比较
            if (lhs.type == TYPE_INT && rhs.type == TYPE_INT) {
                cmp = (lhs.int_val < rhs.int_val) ? -1 : (lhs.int_val > rhs.int_val) ? 1 : 0;
            } else {
                // 先转化成浮点数
                convert(lhs, rhs);
                // 浮点数比较
                cmp = (lhs.float_val < rhs.float_val) ? -1 : (lhs.float_val > rhs.float_val) ? 1 : 0;
            }
        } else if (lhs.type == TYPE_STRING) {
            size_t len = std::max(lhs.str_val.size(), rhs.str_val.size());
            cmp = strncmp(lhs.str_val.c_str(), rhs.str_val.c_str(), len);
        }
        switch (op) {
            case CompOp::OP_EQ:
                return cmp == 0;
            case CompOp::OP_NE:
                return cmp != 0;
            case CompOp::OP_LT:
                return cmp < 0;
            case CompOp::OP_GT:
                return cmp > 0;
            case CompOp::OP_LE:
                return cmp <= 0;
            case CompOp::OP_GE:
                return cmp >= 0;
            default:
                throw InternalError("compare::Unexpected op type at " + getType());
        }
    }
};