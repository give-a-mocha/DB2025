/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include <memory>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    bool update_index(RmRecord *rec, Rid rid_) {
        // 更新索引
        for (auto &index : tab_.indexes) {
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            auto key = std::make_unique<char[]>(index.col_tot_len);
            int offset = 0;
            for (size_t i = 0; i < static_cast<size_t>(index.col_num); ++i) {
                memcpy(key.get() + offset, rec->data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            auto res = ih->update_entry(key.get(), rid_, context_->txn_);
            if(res == false){
                return false;  // 更新索引失败
            }
        }
    }


    std::unique_ptr<RmRecord> Next() override {
        std::vector<std::unique_ptr<RmRecord>> old_records;  // 保存旧记录用于回滚
        std::vector<std::unique_ptr<RmRecord>> new_records;  // 保存新记录
        old_records.reserve(rids_.size());
        new_records.reserve(rids_.size());

        // 第一阶段：准备所有新记录
        for (size_t i = 0; i < rids_.size(); ++i) {
            auto &rid = rids_[i];
            // 获取旧记录
            auto old_rec = fh_->get_record(rid, context_);
            auto new_rec = fh_->get_record(rid, context_);

            for (const auto &set_clause : set_clauses_) {
                auto col = tab_.get_col(set_clause.lhs.col_name);
                // 一定要拷贝
                auto value = set_clause.rhs;
                value.raw.reset();
                if (col->type != set_clause.rhs.type) {
                    // 类型不匹配，值类型尝试转换为列类型
                    if (col->type == ColType::TYPE_INT && value.type == ColType::TYPE_FLOAT) {
                        value.set_int(static_cast<int>(value.float_val));
                    } else if (col->type == ColType::TYPE_FLOAT && value.type == ColType::TYPE_INT) {
                        value.set_float(static_cast<float>(value.int_val));
                    } else {
                        throw IncompatibleTypeError(coltype2str(col->type), coltype2str(value.type));
                    }
                }
                // 设置新记录的对应列
                value.init_raw(col->len);
                memcpy(new_rec->data + col->offset, value.raw->data, col->len);
            }

            old_records.emplace_back(std::move(old_rec));
            new_records.emplace_back(std::move(new_rec));
            if(!update_index(new_records[i].get(), rids_[i])) {
                for(size_t j = 0; j < i; ++j) {
                    update_index(old_records[j].get(), rids_[j]);
                }
                throw RMDBError("Failed to update index, rolled back all changes at " + getType());
            }
        }
        // 更新所有记录
        for (size_t i = 0; i < rids_.size(); ++i) {
            fh_->update_record(rids_[i], new_records[i]->data, context_);
        }

        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }

    std::string getType() { return "UpdateExecutor"; }
};
