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
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name,
                   std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids,
                   Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    void delete_index(RmRecord *rec, Rid rid_) {
        // 从索引中删除
        for (auto &index : tab_.indexes) {
            auto ih = sm_manager_->ihs_
                          .at(sm_manager_->get_ix_manager()->get_index_name(
                              tab_name_, index.cols))
                          .get();
            char *key = new char[index.col_tot_len];
            int offset = 0;
            for (size_t i = 0; i < index.col_num; ++i) {
                memcpy(key + offset, rec->data + index.cols[i].offset,
                       index.cols[i].len);
                offset += index.cols[i].len;
            }
            ih->delete_entry(key, context_->txn_);
            delete[] key;
        }
    }

    void insert_index(RmRecord *rec, Rid rid_) {
        // 插入索引
        for (auto &index : tab_.indexes) {
            auto ih = sm_manager_->ihs_
                          .at(sm_manager_->get_ix_manager()->get_index_name(
                              tab_name_, index.cols))
                          .get();
            char *key = new char[index.col_tot_len];
            int offset = 0;
            for (size_t i = 0; i < index.col_num; ++i) {
                memcpy(key + offset, rec->data + index.cols[i].offset,
                       index.cols[i].len);
                offset += index.cols[i].len;
            }
            ih->insert_entry(key, rid_, context_->txn_);
            delete[] key;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        for (auto &rid : rids_) {
            // 获取旧记录
            auto old_rec = fh_->get_record(rid, context_);
            auto new_rec = fh_->get_record(rid, context_);

            // 更新索引：先删除旧的索引项
            delete_index(old_rec.get(), rid);

            for (const auto &set_clause : set_clauses_) {
                auto col = tab_.get_col(set_clause.lhs.col_name);
                // 一定要拷贝
                auto value = set_clause.rhs;
                value.raw = nullptr;
                if (col->type != set_clause.rhs.type) {
                    // 类型不匹配，值类型尝试转换为列类型
                    if (col->type == TYPE_INT && value.type == TYPE_FLOAT) {
                        value.set_int(static_cast<int>(value.float_val));
                    } else if (col->type == TYPE_FLOAT &&
                               value.type == TYPE_INT) {
                        value.set_float(static_cast<float>(value.int_val));
                    } else {
                        throw IncompatibleTypeError(coltype2str(col->type),
                                                    coltype2str(value.type));
                    }
                }

                // 设置新记录的对应列
                value.init_raw(col->len);

                memcpy(new_rec->data + col->offset, value.raw->data, col->len);
            }

            // 插入新的索引项
            insert_index(new_rec.get(), rid);

            // 更新记录
            fh_->update_record(rid, new_rec->data, context_);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};