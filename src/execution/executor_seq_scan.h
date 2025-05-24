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

/**
     * @brief 初始化顺序扫描，定位到第一个满足条件的记录。
     *
     * 创建表记录的顺序扫描迭代器，并跳过不满足过滤条件的记录，直到找到第一个符合条件的记录或扫描结束。
     */
     
    /**
     * @brief 移动到下一个满足条件的记录。
     *
     * 将扫描器前进到下一个符合所有过滤条件的记录。如果扫描尚未初始化，则抛出异常。
     */
     
    /**
     * @brief 判断扫描是否结束。
     *
     * @return 若扫描器未初始化或已到达末尾，返回 true；否则返回 false。
     */
     
    /**
     * @brief 获取当前记录。
     *
     * @return 指向当前记录的唯一指针。
     */
     
    /**
     * @brief 获取每条记录的长度。
     *
     * @return 记录长度（字节数）。
     */
     
    /**
     * @brief 获取表的字段元数据。
     *
     * @return 字段元数据的常量引用。
     */
     
    /**
     * @brief 获取当前记录的标识符。
     *
     * @return 当前记录的标识符引用。
     */
    class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    void beginTuple() override {
        // Todo:
        // !需要自己实现
        scan_ = std::make_unique<RmScan>(fh_);
        // 移动到第一个满足条件的记录
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                return;
            }
            scan_->next();
        }
    }

    void nextTuple() override {
        // Todo:
        // !需要自己实现
        if(scan_ == nullptr){
            throw InternalError("Scan not initialized");
        }
        scan_->next();
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

    bool is_end() const override {
        return scan_ == nullptr || scan_->is_end();
    }

    std::unique_ptr<RmRecord> Next() override {
        return fh_->get_record(rid_, context_);
    }

    size_t tupleLen() const override {
        return len_;
    }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    Rid &rid() override { return rid_; }

private:
    /**
     * @brief 检查记录是否满足所有条件
     */
    bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds, const RmRecord *rec) {
        // Todo:
        // !需要自己实现
        for (const auto &cond : conds) {
            if (!eval_cond(rec_cols, cond, rec)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief 判断指定记录是否满足给定的单个条件。
     *
     * 根据条件描述，比较记录中指定列与常量值或另一列的值，支持整数、浮点数和字符串类型。若类型不一致则抛出类型不兼容异常。根据条件操作符返回比较结果。
     *
     * @param rec_cols 记录的列元数据集合。
     * @param cond 需要判断的条件。
     * @param rec 待判断的记录指针。
     * @return 若记录满足条件返回 true，否则返回 false。
     *
     * @throws IncompatibleTypeError 当左右操作数类型不一致时抛出。
     */
    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const RmRecord *rec) {
        // Todo:
        // !需要自己实现
        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        char *lhs_data = rec->data + lhs_col->offset;
        char *rhs_data;
        ColType rhs_type;

        if (cond.is_rhs_val) {
            rhs_data = cond.rhs_val.raw->data;
            rhs_type = cond.rhs_val.type;
        } else {
            auto rhs_col = get_col(rec_cols, cond.rhs_col);
            rhs_data = rec->data + rhs_col->offset;
            rhs_type = rhs_col->type;
        }

        // 类型应该一致
        if(lhs_col->type != rhs_type){
            throw IncompatibleTypeError(coltype2str(lhs_col->type), coltype2str(rhs_type));
        }

        int cmp;
        if (lhs_col->type == TYPE_INT) {
            cmp = *(int*)lhs_data - *(int*)rhs_data;
        } else if (lhs_col->type == TYPE_FLOAT) {
            float diff = *(float*)lhs_data - *(float*)rhs_data;
            cmp = (diff > 0) ? 1 : (diff < 0) ? -1 : 0;
        } else if (lhs_col->type == TYPE_STRING) {
            cmp = memcmp(lhs_data, rhs_data, lhs_col->len);
        }

        switch (cond.op) {
            case OP_EQ: return cmp == 0;
            case OP_NE: return cmp != 0;
            case OP_LT: return cmp < 0;
            case OP_GT: return cmp > 0;
            case OP_LE: return cmp <= 0;
            case OP_GE: return cmp >= 0;
            default: return false;
        }
    }
};