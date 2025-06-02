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
 * @brief 索引扫描执行器，通过索引快速访问满足条件的记录
 *
 * 该执行器使用表的索引结构来高效地访问满足查询条件的记录。主要功能：
 * 1. 根据查询条件构建索引扫描范围
 * 2. 利用索引进行范围扫描
 * 3. 对扫描到的记录进行条件过滤
 *
 * 索引扫描比全表扫描更高效，特别是在查询条件命中索引时
 */
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
   /**
    * @brief 构造函数
    *
    * 初始化索引扫描执行器，设置扫描参数和打开必要的文件句柄
    *
    * @param sm_manager 系统管理器指针
    * @param tab_name 要扫描的表名
    * @param conds 扫描条件
    * @param index_col_names 索引涉及的列名
    * @param context 执行上下文
    */
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
                                     sm_manager_->get_ix_manager()->open_index(tab_name_, index_col_names_));
        }

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

    /**
     * @brief 开始扫描第一个元组
     *
     * 该方法：
     * 1. 根据条件构建索引扫描范围
     * 2. 创建索引扫描器
     * 3. 定位到第一个满足条件的记录
     */
    void beginTuple() override {
        // 构建索引查询范围
        auto ih = sm_manager_->ihs_.at(index_name_).get();
        // 从条件中提取索引键的范围
        RmRecord lower_record(index_meta_.col_tot_len), upper_record(index_meta_.col_tot_len);
        off_t offset = 0;

        for (const auto &col : index_meta_.cols) {
            Value max_val, min_val;
            switch (col.type) {
                case ColType::TYPE_INT: {
                    max_val.set_int(std::numeric_limits<int>::max());
                    min_val.set_int(std::numeric_limits<int>::min());
                    max_val.init_raw(sizeof(int)), min_val.init_raw(sizeof(int));
                    break;
                }
                case ColType::TYPE_FLOAT: {
                    max_val.set_float(std::numeric_limits<float>::max());
                    min_val.set_float(std::numeric_limits<float>::lowest());
                    max_val.init_raw(sizeof(float)), min_val.init_raw(sizeof(float));
                    break;
                }
                case ColType::TYPE_STRING: {
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
            }
            memcpy(lower_record.data + offset, min_val.raw->data, col.len);
            memcpy(upper_record.data + offset, max_val.raw->data, col.len);
            offset += col.len;
        }

        auto lower_iid = ih->lower_bound(lower_record.data);
        auto upper_iid = ih->upper_bound(upper_record.data);
        scan_ = std::make_unique<IxScan>(ih, lower_iid, upper_iid, sm_manager_->get_bpm());
        // 移动到第一个满足条件的记录
        while (!is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                return;
            }
            scan_->next();
        }
    }

    /**
     * @brief 移动到下一个满足条件的元组
     *
     * 沿着索引继续扫描，直到找到下一个满足所有条件的记录。
     * 如果扫描器未初始化，会抛出内部错误。
     *
     * @throw InternalError 当扫描器未初始化时
     */
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

    /**
     * @brief 检查是否完成所有记录的扫描
     * @return 如果扫描器为空或已到达末尾则返回true，否则返回false
     */
    bool is_end() const override { return scan_ == nullptr || scan_->is_end(); }

    /**
     * @brief 获取当前记录
     * @return 当前记录的智能指针
     */
    std::unique_ptr<RmRecord> Next() override { return fh_->get_record(rid_, context_); }

    /**
     * @brief 获取记录长度
     * @return 记录的字节长度
     */
    size_t tupleLen() const override { return len_; }

    /**
     * @brief 获取扫描涉及的所有列元数据
     * @return 列元数据向量的常量引用
     */
    const std::vector<ColMeta> &cols() const override { return cols_; }

    /**
     * @brief 获取指定列的元数据
     * @param target 目标列的表列引用
     * @return 目标列的元数据
     */
    ColMeta get_col_offset(const TabCol &target) override {
        auto pos = get_col(cols_, target);
        return *pos;
    }

    /**
     * @brief 获取当前记录的RID
     * @return 当前记录的RID引用
     */
    Rid &rid() override { return rid_; }

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    std::string getType() override { return "IndexScanExecutor"; }

   private:
    /**
     * @brief 比较两个值
     *
     * 支持多种类型的值比较：
     * - 数值类型(INT, FLOAT)之间可以互相转换后比较
     * - 字符串类型按字典序比较
     *
     * @param lhs 左操作数
     * @param rhs 右操作数
     * @param op 比较操作符
     * @return 比较结果
     * @throw IncompatibleTypeError 当比较的类型不兼容时
     */
    bool compare(Value lhs, Value rhs, CompOp op) {
        bool is_numeric = is_numeric_type(lhs.type) && is_numeric_type(rhs.type);
        if (lhs.type != rhs.type && !is_numeric) {
            throw IncompatibleTypeError(coltype2str(lhs.type), coltype2str(rhs.type));
        }
        int cmp;
        if (is_numeric) {
            // 整数比较
            if (lhs.type == ColType::TYPE_INT && rhs.type == ColType::TYPE_INT) {
                cmp = (lhs.int_val < rhs.int_val) ? -1 : (lhs.int_val > rhs.int_val) ? 1 : 0;
            } else {
                // 先转化成浮点数
                convert(lhs, rhs);
                // 浮点数比较
                cmp = (lhs.float_val < rhs.float_val) ? -1 : (lhs.float_val > rhs.float_val) ? 1 : 0;
            }
        } else if (lhs.type == ColType::TYPE_STRING) {
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