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
 * @brief 索引扫描执行器，负责实现基于索引的高效记录访问
 */
class IndexScanExecutor : public AbstractExecutor {
   private:
    /**
     * @brief 表的基本信息
     * @note 用于元数据访问和验证
     */
    std::string tab_name_;  // 表名
    TabMeta tab_;           // 表元数据

    /**
     * @brief 查询条件相关
     * @note 用于过滤和优化
     */
    std::vector<Condition> conds_;      // 原始条件
    std::vector<Condition> fed_conds_;  // 优化后的条件

    /**
     * @brief 数据访问相关
     * @note 处理记录读取和缓存
     */
    RmFileHandle *fh_;           // 表文件句柄
    std::vector<ColMeta> cols_;  // 输出列定义
    size_t len_;                 // 记录长度

    /**
     * @brief 索引访问相关
     * @note 管理索引扫描状态
     */
    std::vector<std::string> index_col_names_;  // 索引列
    IndexMeta index_meta_;                      // 索引元数据
    std::string index_name_;                    // 索引标识

    /**
     * @brief 扫描状态维护
     * @note 控制扫描进度
     */
    Rid rid_;                        // 当前记录ID
    std::unique_ptr<IxScan> scan_;  // 扫描迭代器

    /**
     * @brief 系统组件访问
     * @note 提供系统服务调用
     */
    SmManager *sm_manager_;  // 系统管理器

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
                assert(!(cond.rhs_type == ConditionRhsType::RHS_EXPR) &&
                       !(cond.rhs_type == ConditionRhsType::RHS_VALUE) && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    /**
     * @brief 初始化索引扫描并定位第一条记录
     * @throw InternalError 当索引访问失败时

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
                // 只使用右侧为常量值的条件来确定索引边界
                if (cond.lhs_col.col_name == col.name && cond.rhs_type == ConditionRhsType::RHS_VALUE) {
                    switch (cond.op) {
                        case CompOp::OP_EQ: {
                            // 因为 rhs_type == RHS_VALUE，所以可以直接使用 cond.rhs_val
                            if (Value::compare(cond.rhs_val, min_val, CompOp::OP_GT)) {
                                min_val = cond.rhs_val;
                            }
                            if (Value::compare(cond.rhs_val, max_val, CompOp::OP_LT)) {
                                max_val = cond.rhs_val;
                            }
                            break;
                        }

                        case CompOp::OP_LT:
                        case CompOp::OP_LE: {
                            if (Value::compare(cond.rhs_val, max_val, CompOp::OP_LT)) {
                                max_val = cond.rhs_val;
                            }
                            break;
                        }

                        case CompOp::OP_GT:
                        case CompOp::OP_GE: {
                            if (Value::compare(cond.rhs_val, min_val, CompOp::OP_GT)) {
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
     * @brief 移动到下一条满足条件的记录
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
        if(scan_->is_end()) {
            scan_->unlatch();
        }
    }

    /**
     * @brief 检查索引扫描是否结束
     * @return true表示扫描完成，false表示还有记录
     */
    bool is_end() const override { return scan_ == nullptr || scan_->is_end(); }

    /**
     * @brief 获取当前扫描位置的记录
     * @return 记录的智能指针
     * @throw InternalError 当记录访问失败
     */
    std::unique_ptr<RmRecord> Next() override { return fh_->get_record(rid_, context_); }

    /**
     * @brief 获取记录的物理长度
     * @return 记录的总字节数
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
};