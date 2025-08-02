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

extern SmManager sm_manager;
extern TransactionManager txn_manager;

/**
 * @brief 索引扫描执行器，负责实现基于索引的高效记录访问
 */
class MvccIndexScanExecutor : public AbstractExecutor {
   private:
    TabMeta &tab_;                  // 表元数据
    std::vector<Condition> conds_;  // 原始条件
    RmFileHandle *fh_;              // 表文件句柄
    IxIndexHandle *ih_;             // 索引句柄
    size_t len_;                    // 记录长度
    IndexMeta &index_meta_;         // 索引元数据
    Rid rid_;                       // 当前记录ID
    // std::unique_ptr<IxScan> scan_;  // 扫描迭代器
    std::unique_ptr<IxScanFinal> scan_;
    // SmManager *sm_manager_;  // 系统管理器
    // TransactionManager *txn_mgr_;
    std::unique_ptr<RmRecord> rec_;  // 当前记录
    TupleMeta tuple_meta_;           // 元组元数据
    Iid lower_iid;                   // 索引下界
    Iid upper_iid;                   // 索引上界

   public:
    MvccIndexScanExecutor(std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                          Context *context)
        : tab_(sm_manager.db_.get_table(tab_name)),
          conds_(std::move(conds)),
          fh_(sm_manager.fhs_.at(tab_name).get()),
          ih_(sm_manager.ihs_.at(sm_manager.get_index_name(tab_name, index_col_names)).get()),
          len_(tab_.cols.back().offset + tab_.cols.back().len),
          index_meta_(*tab_.get_index_meta(index_col_names)),
          rid_(),
          scan_(nullptr),
          rec_(nullptr) {
        TRACE_FUNCTION
        context_ = context;  // Initialize context_ in the constructor body
        //! 先留着按道理应该在plan部分被调整顺序
        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_.name) {
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op(cond.op);
            }
        }
        getBound();  // 获取索引的边界
    }

    /**
     * @brief 析构函数，确保正确释放扫描器资源
     */
    ~MvccIndexScanExecutor() {
        // if (scan_) {
        //     scan_->unlatch();  // 无论扫描是否结束，都释放持有的锁
        // }
    }

    void getBound() {
        // 从条件中提取索引键的范围
        RmRecord lower_record(index_meta_.col_tot_len), upper_record(index_meta_.col_tot_len);
        off_t offset = 0;

        for (const auto &col : index_meta_.cols) {
            Value max_val, min_val;
            switch (col.type) {
                case ColType::TYPE_INT: {
                    max_val.set(std::numeric_limits<int>::max());
                    min_val.set(std::numeric_limits<int>::min());
                    max_val.init_raw(sizeof(int)), min_val.init_raw(sizeof(int));
                    break;
                }
                case ColType::TYPE_FLOAT: {
                    max_val.set(std::numeric_limits<float>::max());
                    min_val.set(std::numeric_limits<float>::lowest());
                    max_val.init_raw(sizeof(float)), min_val.init_raw(sizeof(float));
                    break;
                }
                case ColType::TYPE_STRING: {
                    max_val.set(std::string(col.len, 255));
                    min_val.set(std::string(col.len, 0));
                    max_val.init_raw(col.len), min_val.init_raw(col.len);
                    break;
                }
                default:
                    throw InternalError("Unsupported column type in index scan");
            }
            for (const auto &cond : conds_) {
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
        lower_iid = ih_->lower_bound(lower_record.data);
        upper_iid = ih_->upper_bound(upper_record.data);
    }

    /**
     * @brief 初始化索引扫描并定位第一条记录
     * @throw InternalError 当索引访问失败时

     */
    void beginTuple() override {
        TRACE_FUNCTION
        // scan_ = std::make_unique<IxScan>(ih_, lower_iid, upper_iid, sm_manager_->get_bpm());
        if (scan_) {
            scan_->reset();  // 如果扫描器已存在，重置游标
        } else {
            scan_ = std::make_unique<IxScanFinal>(ih_, lower_iid, upper_iid);
        }
        // 移动到第一个满足条件的记录
        while (!is_end()) {
            rid_ = scan_->rid();
            auto [base_meta, base_tuple, link] = txn_manager.GetTupleAndUndoLink(fh_, rid_);
            auto undologs = txn_manager.CollectUndoLogs(rid_, link, context_->txn_);
            auto rec = ReconstructTuple(std::move(base_tuple), base_meta, undologs);
            if (rec != nullptr && eval_conds(tab_.cols, conds_, rec)) {
                rec_ = std::move(rec);
                tuple_meta_ = base_meta;  // 设置元数据
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
        if (!scan_->is_end()) {
            scan_->next();
        }
        // 移动到下一个满足条件的记录
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto [base_meta, base_tuple, link] = txn_manager.GetTupleAndUndoLink(fh_, rid_);
            auto undologs = txn_manager.CollectUndoLogs(rid_, link, context_->txn_);
            auto rec = ReconstructTuple(std::move(base_tuple), base_meta, undologs);
            if (rec != nullptr && eval_conds(tab_.cols, conds_, rec)) {
                rec_ = std::move(rec);
                tuple_meta_ = base_meta;  // 更新元数据
                return;
            }
            scan_->next();
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
    std::unique_ptr<RmRecord> Next() override { return std::move(rec_); }

    /**
     * @brief 获取记录的物理长度
     * @return 记录的总字节数
     */
    size_t tupleLen() const override { return len_; }

    /**
     * @brief 获取扫描涉及的所有列元数据
     * @return 列元数据向量的常量引用
     */
    const std::vector<ColMeta> &cols() const override { return tab_.cols; }

    /**
     * @brief 获取当前记录的RID
     * @return 当前记录的RID引用
     */
    Rid &rid() override { return rid_; }

    TupleMeta &tuple_meta() override { return tuple_meta_; }

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    std::string getType() override { return "IndexScanExecutor"; }
};