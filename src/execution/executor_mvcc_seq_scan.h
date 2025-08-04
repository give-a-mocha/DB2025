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

#include "execution/execution_common.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

extern SmManager sm_manager;
extern TransactionManager txn_manager;

/**
 * @brief 顺序扫描执行器，负责实现表的全表顺序扫描功能
 */
class MvccSeqScanExecutor : public AbstractExecutor {
   private:
    TabMeta &tab_;                   // 表的元数据
    std::vector<Condition> conds_;   // 过滤条件列表
    RmFileHandle *fh_;               // 表文件句柄
    size_t len_;                     // 记录总长度(字节)
    Rid rid_;                        // 当前记录的RID
    std::unique_ptr<RecScan> scan_;  // 表扫描迭代器
    std::unique_ptr<RmRecord> rec_;  // 当前记录的智能指针
    TupleMeta tuple_meta_;           // 元组元数据，用于存储列信息

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     * @param tab_name 要扫描的表名
     * @param conds 过滤条件列表
     * @param context 执行上下文
     */
    MvccSeqScanExecutor(std::string tab_name, std::vector<Condition> conds, Context *context):tab_(sm_manager.db_.get_table(tab_name)) {
        TRACE_FUNCTION
        conds_ = std::move(conds);
        fh_ = sm_manager.fhs_.at(tab_name).get();
        len_ = tab_.cols.back().offset + tab_.cols.back().len;
        context_ = context;
    }

    /**
     * @brief 初始化扫描并定位第一条符合条件的记录
     */
    void beginTuple() override {
        TRACE_FUNCTION
        // 创建扫描迭代器
        scan_ = std::make_unique<RmScan>(fh_);

        // 查找第一个满足条件的记录
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto [base_meta, base_tuple, link] = txn_manager.GetTupleAndUndoLink(fh_, rid_);
            auto undologs = txn_manager.CollectUndoLogs(rid_, link, context_->txn_);
            auto rec = ReconstructTuple(std::move(base_tuple), base_meta, undologs);
            if (rec != nullptr && eval_conds(tab_.cols, conds_, rec)) {
                rec_ = std::move(rec);
                tuple_meta_ = base_meta;
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
        TRACE_FUNCTION
        // 检查扫描器状态
        if (scan_ == nullptr) {
            throw InternalError("Scan not initialized at " + getType());
        }

        // 移动扫描位置
        if (!scan_->is_end()) {
            scan_->next();
        }

        // 查找下一个满足条件的记录
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto [base_meta, base_tuple, link] = txn_manager.GetTupleAndUndoLink(fh_, rid_);
            auto undologs = txn_manager.CollectUndoLogs(rid_, link, context_->txn_);
            auto rec = ReconstructTuple(std::move(base_tuple), base_meta, undologs);
            if (rec != nullptr && eval_conds(tab_.cols, conds_, rec)) {
                rec_ = std::move(rec);
                tuple_meta_ = base_meta;
                return;
            }
            scan_->next();
        }
    }

    /**
     * @brief 检查扫描是否结束
     * @return true表示扫描结束，false表示还有记录
     */
    bool is_end() const override { return scan_ == nullptr || scan_->is_end(); }

    /**
     * @brief 获取当前记录的数据
     * @return 记录的智能指针，扫描结束时返回nullptr
     */
    std::unique_ptr<RmRecord> Next() override {
        TRACE_FUNCTION
        return std::move(rec_);
    }

    /**
     * @brief 获取记录的总长度
     * @return 记录长度(字节数)
     *
     * @note 包含所有字段的总长度，用于内存分配
     */
    size_t tupleLen() const override { return len_; }

    /**
     * @brief 获取输出列的元数据
     * @return 列元数据向量的常量引用
     *
     * @note 包含列的类型、长度、偏移等信息
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
    std::string getType() override { return "MvccSeqScanExecutor"; }
};