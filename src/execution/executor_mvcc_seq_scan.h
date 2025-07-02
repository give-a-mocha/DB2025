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

/**
 * @brief 顺序扫描执行器，负责实现表的全表顺序扫描功能
 */
class MvccSeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 扫描的表名
    std::vector<Condition> conds_;      // 过滤条件列表
    RmFileHandle *fh_;                  // 表文件句柄
    std::vector<ColMeta> cols_;         // 输出列的元数据
    size_t len_;                        // 记录总长度(字节)
    std::vector<Condition> fed_conds_;  // 优化后的条件
    Rid rid_;                           // 当前记录的RID
    std::unique_ptr<RecScan> scan_;     // 表扫描迭代器
    SmManager *sm_manager_;             // 系统管理器指针
    TransactionManager *txn_mgr_;       // 事务管理器指针

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     * @param tab_name 要扫描的表名
     * @param conds 过滤条件列表
     * @param context 执行上下文
     */
    MvccSeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context,
                        TransactionManager *txn_mgr) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);

        // 获取表信息
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;

        // 计算记录长度
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;
        txn_mgr_ = txn_mgr;
        fed_conds_ = conds_;  // 暂未优化的条件列表
    }

    /**
     * @brief 初始化扫描并定位第一条符合条件的记录
     */
    void beginTuple() override {
        // 创建扫描迭代器
        scan_ = std::make_unique<RmScan>(fh_);

        // 查找第一个满足条件的记录
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            std::unique_ptr<RmRecord> rec = mvcc_get_record(rid_, context_, fh_, txn_mgr_, cols_);
            if (rec != nullptr && eval_conds(cols_, fed_conds_, rec)) {
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
            std::unique_ptr<RmRecord> rec = mvcc_get_record(rid_, context_, fh_, txn_mgr_, cols_);
            if (rec != nullptr && eval_conds(cols_, fed_conds_, rec)) {
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
    std::unique_ptr<RmRecord> Next() override { return mvcc_get_record(rid_, context_, fh_, txn_mgr_, cols_); }

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
    const std::vector<ColMeta> &cols() const override { return cols_; }

    /**
     * @brief 获取当前记录的RID
     * @return 当前记录的RID引用
     */
    Rid &rid() override { return rid_; }

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    std::string getType() override { return "MvccSeqScanExecutor"; }
};