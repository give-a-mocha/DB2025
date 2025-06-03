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
 * @brief 顺序扫描执行器，实现对表的顺序扫描
 *
 * 主要功能：
 * 1. 按顺序扫描表中的所有记录
 * 2. 根据条件过滤记录
 * 3. 提供迭代器接口访问满足条件的记录
 *
 * 实现策略：
 * - 使用表扫描迭代器遍历所有记录
 * - 使用条件评估器过滤记录
 * - 维护当前记录的位置信息
 */
class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 要扫描的表名
    std::vector<Condition> conds_;      // 扫描的过滤条件
    RmFileHandle *fh_;                  // 表文件句柄
    std::vector<ColMeta> cols_;         // 输出记录的字段元数据
    size_t len_;                        // 输出记录的总长度
    std::vector<Condition> fed_conds_;  // 传递给条件评估器的条件

    Rid rid_;                        // 当前记录的RID
    std::unique_ptr<RecScan> scan_;  // 表扫描迭代器
    SmManager *sm_manager_;          // 系统管理器指针

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     * @param tab_name 要扫描的表名
     * @param conds 扫描条件
     * @param context 执行上下文
     */
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

    /**
     * @brief 开始扫描操作
     *
     * 初始化扫描迭代器并定位到第一个满足条件的记录
     */
    void beginTuple() override {
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

    /**
     * @brief 移动到下一个满足条件的记录
     *
     * 继续扫描直到找到下一个满足所有条件的记录
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
     * @brief 检查是否已扫描到表末尾
     * @return 如果扫描完成返回true，否则返回false
     */
    bool is_end() const override { return scan_ == nullptr || scan_->is_end(); }

    /**
     * @brief 获取当前记录
     * @return 返回当前记录的指针，如果已到达末尾则返回nullptr
     */
    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return fh_->get_record(rid_, context_);
    }

    /**
     * @brief 获取记录的长度
     * @return 返回记录的总字节数
     */
    size_t tupleLen() const override { return len_; }

    /**
     * @brief 获取输出字段的元数据
     * @return 返回字段元数据的向量引用
     */
    const std::vector<ColMeta> &cols() const override { return cols_; }

    /**
     * @brief 获取当前记录的RID
     * @return 返回当前记录的RID引用
     */
    Rid &rid() override { return rid_; }

    /**
     * @brief 获取执行器类型
     * @return 返回执行器的类型字符串
     */
    std::string getType() override { return "SeqScanExecutor"; }
};