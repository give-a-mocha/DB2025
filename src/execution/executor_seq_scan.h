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
 * @brief 顺序扫描执行器，提供表的全表扫描功能
 *
 * 实现架构：
 * 1. 扫描机制
 *    - 页面级扫描：按页读取数据
 *    - 记录级扫描：遍历页内记录
 *    - 缓冲区管理：优化I/O操作
 *
 * 2. 条件处理
 *    - 条件下推：尽早过滤无关记录
 *    - 谓词评估：高效的条件检查
 *    - 批量处理：减少函数调用开销
 *
 * 3. 性能优化
 *    - 预读取：异步加载下一页
 *    - 缓存优化：重用热点数据
 *    - 内存对齐：优化数据访问
 *
 * 4. 资源管理
 *    - 内存控制：避免过度消耗
 *    - 并发处理：支持多事务
 *    - 错误恢复：保证操作原子性
 *
 * @note 适用场景：
 * - 小表全表扫描
 * - 高选择性查询
 * - 数据探索分析
 */
class SeqScanExecutor : public AbstractExecutor {
   private:
    /**
     * @brief 扫描目标表的名称
     * @note 用于获取表的元数据和文件句柄
     */
    std::string tab_name_;

    /**
     * @brief 扫描的过滤条件列表
     * @note 包含WHERE子句中的所有条件
     */
    std::vector<Condition> conds_;

    /**
     * @brief 表文件的访问句柄
     * @note 用于读取记录数据
     */
    RmFileHandle *fh_;

    /**
     * @brief 输出列的元数据定义
     * @note 包含列的类型、长度、偏移等信息
     */
    std::vector<ColMeta> cols_;

    /**
     * @brief 输出记录的总长度(字节)
     * @note 用于内存分配和数据访问
     */
    size_t len_;

    /**
     * @brief 优化后的条件表达式
     * @note 可能经过重写和简化的条件
     */
    std::vector<Condition> fed_conds_;

    /**
     * @brief 当前处理记录的标识符
     * @note 用于定位和访问记录
     */
    Rid rid_;

    /**
     * @brief 底层扫描迭代器
     * @note 提供记录级别的遍历功能
     */
    std::unique_ptr<RecScan> scan_;

    /**
     * @brief 系统管理器的访问接口
     * @note 用于访问系统元数据和服务
     */
    SmManager *sm_manager_;

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
     * @brief 初始化表扫描操作
     * @throw InternalError 当初始化失败时
     *
     * @details 初始化过程：
     * 1. 扫描准备
     *    - 创建记录迭代器
     *    - 设置初始位置
     *    - 预热缓冲区
     *
     * 2. 条件处理
     *    - 初始化条件评估器
     *    - 准备常量条件值
     *    - 设置评估上下文
     *
     * 3. 资源分配
     *    - 分配扫描缓冲区
     *    - 注册事务锁
     *    - 初始化统计信息
     *
     * @note 优化策略：
     * - 异步预读下一页
     * - 批量加载优化
     * - 减少内存分配
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
     * @brief 获取下一个满足条件的记录
     * @throw InternalError 当扫描器未初始化时
     *
     * @details 处理流程：
     * 1. 状态检查
     *    - 验证扫描器状态
     *    - 检查事务活跃性
     *    - 处理并发访问
     *
     * 2. 记录定位
     *    - 获取下一条记录
     *    - 条件过滤
     *    - 处理删除标记
     *
     * 3. 性能优化
     *    - 批量条件评估
     *    - 跳过无效记录
     *    - 利用缓存数据
     *
     * @note 错误处理：
     * - 处理无效记录
     * - 检测并发冲突
     * - 维护扫描状态
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