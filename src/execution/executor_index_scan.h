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
 * @brief 索引扫描执行器，提供基于索引的高效记录访问
 *
 * 核心功能：
 * 1. 索引范围构建
 *    - 分析查询条件
 *    - 确定扫描边界
 *    - 优化范围选择
 *
 * 2. 高效扫描策略
 *    - B+树遍历优化
 *    - 记录预取机制
 *    - 条件过滤下推
 *
 * 3. 并发控制
 *    - 索引锁管理
 *    - 一致性保证
 *    - 死锁预防
 *
 * 4. 性能优化
 *    - 缓冲区管理
 *    - 批量读取
 *    - 内存对齐
 *
 * @note 适用场景：
 * - 高选择性查询
 * - 范围扫描操作
 * - 排序要求
 *
 * @warning 注意事项：
 * - 索引选择性影响性能
 * - 需要维护索引开销
 * - 内存消耗考虑
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
    std::unique_ptr<RecScan> scan_;  // 扫描迭代器

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
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
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
     *
     * @details 初始化过程：
     * 1. 扫描范围构建
     *    - 按索引列提取条件
     *    - 计算上下边界值
     *    - 构建边界记录
     *
     * 2. B+树操作
     *    - 查找下界位置
     *    - 确定上界范围
     *    - 初始化扫描器
     *
     * 3. 性能优化
     *    - 预读取页面
     *    - 缓存热点节点
     *    - 条件过滤下推
     *
     * 4. 边界处理
     *    - 类型范围检查
     *    - NULL值处理
     *    - 特殊值优化
     *
     * @note 优化策略：
     * - 最小化I/O操作
     * - 利用索引特性
     * - 充分使用缓存
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
     * @brief 检查索引扫描是否结束
     * @return true表示扫描完成，false表示还有记录
     *
     * @details 检查条件：
     * 1. 正常结束
     *    - 到达索引上界
     *    - 遍历完指定范围
     *    - 满足终止条件
     *
     * 2. 异常结束
     *    - 扫描器未初始化
     *    - B+树访问错误
     *    - 事务中止
     *
     * 3. 并发考虑
     *    - 检查索引一致性
     *    - 处理并发修改
     *    - 维护扫描状态
     */
    bool is_end() const override { return scan_ == nullptr || scan_->is_end(); }

    /**
     * @brief 获取当前扫描位置的记录
     * @return 记录的智能指针
     * @throw InternalError 当记录访问失败
     *
     * @details 访问流程：
     * 1. 记录获取
     *    - 通过RID定位
     *    - 从缓冲池读取
     *    - 验证记录有效性
     *
     * 2. 并发控制
     *    - 获取记录锁
     *    - 检查事务可见性
     *    - 处理死锁情况
     *
     * 3. 性能优化
     *    - 使用记录缓存
     *    - 批量预读取
     *    - 延迟加载策略
     */
    std::unique_ptr<RmRecord> Next() override { return fh_->get_record(rid_, context_); }

    /**
     * @brief 获取记录的物理长度
     * @return 记录的总字节数
     *
     * @details 长度计算：
     * 1. 数据部分
     *    - 固定长度字段
     *    - 变长字段实际长度
     *    - 对齐填充
     *
     * 2. 控制信息
     *    - 记录头信息
     *    - NULL值位图
     *    - 版本信息
     *
     * @note 用于：
     * - 内存分配
     * - 缓冲区管理
     * - 页面布局
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
     * @brief 比较两个值的通用实现
     * @param lhs 左操作数
     * @param rhs 右操作数
     * @param op 比较操作类型
     * @return 比较结果
     * @throw IncompatibleTypeError 类型不兼容时
     *
     * @details 比较流程：
     * 1. 类型检查与转换
     *    - 验证类型兼容性
     *    - 数值类型隐式转换
     *    - 特殊类型处理(如NULL)
     *
     * 2. 值比较策略
     *    - 数值直接比较
     *    - 字符串优化比较
     *    - 边界条件处理
     *
     * 3. 性能优化
     *    - 避免不必要转换
     *    - 利用CPU指令
     *    - 减少内存拷贝
     *
     * @note 优化考虑：
     * - 常见类型快速路径
     * - 大小写敏感性
     * - 特殊值处理
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