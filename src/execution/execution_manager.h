/**
 * @file execution_manager.h
 * @author RMDB Development Team
 * @brief 查询执行管理器的实现
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * 执行引擎架构：
 * 1. 执行算子
 *    - 表扫描：顺序扫描、索引扫描
 *    - 连接：嵌套循环、排序合并
 *    - 聚合：分组、排序、聚集函数
 *    - 修改：插入、更新、删除
 *
 * 2. 执行模型
 *    - 火山模型：迭代器接口
 *    - 流水线并行
 *    - 向量化执行
 *
 * 3. 资源管理
 *    - 内存分配与回收
 *    - 缓冲区管理
 *    - 并发控制
 *
 * 4. 异常处理
 *    - 执行错误恢复
 *    - 事务回滚
 *    - 死锁检测
 */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/common.h"
#include "common/context.h"
#include "execution_defs.h"
#include "executor_abstract.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "record/rm.h"
#include "recovery/log_recovery.h"
#include "system/sm.h"
#include "transaction/transaction_manager.h"
#include "common/TraceStack.hpp"

extern SmManager sm_manager;
extern TransactionManager txn_manager;
extern RecoveryManager recovery_manager;

class Planner;

/**
 * @brief 查询执行管理器类
 */
class QlManager {
   public:
    QlManager() = default;

    /**
     * @brief 执行多语句查询
     * @param plan 复合查询计划
     * @param context 执行上下文
     * @throw ExecutionError 当执行出错时
     */
    void run_mutli_query(std::shared_ptr<Plan> plan, Context *context);

    /**
     * @brief 执行数据库工具命令
     * @param plan 工具命令执行计划
     * @param txn_id 事务ID指针
     * @param context 执行上下文
     * @throw CommandError 当命令执行失败时
     */
    void run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context);

    /**
     * @brief 执行SELECT查询语句
     * @param executorTreeRoot 执行器树根节点
     * @param sel_cols 需要选择的列
     * @param context 执行上下文
     * @throw ExecutionError 当查询执行失败时
     */
    void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                     Context *context);

    /**
     * @brief 执行DML语句
     * @param exec DML执行器
     * @throw ExecutionError 当执行失败时
     */
    void run_dml(std::unique_ptr<AbstractExecutor> exec);

    /**
     * @brief 执行EXPLAIN命令，分析并展示查询执行计划
     * @param executorTreeRoot 执行器树根节点
     * @param sel_cols 查询涉及的列
     * @param context 执行上下文
     * @throw ExecutionError 当分析失败时
     */
    void run_explain(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                     Context *context);
};
