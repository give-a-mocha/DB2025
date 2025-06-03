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
#include "system/sm.h"
#include "transaction/transaction_manager.h"

class Planner;

/**
 * @brief 查询语言管理器类，负责执行SQL查询和管理查询执行过程
 *
 * 主要功能：
 * 1. 执行SQL查询和DML操作
 * 2. 管理查询执行计划
 * 3. 协调事务处理
 * 4. 处理实用工具命令
 *
 * 组件交互：
 * - 系统管理器(SmManager): 管理表和系统元数据
 * - 事务管理器(TransactionManager): 处理事务相关操作
 * - 查询计划器(Planner): 生成和优化查询执行计划
 */
class QlManager {
   private:
    SmManager *sm_manager_;        // 系统管理器指针
    TransactionManager *txn_mgr_;  // 事务管理器指针
    Planner *planner_;             // 查询计划器指针

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     * @param txn_mgr 事务管理器指针
     * @param planner 查询计划器指针
     */
    QlManager(SmManager *sm_manager, TransactionManager *txn_mgr, Planner *planner)
        : sm_manager_(sm_manager), txn_mgr_(txn_mgr), planner_(planner) {}

    /**
     * @brief 执行多查询操作
     * @param plan 查询计划
     * @param context 执行上下文
     */
    void run_mutli_query(std::shared_ptr<Plan> plan, Context *context);

    /**
     * @brief 执行实用工具命令
     * @param plan 命令执行计划
     * @param txn_id 事务ID指针
     * @param context 执行上下文
     */
    void run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context);

    /**
     * @brief 执行SELECT查询
     * @param executorTreeRoot 执行器树的根节点
     * @param sel_cols 选择的列
     * @param context 执行上下文
     */
    void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                     Context *context);

    /**
     * @brief 执行DML操作(INSERT/UPDATE/DELETE)
     * @param exec DML执行器
     */
    void run_dml(std::unique_ptr<AbstractExecutor> exec);

    /**
     * @brief 执行EXPLAIN命令，展示查询执行计划
     * @param executorTreeRoot 执行器树的根节点
     * @param sel_cols 选择的列
     * @param context 执行上下文
     */
    void run_explain(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                     Context *context);
};
