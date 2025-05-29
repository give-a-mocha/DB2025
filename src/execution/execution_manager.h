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

#include "execution_defs.h"
#include "record/rm.h"
#include "system/sm.h"
#include "common/context.h"
#include "common/common.h"
#include "optimizer/plan.h"
#include "executor_abstract.h"
#include "transaction/transaction_manager.h"
#include "optimizer/planner.h"

class Planner;

/**
 * @brief 查询执行管理器
 *
 * QlManager是RMDB查询执行引擎的核心管理类，负责协调不同类型SQL语句的执行。
 * 它接收来自Portal的执行器树，根据语句类型选择合适的执行策略，并管理执行过程中的
 * 资源分配、事务控制、结果输出等任务。
 *
 * 主要职责：
 * - DDL语句执行：通过sm_manager_执行表和索引的创建删除操作
 * - DML语句执行：协调各种数据操作语句的执行
 * - SELECT查询执行：管理查询结果的迭代输出和格式化
 * - 事务管理：配合txn_mgr_进行事务的开始、提交、回滚
 * - 系统命令执行：处理HELP、SHOW等工具命令
 */
class QlManager {
   private:
    SmManager *sm_manager_;          ///< 系统管理器，负责元数据和存储管理
    TransactionManager *txn_mgr_;    ///< 事务管理器，负责事务控制
    Planner *planner_;               ///< 查询计划器，用于动态参数设置

   public:
    /**
     * @brief 构造查询执行管理器
     *
     * @param sm_manager 系统管理器实例
     * @param txn_mgr 事务管理器实例
     * @param planner 查询计划器实例
     */
    QlManager(SmManager *sm_manager, TransactionManager *txn_mgr, Planner *planner)
        : sm_manager_(sm_manager),  txn_mgr_(txn_mgr), planner_(planner) {}

    /**
     * @brief 执行DDL语句（数据定义语言）
     *
     * 处理CREATE TABLE、DROP TABLE、CREATE INDEX、DROP INDEX等DDL操作。
     * 这些操作直接修改数据库的元数据结构，具有原子性特征。
     *
     * @param plan DDL执行计划，包含操作类型和参数
     * @param context 执行上下文，包含事务和缓冲区信息
     */
    void run_mutli_query(std::shared_ptr<Plan> plan, Context *context);
    
    /**
     * @brief 执行工具命令和事务控制语句
     *
     * 处理HELP、SHOW TABLES、DESC TABLE等信息查询命令，
     * 以及BEGIN、COMMIT、ROLLBACK等事务控制命令，
     * 还包括SET命令用于动态调整系统参数。
     *
     * @param plan 命令执行计划
     * @param txn_id 事务ID指针，用于事务操作
     * @param context 执行上下文
     */
    void run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context);
    
    /**
     * @brief 执行SELECT查询并输出结果
     *
     * 这是SELECT语句执行的核心函数，负责：
     * 1. 迭代执行器树获取查询结果
     * 2. 格式化输出到客户端缓冲区
     * 3. 同步输出到output.txt文件
     * 4. 统计和报告查询结果数量
     *
     * @param executorTreeRoot 执行器树的根节点
     * @param sel_cols 选择的列信息，用于生成表头
     * @param context 执行上下文，包含输出缓冲区
     */
    void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                        Context *context);

    /**
     * @brief 执行DML语句（数据操作语言）
     *
     * 处理INSERT、UPDATE、DELETE等不返回结果集的DML操作。
     * 这些操作只需调用执行器的Next()方法完成数据修改，
     * 不需要格式化输出结果。
     *
     * @param exec DML执行器，封装了具体的数据操作逻辑
     */
    void run_dml(std::unique_ptr<AbstractExecutor> exec);
    void run_explain(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols, Context *context);
};
