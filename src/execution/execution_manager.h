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
#include "system/sm.h"
#include "transaction/transaction_manager.h"

class Planner;

/**
 * @brief 查询执行管理器类
 *
 * 查询执行生命周期：
 * 1. 计划接收与验证
 *    - 验证计划的合法性
 *    - 检查资源可用性
 *    - 初始化执行上下文
 *
 * 2. 算子树构建
 *    - 创建执行算子实例
 *    - 建立算子间连接
 *    - 设置算子参数
 *
 * 3. 查询执行
 *    - 驱动算子树执行
 *    - 处理中间结果
 *    - 管理执行状态
 *
 * 4. 资源管理
 *    - 内存分配与释放
 *    - 临时结果管理
 *    - 并发控制
 *
 * 5. 结果处理
 *    - 收集查询结果
 *    - 格式化输出
 *    - 错误处理
 *
 * 6. 清理和恢复
 *    - 释放资源
 *    - 回滚失败操作
 *    - 维护系统状态
 */
class QlManager {
   private:
    /**
     * @brief 系统管理器
     * @note 负责管理：
     * - 表和索引的元数据
     * - 系统目录
     * - 权限控制
     */
    SmManager *sm_manager_;

    /**
     * @brief 事务管理器
     * @note 负责：
     * - 事务的创建和提交
     * - 并发控制
     * - 日志管理
     */
    TransactionManager *txn_mgr_;

    /**
     * @brief 查询计划器
     * @note 负责：
     * - 生成执行计划
     * - 优化查询
     * - 代价估算
     */
    Planner *planner_;

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
     * @brief 执行多语句查询
     * @param plan 复合查询计划
     * @param context 执行上下文
     * @throw ExecutionError 当执行出错时
     *
     * @details 执行过程：
     * 1. 语句分析
     *    - 解析语句依赖关系
     *    - 确定执行顺序
     *    - 建立执行环境
     *
     * 2. 并发控制
     *    - 获取必要的锁
     *    - 维护事务状态
     *    - 处理死锁风险
     *
     * 3. 执行管理
     *    - 逐条执行语句
     *    - 维护中间状态
     *    - 处理执行异常
     *
     * @note 多语句执行需要保证：
     * - 语句间的一致性
     * - 原子性（全部成功或全部失败）
     * - 正确的错误处理和恢复
     */
    void run_mutli_query(std::shared_ptr<Plan> plan, Context *context);

    /**
     * @brief 执行数据库工具命令
     * @param plan 工具命令执行计划
     * @param txn_id 事务ID指针
     * @param context 执行上下文
     * @throw CommandError 当命令执行失败时
     *
     * @details 支持的命令：
     * 1. 数据定义
     *    - SHOW TABLES
     *    - DESCRIBE TABLE
     *    - SHOW INDEX
     *
     * 2. 事务控制
     *    - BEGIN
     *    - COMMIT
     *    - ROLLBACK
     *
     * 3. 系统管理
     *    - EXPLAIN
     *    - SET VARIABLE
     *    - HELP
     *
     * @note 工具命令通常：
     * - 不修改数据
     * - 需要特殊权限
     * - 有特定的输出格式
     */
    void run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context);

    /**
     * @brief 执行SELECT查询语句
     * @param executorTreeRoot 执行器树根节点
     * @param sel_cols 需要选择的列
     * @param context 执行上下文
     * @throw ExecutionError 当查询执行失败时
     *
     * @details 查询执行流程：
     * 1. 准备阶段
     *    - 初始化执行器树
     *    - 分配执行资源
     *    - 设置结果收集器
     *
     * 2. 执行阶段
     *    - 迭代处理结果
     *    - 应用选择条件
     *    - 处理投影列表
     *
     * 3. 结果处理
     *    - 格式化输出
     *    - 统计信息收集
     *    - 资源释放
     *
     * @note 性能优化：
     * - 使用火山模型
     * - 支持管道并行
     * - 优化内存使用
     */
    void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                     Context *context);

    /**
     * @brief 执行DML语句
     * @param exec DML执行器
     * @throw ExecutionError 当执行失败时
     *
     * @details 支持操作：
     * 1. INSERT
     *    - 单行插入
     *    - 批量插入
     *    - 带子查询插入
     *
     * 2. UPDATE
     *    - 条件更新
     *    - 批量更新
     *    - 索引维护
     *
     * 3. DELETE
     *    - 条件删除
     *    - 批量删除
     *    - 级联删除
     *
     * @note 事务保证：
     * - 原子性
     * - 一致性
     * - 隔离性
     * - 持久性
     */
    void run_dml(std::unique_ptr<AbstractExecutor> exec);

    /**
     * @brief 执行EXPLAIN命令，分析并展示查询执行计划
     * @param executorTreeRoot 执行器树根节点
     * @param sel_cols 查询涉及的列
     * @param context 执行上下文
     * @throw ExecutionError 当分析失败时
     *
     * @details 分析输出内容：
     * 1. 执行计划结构
     *    - 算子类型和参数
     *    - 算子依赖关系
     *    - 执行顺序说明
     *
     * 2. 访问方法分析
     *    - 表扫描方式选择
     *    - 索引使用计划
     *    - 连接算法选择
     *
     * 3. 性能估算信息
     *    - 预计处理行数
     *    - I/O消耗估算
     *    - CPU代价估算
     *    - 内存使用预估
     *
     * 4. 优化决策说明
     *    - 选用索引的原因
     *    - 连接顺序的选择
     *    - 优化器的关键决策
     *
     * @note 用途：
     * - 帮助开发人员理解执行计划
     * - 诊断性能问题
     * - 验证优化器决策
     */
    void run_explain(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                     Context *context);
};
