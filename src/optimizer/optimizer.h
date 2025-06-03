/**
 * @file optimizer.h
 * @author RMDB Development Team
 * @brief 查询优化器的实现
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * 查询优化器的主要功能：
 * 1. 查询计划生成
 *    - 解析查询语句的类型
 *    - 生成对应的执行计划
 *    - 处理各类DDL和DML语句
 *
 * 2. 特殊语句处理
 *    - 系统命令(HELP, SHOW等)
 *    - 事务控制语句
 *    - 元数据操作语句
 *
 * 3. 错误处理
 *    - 语句类型检查
 *    - 参数验证
 *    - 异常处理机制
 */

#pragma once

#include <map>

#include "errors.h"
#include "execution/execution.h"
#include "parser/parser.h"
#include "system/sm.h"
#include "common/context.h"
#include "transaction/transaction_manager.h"
#include "planner.h"
#include "plan.h"
#include "common/print.hpp"

/**
 * @brief 查询优化器类
 *
 * 负责功能：
 * 1. 语句分类处理
 *    - DDL语句(CREATE, DROP等)
 *    - DML语句(SELECT, INSERT等)
 *    - 系统命令(SHOW, DESC等)
 *    - 事务控制语句
 *
 * 2. 计划生成
 *    - 根据语句类型选择合适的计划
 *    - 调用Planner生成具体执行计划
 *    - 处理元数据相关操作
 *
 * 3. 系统管理
 *    - 与系统管理器交互
 *    - 处理事务相关命令
 *    - 维护系统状态
 */
class Optimizer {
   private:
    SmManager *sm_manager_;        // 系统管理器指针
    Planner *planner_;            // 计划生成器指针

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     * @param planner 计划生成器指针
     * @throw InvalidPointerError 如果任一指针为空
     */
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     * @param planner 计划生成器指针
     * @throw InternalError 如果任一指针为空
     */
    Optimizer(SmManager *sm_manager, Planner *planner)
        : sm_manager_(sm_manager), planner_(planner) {
        if (!sm_manager || !planner) {
            throw InternalError("Null pointer in Optimizer constructor");
        }
    }
    
    /**
     * @brief 为查询生成执行计划
     * @param query 查询对象
     * @param context 执行上下文
     * @return 生成的执行计划
     * @throw PlanError 当计划生成失败时
     *
     * @details 处理流程：
     * 1. 系统命令处理
     *    - HELP: 显示帮助信息
     *    - SHOW TABLES: 显示所有表
     *    - SHOW INDEX: 显示指定表的索引
     *    - DESC: 显示表结构
     *
     * 2. 事务控制语句
     *    - BEGIN: 开始事务
     *    - COMMIT: 提交事务
     *    - ABORT/ROLLBACK: 回滚事务
     *
     * 3. 系统设置
     *    - SET: 设置系统参数
     *
     * 4. 常规SQL语句
     *    - 委托给Planner处理
     *    - 包括SELECT, INSERT, UPDATE, DELETE等
     *
     * @note 每种语句类型会生成对应的Plan对象，
     * 具体执行逻辑由执行器实现
     */
    std::shared_ptr<Plan> plan_query(std::shared_ptr<Query> query, Context *context) {
        try {
            // 1. 系统命令处理
            if (auto x = std::dynamic_pointer_cast<ast::Help>(query->parse)) {
                return std::make_shared<OtherPlan>(PlanTag::T_Help, std::string());
            } else if (auto x = std::dynamic_pointer_cast<ast::ShowTables>(query->parse)) {
                return std::make_shared<OtherPlan>(PlanTag::T_ShowTable, std::string());
            } else if (auto x = std::dynamic_pointer_cast<ast::ShowIndex>(query->parse)) {
                return std::make_shared<OtherPlan>(PlanTag::T_ShowIndex, x->tab_name);
            } else if (auto x = std::dynamic_pointer_cast<ast::DescTable>(query->parse)) {
                return std::make_shared<OtherPlan>(PlanTag::T_DescTable, x->tab_name);
            }
            // 2. 事务控制语句处理
            else if (auto x = std::dynamic_pointer_cast<ast::TxnBegin>(query->parse)) {
                return std::make_shared<OtherPlan>(PlanTag::T_Transaction_begin, std::string());
            } else if (auto x = std::dynamic_pointer_cast<ast::TxnAbort>(query->parse)) {
                return std::make_shared<OtherPlan>(PlanTag::T_Transaction_abort, std::string());
            } else if (auto x = std::dynamic_pointer_cast<ast::TxnCommit>(query->parse)) {
                return std::make_shared<OtherPlan>(PlanTag::T_Transaction_commit, std::string());
            } else if (auto x = std::dynamic_pointer_cast<ast::TxnRollback>(query->parse)) {
                return std::make_shared<OtherPlan>(PlanTag::T_Transaction_rollback, std::string());
            }
            // 3. 系统设置处理
            else if (auto x = std::dynamic_pointer_cast<ast::SetStmt>(query->parse)) {
                return std::make_shared<SetKnobPlan>(x->set_knob_type_, x->bool_val_);
            }
            // 4. 常规SQL语句处理
            else {
                return planner_->do_planner(query, context);
            }
        } catch (const std::exception& e) {
            throw InternalError("Failed to generate plan: " + std::string(e.what()));
        }
    }

};
