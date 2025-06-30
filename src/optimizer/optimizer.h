/**
 * @file optimizer.h
 * @author RMDB Development Team
 * @brief 查询优化器的实现
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 */

#pragma once

#include <map>

#include "common/TraceStack.hpp"
#include "common/context.h"
#include "common/print.hpp"
#include "errors.h"
#include "execution/execution.h"
#include "parser/parser.h"
#include "plan.h"
#include "planner.h"
#include "system/sm.h"
#include "transaction/transaction_manager.h"

/**
 * @brief 查询优化器类
 */
class Optimizer {
   private:
    SmManager *sm_manager_;  // 系统管理器指针
    Planner *planner_;       // 计划生成器指针

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     * @param planner 计划生成器指针
     * @throw InternalError 如果任一指针为空
     */
    Optimizer(SmManager *sm_manager, Planner *planner) : sm_manager_(sm_manager), planner_(planner) {
        TRACE_FUNCTION
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
     */
    std::shared_ptr<Plan> plan_query(std::shared_ptr<Query> query, Context *context) {
        TRACE_FUNCTION
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
            } else if (auto x = std::dynamic_pointer_cast<ast::LoadStmt>(query->parse)) {
                // LoadStmt load file_name into table_name;
                return std::make_shared<LoadPlan>(PlanTag::T_LOAD, x->table_name, x->file_name);
            } else if (auto x = std::dynamic_pointer_cast<ast::SetOutputStmt>(query->parse)) {
                return std::make_shared<SetOutputPlan>(PlanTag::T_SetOutput, x->enable);
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
            } else if (auto x = std::dynamic_pointer_cast<ast::CreateStaticCheckpoint>(query->parse)) {
                // CreateStaticCheckpoint
                return std::make_shared<OtherPlan>(PlanTag::T_Create_StaticCheckPoint, std::string());
            }

            // 3. 系统设置处理
            else if (auto x = std::dynamic_pointer_cast<ast::SetStmt>(query->parse)) {
                return std::make_shared<SetKnobPlan>(x->set_knob_type_, x->bool_val_);
            }
            // 4. 常规SQL语句处理
            else {
                return planner_->do_planner(query, context);
            }
        } catch (const std::exception &e) {
            throw InternalError("Failed to generate plan: " + std::string(e.what()));
        }
    }
};
