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

extern SmManager sm_manager;
extern Planner planner;


/**
 * @brief 查询优化器类
 */
class Optimizer {
   public:
    Optimizer() = default;

    /**
     * @brief 为查询生成执行计划
     * @param query 查询对象
     * @param context 执行上下文
     * @return 生成的执行计划
     * @throw PlanError 当计划生成失败时
     */
    std::shared_ptr<Plan> plan_query(std::shared_ptr<Query> query, Context *context) {
        TRACE_FUNCTION
        // 1. 系统命令处理
        switch (query->parse->getType()) {
            case ast::AstType::Help:
                return std::make_shared<OtherPlan>(PlanTag::T_Help, std::string());
            case ast::AstType::ShowTables:
                return std::make_shared<OtherPlan>(PlanTag::T_ShowTable, std::string());
            case ast::AstType::ShowIndex: {
                auto x = std::static_pointer_cast<ast::ShowIndex>(query->parse);
                return std::make_shared<OtherPlan>(PlanTag::T_ShowIndex, x->tab_name);
            }
            case ast::AstType::DescTable: {
                auto x = std::static_pointer_cast<ast::DescTable>(query->parse);
                return std::make_shared<OtherPlan>(PlanTag::T_DescTable, x->tab_name);
            }
            case ast::AstType::LoadStmt: {
                auto x = std::static_pointer_cast<ast::LoadStmt>(query->parse);
                return std::make_shared<LoadPlan>(PlanTag::T_LOAD, x->table_name, x->file_name);
            }
            case ast::AstType::SetOutputStmt: {
                auto x = std::static_pointer_cast<ast::SetOutputStmt>(query->parse);
                return std::make_shared<SetOutputPlan>(PlanTag::T_SetOutput, x->enable);
            }
            case ast::AstType::TxnBegin:
                return std::make_shared<OtherPlan>(PlanTag::T_Transaction_begin, std::string());
            case ast::AstType::TxnAbort:
                return std::make_shared<OtherPlan>(PlanTag::T_Transaction_abort, std::string());
            case ast::AstType::TxnCommit:
                return std::make_shared<OtherPlan>(PlanTag::T_Transaction_commit, std::string());
            case ast::AstType::TxnRollback:
                return std::make_shared<OtherPlan>(PlanTag::T_Transaction_rollback, std::string());
            case ast::AstType::CreateStaticCheckpoint:
                return std::make_shared<OtherPlan>(PlanTag::T_Create_StaticCheckPoint, std::string());
            case ast::AstType::SetStmt: {
                auto x = std::static_pointer_cast<ast::SetStmt>(query->parse);
                return std::make_shared<SetKnobPlan>(x->set_knob_type_, x->bool_val_);
            }
            default:
                return planner.do_planner(query, context);
        }
    }
};
