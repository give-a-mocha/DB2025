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

#include <cerrno>
#include <cstring>
#include <string>

#include "common/common.h"
#include "common/print.hpp"
#include "execution/execution_explain_filter.h"
#include "execution/execution_explain_join.h"
#include "execution/execution_explain_project.h"
#include "execution/execution_explain_scan.h"
#include "execution/execution_sort.h"
#include "execution/executor_abstract.h"
#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_nestedloop_semi_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "optimizer/plan.h"

typedef enum portalTag {
    PORTAL_Invalid_Query = 0,
    PORTAL_ONE_SELECT,
    PORTAL_DML_WITHOUT_SELECT,
    PORTAL_MULTI_QUERY,
    PORTAL_CMD_UTILITY,
    PORTAL_EXPLAIN
} portalTag;

struct PortalStmt {
    portalTag tag;

    std::vector<TabCol> sel_cols;
    std::unique_ptr<AbstractExecutor> root;
    std::shared_ptr<Plan> plan;

    PortalStmt(portalTag tag_, std::vector<TabCol> sel_cols_, std::unique_ptr<AbstractExecutor> root_,
               std::shared_ptr<Plan> plan_)
        : tag(tag_), sel_cols(std::move(sel_cols_)), root(std::move(root_)), plan(std::move(plan_)) {}
};

class Portal {
   private:
    SmManager *sm_manager_;

   public:
    Portal(SmManager *sm_manager) : sm_manager_(sm_manager) {}
    ~Portal() {}

    // 将查询执行计划转换成对应的算子树
    std::shared_ptr<PortalStmt> start(std::shared_ptr<Plan> plan, Context *context) {
        // 这里可以将select进行拆分，例如：一个select，带有return的select等
        if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(),
                                                std::unique_ptr<AbstractExecutor>(), plan);
        } else if (auto x = std::dynamic_pointer_cast<SetKnobPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(),
                                                std::unique_ptr<AbstractExecutor>(), plan);
        } else if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<TabCol>(),
                                                std::unique_ptr<AbstractExecutor>(), plan);
        } else if (auto x = std::dynamic_pointer_cast<DMLPlan>(plan)) {
            switch (x->tag) {
                case PlanTag::T_select: {
                    std::shared_ptr<ProjectionPlan> p = std::dynamic_pointer_cast<ProjectionPlan>(x->subplan_);
                    std::unique_ptr<AbstractExecutor> root = convert_plan_executor(p, context);
                    return std::make_shared<PortalStmt>(PORTAL_ONE_SELECT, std::move(p->sel_cols_), std::move(root),
                                                        plan);
                }

                case PlanTag::T_Update: {
                    std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
                    std::vector<Rid> rids;
                    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                        rids.push_back(scan->rid());
                    }
                    std::unique_ptr<AbstractExecutor> root = std::make_unique<UpdateExecutor>(
                        sm_manager_, x->tab_name_, x->set_clauses_, x->conds_, rids, context);
                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(),
                                                        std::move(root), plan);
                }
                case PlanTag::T_Delete: {
                    std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
                    std::vector<Rid> rids;
                    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                        rids.push_back(scan->rid());
                    }

                    std::unique_ptr<AbstractExecutor> root =
                        std::make_unique<DeleteExecutor>(sm_manager_, x->tab_name_, x->conds_, rids, context);

                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(),
                                                        std::move(root), plan);
                }

                case PlanTag::T_Insert: {
                    std::unique_ptr<AbstractExecutor> root =
                        std::make_unique<InsertExecutor>(sm_manager_, x->tab_name_, x->values_, context);

                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(),
                                                        std::move(root), plan);
                }
                case PlanTag::T_explain: {
                    std::shared_ptr<ProjectionPlan> p = std::dynamic_pointer_cast<ProjectionPlan>(x->subplan_);
                    std::vector<TabCol> sel_cols = p->sel_cols_;
                    std::vector<std::string> join_tables;
                    std::unique_ptr<AbstractExecutor> root = convert_plan_explain_executor(p, context, 0, join_tables);
                    return std::make_shared<PortalStmt>(PORTAL_EXPLAIN, std::move(sel_cols), std::move(root), plan);
                }
                default:
                    throw InternalError("Unexpected field type");
                    break;
            }
        } else {
            throw InternalError("Unexpected field type");
        }
        return nullptr;
    }

    // 遍历算子树并执行算子生成执行结果
    void run(std::shared_ptr<PortalStmt> portal, QlManager *ql, txn_id_t *txn_id, Context *context) {
        switch (portal->tag) {
            case PORTAL_ONE_SELECT: {
                ql->select_from(std::move(portal->root), std::move(portal->sel_cols), context);
                break;
            }

            case PORTAL_DML_WITHOUT_SELECT: {
                ql->run_dml(std::move(portal->root));
                break;
            }
            case PORTAL_MULTI_QUERY: {
                ql->run_mutli_query(portal->plan, context);
                break;
            }
            case PORTAL_CMD_UTILITY: {
                ql->run_cmd_utility(portal->plan, txn_id, context);
                break;
            }

            case PORTAL_EXPLAIN: {
                ql->run_explain(std::move(portal->root), std::move(portal->sel_cols), context);
                break;
            }
            default: {
                throw InternalError("Unexpected field type");
            }
        }
    }

    // 清空资源
    void drop() {}

    std::unique_ptr<AbstractExecutor> convert_plan_executor(std::shared_ptr<Plan> plan, Context *context) {
        if (auto x = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
            return std::make_unique<ProjectionExecutor>(convert_plan_executor(x->subplan_, context), x->sel_cols_);
        } else if (auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
            if (x->tag == PlanTag::T_SeqScan) {
                return std::make_unique<SeqScanExecutor>(sm_manager_, x->tab_name_, x->conds_, context);
            } else {
                return std::make_unique<IndexScanExecutor>(sm_manager_, x->tab_name_, x->conds_, x->index_col_names_,
                                                           context);
            }
        } else if (auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
            std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->left_, context);
            std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->right_, context);
            if(x->type == JoinType::SEMI_JOIN){
                return std::make_unique<NestedLoopSemiJoinExecutor>(std::move(left), std::move(right), std::move(x->conds_));
            }else{
                return std::make_unique<NestedLoopJoinExecutor>(std::move(left), std::move(right), std::move(x->conds_));
            }
        } else if (auto x = std::dynamic_pointer_cast<SortPlan>(plan)) {
            return std::make_unique<SortExecutor>(convert_plan_executor(x->subplan_, context), x->sel_col_,
                                                  x->is_desc_);
        }
        return nullptr;
    }

    std::unique_ptr<AbstractExecutor> convert_plan_explain_executor(std::shared_ptr<Plan> plan, Context *context,
                                                                    int offset, std::vector<std::string> &join_tables) {
        if (auto x = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
            return std::make_unique<ExplainProjectExecutor>(
                convert_plan_explain_executor(std::move(x->subplan_), context, offset + 1, join_tables), std::move(x->sel_cols_), offset, x->isStar_);
        } else if (auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
            join_tables.push_back(x->tab_name_);
            if (x->conds_.empty()) {
                return std::make_unique<ExplainScanExecutor>(std::move(x->tab_name_), offset);
            } else {
                auto res = std::make_unique<ExplainScanExecutor>(std::move(x->tab_name_), offset + 1);
                return std::make_unique<ExplainFilterExecutor>(std::move(res), std::move(x->conds_), offset);
            }
        } else if (auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
            std::vector<Condition> solve_conds;
            std::vector<Condition> conds;
            for (const auto &cond : x->conds_) {
                if (cond.op == CompOp::OP_EQ) {
                    solve_conds.push_back(cond);
                } else {
                    conds.push_back(cond);
                }
            }
            int add_offset = 0;
            if (!conds.empty()) {
                add_offset = 2;
            } else {
                add_offset = 1;
            }
            auto left = convert_plan_explain_executor(std::move(x->left_), context, offset + add_offset, join_tables);
            auto right = convert_plan_explain_executor(std::move(x->right_), context, offset + add_offset, join_tables);
            auto get_level = [](const std::unique_ptr<AbstractExecutor> &executor) -> std::string {
                if (auto y = dynamic_cast<ExplainFilterExecutor *>(executor.get())) {
                    return "1_" + y->get_conds();  // Filter
                } else if (auto y = dynamic_cast<ExplainJoinExecutor *>(executor.get())) {
                    return "2_" + y->get_tables();  // Join
                } else if (auto y = dynamic_cast<ExplainProjectExecutor *>(executor.get())) {
                    return "3_" + y->get_cols();  // Project
                } else if (auto y = dynamic_cast<ExplainScanExecutor *>(executor.get())) {
                    return "4_" + y->get_tab_name();  // Scan
                } else{
                    //未知类型
                    return "unknown";
                }
            };
            if (get_level(left) > get_level(right)) {
                std::swap(left, right);
            }
            if (!conds.empty()) {
                auto res = std::make_unique<ExplainJoinExecutor>(std::move(left), std::move(right), join_tables,
                                                                 std::move(solve_conds), offset + 1);
                return std::make_unique<ExplainFilterExecutor>(std::move(res), conds, offset);
            } else {
                return std::make_unique<ExplainJoinExecutor>(std::move(left), std::move(right), join_tables, std::move(x->conds_),
                                                             offset);
            }
        } else if (auto x = std::dynamic_pointer_cast<SortPlan>(plan)) {
            return convert_plan_explain_executor(std::move(x->subplan_), context, offset, join_tables);
        }
        return nullptr;
    }
};