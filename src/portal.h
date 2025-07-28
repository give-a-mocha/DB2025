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
#include "execution/execution_aggregate.h"
#include "execution/explain/execution_explain_filter.h"
#include "execution/explain/execution_explain_group.h"
#include "execution/explain/execution_explain_join.h"
#include "execution/explain/execution_explain_limit.h"
#include "execution/explain/execution_explain_project.h"
#include "execution/explain/execution_explain_scan.h"
#include "execution/explain/execution_explain_sort.h"
#include "execution/execution_group.h"
#include "execution/execution_sort.h"
#include "execution/executor_abstract.h"
#include "execution/executor_limit.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_nestedloop_semi_join.h"
#include "execution/executor_projection.h"
#include "optimizer/plan.h"
#include "execution/executor_mvcc_update.h"
#include "execution/executor_mvcc_delete.h"
#include "execution/executor_mvcc_insert.h"
#include "execution/executor_mvcc_seq_scan.h"
#include "execution/executor_mvcc_index_scan.h"

extern SmManager sm_manager;
extern QlManager ql_manager;

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
   public:
    Portal() = default;
    ~Portal() = default;

    // 将查询执行计划转换成对应的算子树
    std::shared_ptr<PortalStmt> start(std::shared_ptr<Plan> plan, Context *context) {
        TRACE_FUNCTION
        // 这里可以将select进行拆分，例如：一个select，带有return的select等
        if (plan->tag == PlanTag::T_Help || plan->tag == PlanTag::T_ShowTable || plan->tag == PlanTag::T_ShowIndex ||
            plan->tag == PlanTag::T_DescTable || plan->tag == PlanTag::T_Transaction_begin ||
            plan->tag == PlanTag::T_Transaction_commit || plan->tag == PlanTag::T_Transaction_abort ||
            plan->tag == PlanTag::T_Transaction_rollback || plan->tag == PlanTag::T_LOAD ||
            plan->tag == PlanTag::T_SetOutput || plan->tag == PlanTag::T_SetKnob ||
            plan->tag == PlanTag::T_Create_StaticCheckPoint) {
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(),
                                                std::unique_ptr<AbstractExecutor>(), plan);
        } else if (plan->tag == PlanTag::T_CreateTable || plan->tag == PlanTag::T_DropTable ||
                   plan->tag == PlanTag::T_CreateIndex || plan->tag == PlanTag::T_DropIndex) {
            return std::make_shared<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<TabCol>(),
                                                std::unique_ptr<AbstractExecutor>(), plan);
        } else if (plan->tag == PlanTag::T_Insert || plan->tag == PlanTag::T_Delete || plan->tag == PlanTag::T_Update ||
                   plan->tag == PlanTag::T_select || plan->tag == PlanTag::T_explain) {
            auto x = std::static_pointer_cast<DMLPlan>(plan);
            switch (x->tag) {
                case PlanTag::T_select: {
                    auto p = std::static_pointer_cast<ProjectionPlan>(x->subplan_);
                    std::unique_ptr<AbstractExecutor> root = convert_plan_executor(p, context);
                    return std::make_shared<PortalStmt>(PORTAL_ONE_SELECT, std::move(p->sel_cols_), std::move(root),
                                                        plan);
                }

                case PlanTag::T_Update: {
                    std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
                    std::vector<std::tuple<TupleMeta, std::unique_ptr<RmRecord>, Rid>> old_recs;
                    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                        old_recs.emplace_back(scan->tuple_meta(), scan->Next(), scan->rid());
                    }
                    std::unique_ptr<AbstractExecutor> root = std::make_unique<MvccUpdateExecutor>(
                        x->tab_name_, x->set_clauses_, x->conds_, std::move(old_recs), context);

                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(),
                                                        std::move(root), plan);
                }
                case PlanTag::T_Delete: {
                    std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
                    std::vector<std::tuple<TupleMeta, std::unique_ptr<RmRecord>, Rid>> old_recs;
                    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                        old_recs.emplace_back(scan->tuple_meta(), scan->Next(), scan->rid());
                    }

                    std::unique_ptr<AbstractExecutor> root =
                        std::make_unique<MvccDeleteExecutor>(x->tab_name_, x->conds_, std::move(old_recs), context);

                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(),
                                                        std::move(root), plan);
                }

                case PlanTag::T_Insert: {
                    std::unique_ptr<AbstractExecutor> root =
                        std::make_unique<MvccInsertExecutor>(x->tab_name_, x->values_, context);

                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(),
                                                        std::move(root), plan);
                }
                case PlanTag::T_explain: {
                    auto p = std::static_pointer_cast<ProjectionPlan>(x->subplan_);
                    std::vector<TabCol> sel_cols = p->sel_cols_;
                    std::vector<std::string> join_tables;
                    std::unique_ptr<AbstractExecutor> root = convert_plan_explain_executor(p, context, 0, join_tables);
                    return std::make_shared<PortalStmt>(PORTAL_EXPLAIN, std::move(sel_cols), std::move(root), plan);
                }
                default:
                    throw InternalError("Portal Unexpected field type");
                    break;
            }
        } else {
            throw InternalError("Portal Unexpected field type");
        }
        return nullptr;
    }

    // 遍历算子树并执行算子生成执行结果
    void run(std::shared_ptr<PortalStmt> portal, txn_id_t *txn_id, Context *context) {
        TRACE_FUNCTION
        switch (portal->tag) {
            case PORTAL_ONE_SELECT: {
                ql_manager.select_from(std::move(portal->root), std::move(portal->sel_cols), context);
                break;
            }

            case PORTAL_DML_WITHOUT_SELECT: {
                ql_manager.run_dml(std::move(portal->root));
                break;
            }
            case PORTAL_MULTI_QUERY: {
                ql_manager.run_mutli_query(portal->plan, context);
                break;
            }
            case PORTAL_CMD_UTILITY: {
                ql_manager.run_cmd_utility(portal->plan, txn_id, context);
                break;
            }

            case PORTAL_EXPLAIN: {
                ql_manager.run_explain(std::move(portal->root), std::move(portal->sel_cols), context);
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
        TRACE_FUNCTION
        if (plan->tag == PlanTag::T_Projection) {
            auto x = std::static_pointer_cast<ProjectionPlan>(plan);
            return std::make_unique<ProjectionExecutor>(convert_plan_executor(x->subplan_, context), x->sel_cols_);
        } else if (plan->tag == PlanTag::T_SeqScan || plan->tag == PlanTag::T_IndexScan) {
            auto x = std::static_pointer_cast<ScanPlan>(plan);
            if (x->tag == PlanTag::T_SeqScan) {
                return std::make_unique<MvccSeqScanExecutor>(x->tab_name_, x->conds_, context);
            } else {
                return std::make_unique<MvccIndexScanExecutor>(x->tab_name_, x->conds_, x->index_col_names_, context);
            }
        } else if (plan->tag == PlanTag::T_NestLoop || plan->tag == PlanTag::T_SortMerge) {
            auto x = std::static_pointer_cast<JoinPlan>(plan);
            std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->left_, context);
            std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->right_, context);
            if (x->type == JoinType::SEMI_JOIN) {
                return std::make_unique<NestedLoopSemiJoinExecutor>(std::move(left), std::move(right),
                                                                    std::move(x->conds_));
            } else {
                return std::make_unique<NestedLoopJoinExecutor>(std::move(left), std::move(right),
                                                                std::move(x->conds_));
            }
        } else if (plan->tag == PlanTag::T_Sort) {
            auto x = std::static_pointer_cast<SortPlan>(plan);
            return std::make_unique<SortExecutor>(convert_plan_executor(x->subplan_, context), x->sel_cols_,
                                                  x->is_desc_);
        } else if (plan->tag == PlanTag::T_Aggregate) {
            auto x = std::static_pointer_cast<AggregatePlan>(plan);
            return std::make_unique<AggregateExecutor>(convert_plan_executor(x->subplan_, context), x->sel_cols_,
                                                       x->agg_types_);
        } else if (plan->tag == PlanTag::T_Group) {
            auto x = std::static_pointer_cast<GroupPlan>(plan);
            return std::make_unique<GroupExecutor>(convert_plan_executor(x->subplan_, context), x->sel_cols_,
                                                   x->group_cols_, x->having_conds_);
        } else if (plan->tag == PlanTag::T_Limit) {
            auto x = std::static_pointer_cast<LimitPlan>(plan);
            return std::make_unique<LimitExecutor>(convert_plan_executor(x->subplan_, context), x->offset_, x->count_);
        }
        return nullptr;
    }

    std::unique_ptr<AbstractExecutor> convert_plan_explain_executor(std::shared_ptr<Plan> plan, Context *context,
                                                                    int offset, std::vector<std::string> &join_tables) {
        TRACE_FUNCTION
        if (plan->tag == PlanTag::T_Projection) {
            auto x = std::static_pointer_cast<ProjectionPlan>(plan);
            return std::make_unique<ExplainProjectExecutor>(
                convert_plan_explain_executor(std::move(x->subplan_), context, offset + 1, join_tables),
                std::move(x->sel_cols_), offset, x->isStar_);
        } else if (plan->tag == PlanTag::T_SeqScan || plan->tag == PlanTag::T_IndexScan) {
            auto x = std::static_pointer_cast<ScanPlan>(plan);
            join_tables.push_back(x->tab_name_);
            if (x->conds_.empty()) {
                return std::make_unique<ExplainScanExecutor>(std::move(x->tab_name_), offset);
            } else {
                auto res = std::make_unique<ExplainScanExecutor>(std::move(x->tab_name_), offset + 1);
                return std::make_unique<ExplainFilterExecutor>(std::move(res), std::move(x->conds_), offset);
            }
        } else if (plan->tag == PlanTag::T_NestLoop || plan->tag == PlanTag::T_SortMerge) {
            auto x = std::static_pointer_cast<JoinPlan>(plan);
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
                } else {
                    // 未知类型
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
                return std::make_unique<ExplainJoinExecutor>(std::move(left), std::move(right), join_tables,
                                                             std::move(x->conds_), offset);
            }
        } else if (plan->tag == PlanTag::T_Sort) {
            auto x = std::static_pointer_cast<SortPlan>(plan);
            return std::make_unique<ExplainSortExecutor>(
                convert_plan_explain_executor(std::move(x->subplan_), context, offset + 1, join_tables),
                std::move(x->sel_cols_), std::move(x->is_desc_), offset);
        } else if (plan->tag == PlanTag::T_Aggregate) {
            auto x = std::static_pointer_cast<AggregatePlan>(plan);
            return convert_plan_explain_executor(std::move(x->subplan_), context, offset, join_tables);
        } else if (plan->tag == PlanTag::T_Group) {
            auto x = std::static_pointer_cast<GroupPlan>(plan);
            return std::make_unique<ExplainGroupExecutor>(
                convert_plan_explain_executor(std::move(x->subplan_), context, offset + 1, join_tables),
                std::move(x->group_cols_), std::move(x->having_conds_), offset);
        } else if (plan->tag == PlanTag::T_Limit) {
            auto x = std::static_pointer_cast<LimitPlan>(plan);
            return std::make_unique<ExplainLimitExecutor>(
                convert_plan_explain_executor(std::move(x->subplan_), context, offset + 1, join_tables), x->offset_,
                x->count_, offset);
        } else {
            assert(0);
        }
        return nullptr;
    }
};