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
#include "execution/execution_sort.h"
#include "execution/executor_abstract.h"
#include "execution/executor_aggregate.h"
#include "execution/executor_delete.h"
#include "execution/executor_group.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "execution/execution_explain_project.h"
#include "execution/execution_explain_scan.h"
#include "execution/execution_explain_filter.h"

#include "optimizer/plan.h"

/**
 * @brief Portal语句类型标识
 *
 * 用于区分不同类型的SQL语句，以便Portal系统选择合适的执行策略。
 * 每种类型对应不同的执行路径和结果处理方式。
 */
typedef enum portalTag {
    PORTAL_Invalid_Query = 0,   ///< 无效查询
    PORTAL_ONE_SELECT,          ///< 单个SELECT查询，需要返回结果集
    PORTAL_DML_WITHOUT_SELECT,  ///< 不返回结果的DML操作（INSERT/UPDATE/DELETE）
    PORTAL_MULTI_QUERY,         ///< 复合查询，主要是DDL操作
    PORTAL_CMD_UTILITY          ///< 工具命令（HELP、SHOW、事务控制等）,
    PORTAL_EXPLAIN
} portalTag;

/**
 * @brief Portal语句结构
 *
 * 封装了一个完整的可执行语句，包含执行器树和相关元数据。
 * Portal系统通过这个结构在计划生成和执行之间传递信息。
 */
struct PortalStmt {
    portalTag tag;                           ///< 语句类型标识
    std::vector<TabCol> sel_cols;            ///< SELECT语句的输出列信息
    std::unique_ptr<AbstractExecutor> root;  ///< 执行器树的根节点
    std::shared_ptr<Plan> plan;              ///< 原始执行计划，用于某些不需要执行器的操作

    /**
     * @brief 构造Portal语句
     *
     * @param tag_ 语句类型
     * @param sel_cols_ 输出列信息（仅SELECT语句使用）
     * @param root_ 执行器树根节点
     * @param plan_ 原始执行计划
     */
    PortalStmt(portalTag tag_, std::vector<TabCol> sel_cols_, std::unique_ptr<AbstractExecutor> root_,
               std::shared_ptr<Plan> plan_)
        : tag(tag_), sel_cols(std::move(sel_cols_)), root(std::move(root_)), plan(std::move(plan_)) {}
};

/**
 * @brief Portal：计划到执行器的转换器
 *
 * Portal是RMDB执行引擎的关键组件，负责将查询优化器生成的Plan树转换为
 * 可执行的AbstractExecutor树。它实现了查询计划和查询执行之间的桥梁。
 *
 * 主要职责：
 * - 计划转换：递归地将Plan节点转换为对应的Executor节点
 * - 执行协调：根据语句类型选择合适的执行策略
 * - 资源管理：管理执行器的创建和生命周期
 * - 类型分发：根据Plan类型创建对应的执行器实例
 *
 * 转换过程采用访问者模式，通过dynamic_pointer_cast识别具体的Plan类型，
 * 然后创建对应的Executor实例，形成执行器树结构。
 */
class Portal {
   private:
    SmManager *sm_manager_;  ///< 系统管理器，用于创建需要访问存储层的执行器

   public:
    /**
     * @brief 构造Portal转换器
     * @param sm_manager 系统管理器实例
     */
    Portal(SmManager *sm_manager) : sm_manager_(sm_manager) {}
    ~Portal() {}

    /**
     * @brief 启动计划转换过程
     *
     * 这是Portal的主入口函数，负责将各种类型的Plan转换为PortalStmt。
     * 根据Plan的具体类型，选择不同的转换策略：
     *
     * - OtherPlan/SetKnobPlan：工具命令，不需要执行器树
     * - DDLPlan：DDL操作，通过系统管理器直接执行
     * - DMLPlan：根据DML类型进行不同处理
     *   - SELECT：转换为完整的执行器树
     *   - INSERT：创建插入执行器
     *   - UPDATE/DELETE：先扫描定位记录，再执行修改
     *
     * @param plan 待转换的执行计划
     * @param context 执行上下文
     * @return 转换后的Portal语句，包含执行器树和元数据
     */
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
                    std::unique_ptr<AbstractExecutor> root = convert_plan_explain_executor(p, context, 0);
                    return std::make_shared<PortalStmt>(PORTAL_EXPLAIN, std::move(p->sel_cols_), std::move(root),
                                                        plan);
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

    /**
     * @brief 执行Portal语句
     *
     * 根据Portal语句的类型，选择合适的执行路径。这个函数是执行阶段的
     * 分发器，将不同类型的语句分发给QlManager的对应方法处理。
     *
     * 执行路径：
     * - PORTAL_ONE_SELECT：调用select_from处理SELECT查询
     * - PORTAL_DML_WITHOUT_SELECT：调用run_dml处理INSERT/UPDATE/DELETE
     * - PORTAL_MULTI_QUERY：调用run_mutli_query处理DDL操作
     * - PORTAL_CMD_UTILITY：调用run_cmd_utility处理工具命令
     *
     * @param portal 待执行的Portal语句
     * @param ql 查询执行管理器，负责具体的执行逻辑
     * @param txn_id 事务ID，用于事务相关操作
     * @param context 执行上下文
     *
     * @throws InternalError 当遇到未知的Portal类型时
     */
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

    /**
     * @brief 清理Portal资源
     *
     * 当前实现为空，未来可以在这里添加资源清理逻辑，
     * 如关闭文件句柄、释放内存等。
     */
    void drop() {}

    /**
     * @brief 递归转换Plan树为Executor树
     *
     * 这是Portal系统的核心转换函数，采用递归方式将Plan树转换为对应的Executor树。
     * 转换过程保持了原有的树形结构，每个Plan节点对应一个Executor节点。
     *
     * 转换策略（按Plan类型）：
     *
     * 1. **ProjectionPlan → ProjectionExecutor**
     *    - 递归转换子计划
     *    - 传递选择列信息
     *
     * 2. **ScanPlan → SeqScanExecutor/IndexScanExecutor**
     *    - 根据tag区分顺序扫描和索引扫描
     *    - 传递表名、条件、索引列等参数
     *
     * 3. **JoinPlan → NestedLoopJoinExecutor**
     *    - 递归转换左右子计划
     *    - 传递连接条件
     *    - 注意：当前只支持嵌套循环连接
     *
     * 4. **SortPlan → SortExecutor**
     *    - 递归转换子计划
     *    - 传递排序列和排序方向
     *
     * 5. **AggregatePlan → AggregateExecutor**
     *    - 递归转换子计划
     *    - 传递聚合函数信息
     *
     * 6. **GroupPlan → GroupExecutor**
     *    - 递归转换子计划
     *    - 传递分组列信息
     *
     * @param plan 待转换的Plan节点
     * @param context 执行上下文，包含事务和缓冲区信息
     * @return 转换后的Executor节点，如果Plan类型未知则返回nullptr
     *
     * @note 递归转换确保了执行器树与计划树结构的一致性
     * @note 未来可扩展支持更多Plan类型和Executor类型
     */
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
            std::unique_ptr<AbstractExecutor> join =
                std::make_unique<NestedLoopJoinExecutor>(std::move(left), std::move(right), std::move(x->conds_));
            return join;
        } else if (auto x = std::dynamic_pointer_cast<SortPlan>(plan)) {
            return std::make_unique<SortExecutor>(convert_plan_executor(x->subplan_, context), x->sel_col_,
                                                  x->is_desc_);
        } else if (auto x = std::dynamic_pointer_cast<AggregatePlan>(plan)) {
            std::cerr << "AggregatePlan to Executor" << std::endl;
            return std::make_unique<AggregateExecutor>(convert_plan_executor(x->subplan_, context), x->sel_cols_);
        } else if (auto x = std::dynamic_pointer_cast<GroupPlan>(plan)) {
            std::cerr << "GroupPlan to Executor" << std::endl;
            return std::make_unique<GroupExecutor>(convert_plan_executor(x->subplan_, context), x->sel_cols_,
                                                   x->group_cols_, x->having_conds_);
        }
        return nullptr;
    }

    std::unique_ptr<AbstractExecutor> convert_plan_explain_executor(std::shared_ptr<Plan> plan, Context *context, int offset) {
        if (auto x = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
            return std::make_unique<ExplainProjectExecutor>(convert_plan_explain_executor(x->subplan_, context, offset + 1), x->sel_cols_, offset, context);
        } else if (auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
            std::cerr << "plan->执行器这是一个ScanPlan: " << x->tab_name_ << std::endl;
            if(x->conds_.empty()){
                std::cerr << "进入:ScanExecutor" << std::endl;
                return std::make_unique<ExplainScanExecutor>(x->tab_name_, offset, context);
            }else{
                std::cerr << "进入:FilterExecutor" << std::endl;
                auto res =  std::make_unique<ExplainScanExecutor>(x->tab_name_, offset + 1, context);
                return std::make_unique<ExplainFilterExecutor>(std::move(res), x->conds_, offset, context);
            }
        } else if (auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
            
        } else if (auto x = std::dynamic_pointer_cast<SortPlan>(plan)) {
            return convert_plan_explain_executor(x->subplan_, context, offset); 
        }
        return nullptr;
    }
};