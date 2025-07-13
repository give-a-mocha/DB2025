/**
 * @file execution_manager.cpp
 * @author RMDB Development Team
 * @brief RMDB执行管理器实现
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * RMDB查询执行管理器负责：
 * 1. 执行DDL语句（创建/删除表和索引）
 * 2. 执行DML语句（插入/更新/删除数据）
 * 3. 执行查询语句（SELECT）
 * 4. 执行事务控制语句（开始/提交/回滚）
 * 5. 执行实用工具命令（帮助/显示表等）
 */

#include "execution_manager.h"

#include "common/print.hpp"
#include "executor_index_scan.h"
#include "executor_nestedloop_join.h"
#include "executor_projection.h"
#include "executor_seq_scan.h"
#include "index/ix.h"
#include "record_printer.h"
#include "common/BatchArray.hpp"

/**
 * @brief SQL命令帮助信息
 * 包含所有支持的SQL语法说明，包括：
 * - 表和索引的创建/删除
 * - 数据的插入/更新/删除
 * - 查询语句语法
 * - 数据类型说明
 * - WHERE子句语法
 * - 运算符说明
 */
constexpr const char *help_info =
    "Supported SQL syntax:\n"
    "  command ;\n"
    "command:\n"
    "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
    "  DROP TABLE table_name\n"
    "  CREATE INDEX table_name (column_name)\n"
    "  DROP INDEX table_name (column_name)\n"
    "  INSERT INTO table_name VALUES (value [, value ...])\n"
    "  DELETE FROM table_name [WHERE where_clause]\n"
    "  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
    "  SELECT selector FROM table_name [WHERE where_clause]\n"
    "type:\n"
    "  {INT | FLOAT | CHAR(n)}\n"
    "where_clause:\n"
    "  condition [AND condition ...]\n"
    "condition:\n"
    "  column op {column | value}\n"
    "column:\n"
    "  [table_name.]column_name\n"
    "op:\n"
    "  {= | <> | < | > | <= | >=}\n"
    "selector:\n"
    "  {* | column [, column ...]}\n";
constexpr int help_info_len = strlen(help_info);

/**
 * @brief 执行DDL(数据定义语言)语句
 *
 * @param plan DDL语句的执行计划，包含创建/删除表和索引的具体信息
 * @param context 执行上下文，包含事务信息和结果缓冲区
 * @throw InternalError 当遇到未预期的计划类型时抛出
 */
void QlManager::run_mutli_query(std::shared_ptr<Plan> plan, Context *context) {
    if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
        switch (x->tag) {
            case PlanTag::T_CreateTable: {
                sm_manager_->create_table(x->tab_name_, x->cols_, context);
                break;
            }
            case PlanTag::T_DropTable: {
                sm_manager_->drop_table(x->tab_name_, context);
                break;
            }
            case PlanTag::T_CreateIndex: {
                sm_manager_->create_index(x->tab_name_, x->tab_col_names_, context);
                break;
            }
            case PlanTag::T_DropIndex: {
                sm_manager_->drop_index(x->tab_name_, x->tab_col_names_, context);
                break;
            }
            default:
                throw InternalError("Unexpected field type");
                break;
        }
    }
}

/**
 * @brief 执行实用工具命令和事务控制
 *
 * @param plan 命令的执行计划
 * @param txn_id 事务ID指针，用于事务控制命令
 * @param context 执行上下文，包含事务和输出信息
 * @throw InternalError 当遇到未预期的命令类型时抛出
 */
void QlManager::run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context) {
    if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
        switch (x->tag) {
            case PlanTag::T_Help: {
                strcpy(context->data_send_ + *(context->offset_), help_info);
                *(context->offset_) = help_info_len;
                break;
            }
            case PlanTag::T_ShowTable: {
                sm_manager_->show_tables(context);
                break;
            }
            case PlanTag::T_ShowIndex: {
                sm_manager_->show_index(x->tab_name_, context);
                break;
            }
            case PlanTag::T_DescTable: {
                sm_manager_->desc_table(x->tab_name_, context);
                break;
            }
            case PlanTag::T_Transaction_begin: {
                // 显示开启一个事务
                context->txn_->set_txn_mode(true);
                break;
            }
            case PlanTag::T_Transaction_commit: {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->commit(context->txn_, context->log_mgr_);
                // if (txn_mgr_->should_perform_gc()) {
                //     // 如果事务数量过多，或者有大量已终止的事务，则执行垃圾回收
                //     txn_mgr_->GarbageCollection();
                // }
                break;
            }
            case PlanTag::T_Transaction_rollback: {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->abort(context, context->log_mgr_);
                // if (txn_mgr_->should_perform_gc()) {
                //     // 如果事务数量过多，或者有大量已终止的事务，则执行垃圾回收
                //     txn_mgr_->GarbageCollection();
                // }
                break;
            }
            case PlanTag::T_Transaction_abort: {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->abort(context, context->log_mgr_);
                // if (txn_mgr_->should_perform_gc()) {
                //     // 如果事务数量过多，或者有大量已终止的事务，则执行垃圾回收
                //     txn_mgr_->GarbageCollection();
                // }
                break;
            }
            case PlanTag::T_Create_StaticCheckPoint: {
                recovery_manager_->create_static_check_point();
                break;
            }
            default:
                throw InternalError("Unexpected field type");
                break;
        }

    } else if (auto x = std::dynamic_pointer_cast<SetKnobPlan>(plan)) {
        switch (x->set_knob_type_) {
            case ast::SetKnobType::EnableNestLoop: {
                planner_->set_enable_nestedloop_join(x->bool_value_);
                break;
            }
            case ast::SetKnobType::EnableSortMerge: {
                planner_->set_enable_sortmerge_join(x->bool_value_);
                break;
            }
            default: {
                throw RMDBError("Not implemented!\n");
                break;
            }
        }
    } else if (auto x = std::dynamic_pointer_cast<LoadPlan>(plan)) {
        // Load数据到表中
        sm_manager_->load_csv_data_auto(x->table_name_, x->file_path_, context->txn_);
    } else if (auto x = std::dynamic_pointer_cast<SetOutputPlan>(plan)) {
        // 设置输出文件
        sm_manager_->set_output_file(x->enable_);
    }
}

/**
 * @brief 执行SELECT查询语句
 *
 * 该函数负责执行SELECT查询并处理结果输出：
 * 1. 将结果返回给客户端
 * 2. 同时写入output.txt文件
 * 输出格式为表格形式，包含列名和分隔符
 *
 * @param executorTreeRoot 执行器树的根节点
 * @param sel_cols 需要查询的列
 * @param context 执行上下文
 */
void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                            Context *context) {
    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto &sel_col : sel_cols) {
        if (!sel_col.col_alias.empty()) captions.push_back(sel_col.col_alias);
        else captions.push_back(sel_col.col_name);
    }

    // Print header into buffer
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    // print header into file
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "|";
    for (std::size_t i = 0; i < captions.size(); ++i) {
        outfile << " " << captions[i] << " |";
    }
    outfile << "\n";

    // Print records
    size_t num_rec = 0;
    // 执行query_plan
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
        auto batch_record = executorTreeRoot->Next();
        if (!batch_record) continue;
        for (auto &record : *batch_record) {
            std::vector<std::string> columns;
            for (auto &col : executorTreeRoot->cols()) {
                std::string col_str;
                char *rec_buf = record->data + col.offset;
                if (col.type == ColType::TYPE_INT) {
                    col_str = std::to_string(*(int *)rec_buf);
                } else if (col.type == ColType::TYPE_FLOAT) {
                    col_str = std::to_string(*(float *)rec_buf);
                } else if (col.type == ColType::TYPE_STRING) {
                    col_str = std::string((char *)rec_buf, col.len);
                    col_str.resize(strlen(col_str.c_str()));
                }
                columns.push_back(col_str);
            }
            // print record into buffer
            rec_printer.print_record(columns, context);
            // print record into file
            outfile << "|";
            for (const std::string &column : columns) {
                outfile << " " << column << " |";
            }
            outfile << "\n";
            num_rec++;
        }
    }
    outfile.close();
    // Print footer into buffer
    rec_printer.print_separator(context);
    // Print record count into buffer
    RecordPrinter::print_record_count(num_rec, context);
}

/**
 * @brief 执行DML(数据操作语言)语句
 *
 * @param exec DML执行器指针，可以是插入、更新或删除执行器
 *
 * @note 调用执行器的Next()方法来执行具体的DML操作：
 * - INSERT: 插入新记录
 * - UPDATE: 更新已有记录
 * - DELETE: 删除符合条件的记录
 * 具体的执行逻辑由对应的执行器实现。
 */
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec) { exec->Next(); }

/**
 * @brief 执行EXPLAIN命令
 *
 * @param executorTreeRoot 执行器树的根节点，包含完整的执行计划
 * @param sel_cols 查询涉及的列信息
 * @param context 执行上下文，用于存储和输出执行计划
 */
void QlManager::run_explain(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                            Context *context) {
    executorTreeRoot->Next();
}