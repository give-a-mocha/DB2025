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

#include "common/context.h"
#include "index/ix.h"
#include "record/rm_file_handle.h"
#include "sm_defs.h"
#include "sm_meta.h"

class Context;

/**
 * @brief 列定义结构体，用于创建表时定义列的属性
 *
 * 该结构体包含了创建表时每个列的基本信息，包括列名、数据类型和长度。
 * 在CREATE TABLE语句处理时使用。
 */
struct ColDef {
    std::string name;  // 列名
    ColType type;      // 列的数据类型
    int len;           // 列的长度
};

/**
 * @brief 系统管理器类，负责数据库元数据管理和DDL语句的执行
 *
 * 该类是数据库系统的核心组件之一，主要负责：
 * 1. 管理数据库的创建、删除、打开和关闭
 * 2. 管理表的创建、删除和属性查看
 * 3. 管理索引的创建和删除
 * 4. 维护所有数据库对象的元数据
 */
class SmManager {
   public:
    /**
     * @brief 系统管理器的主要成员变量
     */
    DbMeta db_;  // 当前打开的数据库的元数据
    std::unordered_map<std::string, std::unique_ptr<RmFileHandle>>
        fhs_;  // 文件名到记录文件句柄的映射，管理当前数据库中每张表的数据文件
    std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>>
        ihs_;  // 文件名到索引文件句柄的映射，管理当前数据库中每个索引的文件

   private:
    DiskManager* disk_manager_;               // 磁盘管理器指针
    BufferPoolManager* buffer_pool_manager_;  // 缓冲池管理器指针
    RmManager* rm_manager_;                   // 记录管理器指针
    IxManager* ix_manager_;                   // 索引管理器指针

   public:
    SmManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, RmManager* rm_manager,
              IxManager* ix_manager)
        : disk_manager_(disk_manager),
          buffer_pool_manager_(buffer_pool_manager),
          rm_manager_(rm_manager),
          ix_manager_(ix_manager) {}

    ~SmManager() {}

    BufferPoolManager* get_bpm() { return buffer_pool_manager_; }

    RmManager* get_rm_manager() { return rm_manager_; }

    IxManager* get_ix_manager() { return ix_manager_; }

    /**
     * @brief 检查指定名称的数据库是否存在
     * @param db_name 数据库名称
     * @return true如果数据库存在，false否则
     */
    bool is_dir(const std::string& db_name);

    /**
     * @brief 创建一个新的数据库
     * @param db_name 要创建的数据库名称
     */
    void create_db(const std::string& db_name);

    /**
     * @brief 删除指定的数据库
     * @param db_name 要删除的数据库名称
     */
    void drop_db(const std::string& db_name);

    /**
     * @brief 打开指定的数据库
     * @param db_name 要打开的数据库名称
     */
    void open_db(const std::string& db_name);

    /**
     * @brief 关闭当前打开的数据库
     */
    void close_db();

    /**
     * @brief 将数据库的元数据写入磁盘
     */
    void flush_meta();

    /**
     * @brief 显示当前数据库中的所有表
     * @param context 执行上下文
     */
    void show_tables(Context* context);

    /**
     * @brief 显示指定表的所有索引
     * @param tab_name 表名
     * @param context 执行上下文
     */
    void show_index(const std::string& tab_name, Context* context);

    /**
     * @brief 描述指定表的结构
     * @param tab_name 表名
     * @param context 执行上下文
     */
    void desc_table(const std::string& tab_name, Context* context);

    /**
     * @brief 创建新表
     * @param tab_name 表名
     * @param col_defs 列定义向量
     * @param context 执行上下文
     */
    void create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context);

    /**
     * @brief 删除指定的表
     * @param tab_name 要删除的表名
     * @param context 执行上下文
     */
    void drop_table(const std::string& tab_name, Context* context);

    /**
     * @brief 在指定表上创建索引
     * @param tab_name 表名
     * @param col_names 需要创建索引的列名向量
     * @param context 执行上下文
     */
    void create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);

    /**
     * @brief 删除指定表上的索引
     * @param tab_name 表名
     * @param col_names 索引列名向量
     * @param context 执行上下文
     */
    void drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);

    /**
     * @brief 删除指定表上的索引(使用列元数据)
     * @param tab_name 表名
     * @param col_names 索引列元数据向量
     * @param context 执行上下文
     */
    void drop_index(const std::string& tab_name, const std::vector<ColMeta>& col_names, Context* context);
};