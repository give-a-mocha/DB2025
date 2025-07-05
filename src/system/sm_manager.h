/**
 * @file sm_manager.h
 * @author RMDB Development Team
 * @brief 系统管理器头文件
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * 系统管理器(System Manager)是数据库系统的核心组件之一，主要负责：
 * - 数据库的创建、删除和打开
 * - 表和索引的创建、删除和维护
 * - 元数据(metadata)的管理
 * - DDL语句的执行
 *
 * 该模块通过以下方式保证数据库的完整性：
 * 1. 维护数据字典，存储所有数据库对象的定义
 * 2. 管理表和索引文件的创建和删除
 * 3. 提供原子性的DDL操作执行
 * 4. 与其他模块(记录管理、索引管理等)协同工作
 */

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
 */
class SmManager {
   public:
    /**
     * @brief 系统管理器的主要成员变量
     */
    DbMeta db_;  // 当前打开的数据库的元数据，包含数据库名称和所有表的定义

    std::unordered_map<std::string, std::unique_ptr<RmFileHandle>> fhs_;
    // 表文件句柄映射表
    // 键：表名
    // 值：对应的记录文件句柄
    // 用途：管理当前数据库中每张表的数据文件访问

    std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>> ihs_;
    // 索引文件句柄映射表
    // 键：索引名（格式：表名_列名）
    // 值：对应的索引文件句柄
    // 用途：管理当前数据库中每个索引的文件访问
    bool is_output_file_ = true;  // 是否启用输出文件

   private:
    DiskManager* disk_manager_;               // 磁盘管理器，负责文件系统操作
    BufferPoolManager* buffer_pool_manager_;  // 缓冲池管理器，提供页面缓存功能
    RmManager* rm_manager_;                   // 记录管理器，处理记录级别的操作
    IxManager* ix_manager_;                   // 索引管理器，处理索引的创建和维护

   public:
    /**
     * @brief 系统管理器构造函数
     * @param disk_manager 磁盘管理器
     * @param buffer_pool_manager 缓冲池管理器
     * @param rm_manager 记录管理器
     * @param ix_manager 索引管理器
     * @note 初始化系统管理器，建立与其他管理器的关联
     */
    SmManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, RmManager* rm_manager,
              IxManager* ix_manager)
        : disk_manager_(disk_manager),
          buffer_pool_manager_(buffer_pool_manager),
          rm_manager_(rm_manager),
          ix_manager_(ix_manager) {}

    /**
     * @brief 系统管理器析构函数
     * @note 确保数据库正确关闭，资源被适当释放
     */
    ~SmManager() {}

    /**
     * @brief 获取缓冲池管理器指针
     * @return BufferPoolManager* 缓冲池管理器指针
     */
    BufferPoolManager* get_bpm() { return buffer_pool_manager_; }

    /**
     * @brief 获取记录管理器指针
     * @return RmManager* 记录管理器指针
     */
    RmManager* get_rm_manager() { return rm_manager_; }

    /**
     * @brief 获取索引管理器指针
     * @return IxManager* 索引管理器指针
     */
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

    void set_log_offset(size_t offset) {
        db_.log_offset_ = offset;
        flush_meta();
    }

    bool insert_index(const std::string& tab_name, RmRecord& rec, Rid rid, Transaction* txn);

    bool insert_index_force(const std::string& tab_name, RmRecord& rec, Rid rid, Transaction* txn);

    bool delete_index(const std::string& tab_name, RmRecord& rec, Transaction* txn);

    bool delete_index_with_rid(const std::string& tab_name, RmRecord& rec, Rid rid, Transaction* txn);

    void flush_to_disk() {
        for (const auto& [tab_name_, fh_] : fhs_) {
            auto file_hdr_ = fh_->get_file_hdr();
            disk_manager_->write_page(fh_->GetFd(), RM_FILE_HDR_PAGE, (char*)(&file_hdr_), sizeof(file_hdr_));
            buffer_pool_manager_->flush_all_pages(fh_->GetFd());
        }
    }

    void set_output_file(bool enable);

    void load_csv_data(const std::string& table_name, const std::string& file_path, Transaction* txn);
};