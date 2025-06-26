/**
 * @file sm_manager.cpp
 * @author RMDB Development Team
 * @brief 系统管理器实现
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * 系统管理器是数据库的核心组件，负责：
 * 1. 物理文件组织：
 *    - 数据库目录结构管理
 *    - 表文件和索引文件的创建与删除
 * 2. 元数据管理：
 *    - 维护数据字典
 *    - 管理表的定义、索引信息
 * 3. DDL语句执行：
 *    - CREATE/DROP DATABASE/TABLE/INDEX
 *    - SHOW/DESC TABLE
 * 4. 系统恢复：
 *    - 数据库启动时的元数据加载
 *    - 故障恢复时的系统状态重建
 */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @brief 判断指定路径是否为一个文件夹
 *
 * @param db_name 数据库文件名称
 * @return true 如果是一个有效的文件夹
 * @return false 如果不是文件夹或路径不存在
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @brief 创建新的数据库
 *
 * @param db_name 数据库名称
 * @throw DatabaseExistsError 如果数据库已存在
 * @throw UnixError 如果文件系统操作失败
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    // 为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    // 创建系统目录
    DbMeta* new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @brief 删除指定的数据库
 *
 * @param db_name 数据库名称
 * @throw DatabaseNotFoundError 如果数据库不存在
 * @throw UnixError 如果文件系统操作失败
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，加载数据库的元数据和文件
 *
 * 该函数完成以下操作：
 * 1. 检查数据库是否存在
 * 2. 进入数据库目录
 * 3. 读取数据库元数据文件
 * 4. 打开所有表文件的文件句柄
 * 5. 打开所有索引文件的句柄
 *
 * @param {string&} db_name 数据库名称，与文件夹同名
 * @throw DatabaseNotFoundError 如果数据库不存在
 * @throw UnixError 如果文件系统操作失败
 */
void SmManager::open_db(const std::string& db_name) {
    //! DO
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }

    // 进入数据库目录
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }

    // 读取数据库元数据
    std::ifstream ifs(DB_META_NAME);
    ifs >> db_;

    // 打开所有表的文件句柄
    for (auto& [table_name, table_info] : db_.tabs_) {
        fhs_.emplace(table_name, rm_manager_->open_file(table_name));
        for (auto& index : table_info.indexes) {
            auto&& index_name = ix_manager_->get_index_name(table_name, index.cols);
            ihs_.emplace(index_name, ix_manager_->open_index_with_index_name(index_name));
        }
    }
}

/**
 * @description: 将数据库元数据持久化到磁盘
 *
 * 该函数将内存中的数据库元数据写入磁盘文件，确保数据库结构的持久性。
 * 写入操作会清空并重写整个元数据文件。
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭当前打开的数据库
 * @throw DatabaseNotFoundError 如果数据库不存在
 * @throw UnixError 如果文件系统操作失败
 */
void SmManager::close_db() {
    //! DO
    auto&& db_name = db_.name_;
    if (chdir("..") >= 0) {
        if (!is_dir(db_name)) {
            throw DatabaseNotFoundError(db_name);
        }
        if (chdir(db_name.c_str()) < 0) {
            throw UnixError();
        }
    }

    // 1. 先关闭所有表文件并确保数据写入磁盘
    for (const auto& [table_name, file_handle] : fhs_) {
        // 使用close_file_and_clear_buffer确保数据完全写入并清理缓冲区
        rm_manager_->close_file_and_clear_buffer(file_handle.get());
    }

    // 2. 关闭所有索引文件
    for (const auto& [index_name, index_handle] : ihs_) {
        ix_manager_->close_index(index_handle.get());
    }

    // 3. 最后刷新并保存元数据
    flush_meta();

    db_.name_.clear();
    db_.tabs_.clear();
    fhs_.clear();
    ihs_.clear();

    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示数据库中的所有表
 * @param {Context*} context 执行上下文
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto& entry : db_.tabs_) {
        auto& tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}
// TODO:
/**
 * @description: 显示指定表的所有索引信息
 *
 * 该函数将：
 * 1. 验证表是否存在
 * 2. 获取表的元数据
 * 3. 遍历并显示表的所有索引信息，包括：
 *    - 表名
 *    - 索引类型（unique）
 *    - 索引包含的列名
 *
 * @param {string&} tab_name 表名称
 * @param {Context*} context 执行上下文
 * @throw TableNotFoundError 如果表不存在
 */
void SmManager::show_index(const std::string& tab_name, Context* context) {
    // 检查表是否存在
    if (db_.tabs_.find(tab_name) == db_.tabs_.end()) {
        throw TableNotFoundError(tab_name);
    }

    TabMeta& tab = db_.get_table(tab_name);

    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);

    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"index"}, context);
    printer.print_separator(context);

    // 遍历表的所有索引
    for (auto& index : tab.indexes) {
        std::string col_names = "(";
        for (size_t i = 0; i < index.cols.size(); ++i) {
            if (i != 0) col_names += ",";
            col_names += index.cols[i].name;
        }
        col_names += ")";
        outfile << "| " << tab_name << " | " << "unique" << " | " << col_names << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的详细结构信息
 *
 * 该函数展示表的完整元数据信息，包括：
 * 1. 所有字段的名称
 * 2. 每个字段的数据类型
 * 3. 是否建立了索引
 *
 * 输出格式为表格形式，包含字段名、类型和索引三列。
 *
 * @param {string&} tab_name 表名称
 * @param {Context*} context 执行上下文
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta& tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto& col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建新表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段定义
 * @param {Context*} context 执行上下文
 * @throw TableExistsError 如果表已经存在
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    // 1. 检查表是否已存在
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }

    // 2. 创建表元数据
    int curr_offset = 0;  // 记录每个字段在记录中的偏移量
    TabMeta tab;
    tab.name = tab_name;

    // 3. 构建列的元数据
    for (auto& col_def : col_defs) {
        // 创建列元数据对象
        ColMeta col = {
            .tab_name = tab_name,   // 表名
            .name = col_def.name,   // 列名
            .type = col_def.type,   // 数据类型
            .len = col_def.len,     // 字段长度
            .offset = curr_offset,  // 在记录中的偏移量
            .index = false          // 初始无索引
        };
        curr_offset += col_def.len;  // 更新偏移量
        tab.cols.push_back(col);     // 添加到表的列集合
    }

    // 4. 创建表的物理文件
    // record_size表示每条记录的大小，等于所有字段长度之和
    int record_size = curr_offset;
    rm_manager_->create_file(tab_name, record_size);

    // 5. 更新数据库元数据
    db_.tabs_[tab_name] = tab;

    // 6. 打开表文件，创建文件句柄
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    // 7. 持久化元数据变更
    flush_meta();
}

/**
 * @description: 删除指定的表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context 执行上下文
 * @throw TableNotFoundError 如果表不存在
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    // 1. 验证表是否存在，不存在则抛出异常
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    // 2. 获取表的排他锁，确保当前没有其他事务在访问该表
    // if (context != nullptr) {
    //     context->lock_mgr_->lock_exclusive_on_table(context->txn_, fhs_[tab_name]->GetFd());
    // }

    // 3. 获取表元数据
    TabMeta& tab_meta = db_.get_table(tab_name);

    // 4. 删除表的所有索引文件
    for (auto& index : tab_meta.indexes) {
        // 递归调用删除索引的函数
        drop_index(tab_name, index.cols, context);
    }

    // 5. 关闭表文件并清理资源
    if (fhs_.count(tab_name)) {
        // 关闭文件并清理缓冲池中的相关页面
        rm_manager_->close_file_and_clear_buffer(fhs_[tab_name].get());
        // 从文件句柄映射中移除
        fhs_.erase(tab_name);
    }

    // 6. 删除表的物理文件
    rm_manager_->destroy_file(tab_name);

    // 7. 更新数据库元数据
    db_.tabs_.erase(tab_name);

    // 8. 持久化元数据变更
    flush_meta();
}

/**
 * @description: 在指定表上创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context 执行上下文
 * @throw IndexExistsError 如果索引已存在
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // 1. 获取表的元数据并验证
    TabMeta& tab = db_.get_table(tab_name);

    auto&& index_name = ix_manager_->get_index_name(tab_name, col_names);

    // 2. 检查索引是否已存在
    if (ix_manager_->exists_with_index_name(index_name)) {
        throw IndexExistsError(tab_name, col_names);
    }

    // 3. 获取索引列的元数据
    std::vector<ColMeta> cols(col_names.size());
    int tot_col_len = 0;  // 索引键的总长度
    for (size_t i = 0; i < col_names.size(); ++i) {
        auto col_name = col_names[i];
        cols[i] = (*tab.get_col(col_name));  // 获取列的元数据
        tot_col_len += cols[i].len;          // 累加列长度
    }

    // 4. 创建和打开索引文件
    auto fh_ = fhs_[tab_name].get();                                 // 获取表的文件句柄
    ix_manager_->create_index(tab_name, cols);                       // 创建索引文件
    auto ih_ = ix_manager_->open_index_with_index_name(index_name);  // 打开索引文件

    // 5. 为索引键分配缓冲区
    std::vector<char> key_buffer(tot_col_len);  // 存储组合索引键的缓冲区
    char* key = key_buffer.data();

    // 6. 扫描表中所有记录，构建B+树索引
    for (RmScan rmScan(fh_); !rmScan.is_end(); rmScan.next()) {
        // 获取记录数据
        auto record = fh_->get_record(rmScan.rid(), context);
        // 构建组合索引键
        int offset = 0;
        for (auto& col : cols) {
            // 从记录中复制对应列的数据到索引键缓冲区
            std::memcpy(key + offset, record.get()->data + col.offset, col.len);
            offset += col.len;
        }

        // 将键值对插入B+树
        // 键：索引列值的组合
        // 值：记录的RID
        page_id_t res = INVALID_PAGE_ID;  // 初始化结果为无效页ID
        if (context == nullptr) {
            res = ih_->insert_entry_without_lock(key, rmScan.rid());
        } else {
            res = ih_->insert_entry(key, rmScan.rid(), context->txn_);
        }
        // 如果插入失败（可能是违反唯一性约束），回滚索引创建
        if (res == INVALID_PAGE_ID) {
            drop_index(tab_name, col_names, context);
            return;
        }
    }

    // 7. 更新内存中的索引信息
    ihs_.emplace(index_name, std::move(ih_));  // 保存索引句柄

    // 8. 更新表的元数据
    IndexMeta indexMeta = {
        tab_name,                       // 表名
        tot_col_len,                    // 索引键总长度
        static_cast<int>(cols.size()),  // 索引列数
        cols                            // 索引列元数据
    };
    tab.indexes.emplace_back(indexMeta);

    // 9. 持久化元数据变更
    flush_meta();
}

/**
 * @description: 删除指定表上的索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context 执行上下文
 * @throw IndexNotFoundError 如果索引不存在
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    //! DO
    auto&& index_name = ix_manager_->get_index_name(tab_name, col_names);
    if (!ix_manager_->exists_with_index_name(index_name)) {
        throw IndexNotFoundError(tab_name, col_names);
    }
    // 关闭并删除索引文件
    auto it = ihs_.find(index_name);
    if (it != ihs_.end()) {
        // 如果已经打开从缓冲池中删掉，并关闭文件
        ix_manager_->close_index_without_flush(it->second.get());
        ihs_.erase(it);
    }
    // 删除索引文件
    ix_manager_->destroy_index_with_index_name(index_name);
    // 从表的元数据中删除索引
    TabMeta& tab = db_.get_table(tab_name);
    auto index = tab.get_index_meta(col_names);
    tab.indexes.erase(index);
    flush_meta();
}

/**
 * @description: 删除索引的重载函数
 *
 * 该函数将列元数据转换为列名列表，然后调用另一个删除索引的函数。
 * 主要用于内部实现，提供了一种使用列元数据直接删除索引的方式。
 *
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} cols 索引包含的字段元数据
 * @param {Context*} context 执行上下文
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    //! DO
    std::vector<std::string> col_names;
    for (auto& col : cols) {
        col_names.push_back(col.name);
    }
    drop_index(tab_name, col_names, context);
}

bool SmManager::insert_index(const std::string& tab_name, RmRecord& rec, Rid rid, Context* context_) {
    TabMeta& tab_ = db_.get_table(tab_name);
    std::vector<std::unique_ptr<char[]>> inserted_keys;  // 记录已插入的键值
    inserted_keys.reserve(tab_.indexes.size());          // 预分配空间以提高性能

    // 遍历表的所有索引
    for (size_t i = 0; i < tab_.indexes.size(); ++i) {
        auto& index = tab_.indexes[i];
        // 获取索引句柄
        auto ih = ihs_.at(get_ix_manager()->get_index_name(tab_.name, index.cols)).get();

        // 构造索引键值
        auto key = std::make_unique<char[]>(index.col_tot_len);
        int offset = 0;
        for (size_t j = 0; j < static_cast<size_t>(index.col_num); ++j) {
            memcpy(key.get() + offset, rec.data + index.cols[j].offset, index.cols[j].len);
            offset += index.cols[j].len;
        }

        // 插入索引项
        auto res = ih->insert_entry(key.get(), rid, context_->txn_);
        if (res == INVALID_PAGE_ID) {
            // 插入失败，回滚已插入的索引
            for (size_t rollback_i = 0; rollback_i < i; ++rollback_i) {
                auto& rollback_index = tab_.indexes[rollback_i];
                auto rollback_ih = ihs_.at(get_ix_manager()->get_index_name(tab_.name, rollback_index.cols)).get();
                rollback_ih->delete_entry(inserted_keys[rollback_i].get(), context_->txn_);
            }
            return false;
        }
        inserted_keys.emplace_back(std::move(key));
    }
    return true;
}

bool SmManager::insert_index_without_rollback(const std::string& tab_name, RmRecord& rec, Rid rid, Context* context_) {
    TabMeta& tab_ = db_.get_table(tab_name);
    // 遍历表的所有索引
    for (size_t i = 0; i < tab_.indexes.size(); ++i) {
        auto& index = tab_.indexes[i];
        auto ih = ihs_.at(get_ix_manager()->get_index_name(tab_.name, index.cols)).get();
        auto key = std::make_unique<char[]>(index.col_tot_len);
        int offset = 0;
        for (size_t j = 0; j < static_cast<size_t>(index.col_num); ++j) {
            memcpy(key.get() + offset, rec.data + index.cols[j].offset, index.cols[j].len);
            offset += index.cols[j].len;
        }
        // 插入索引项
        auto res = ih->insert_entry(key.get(), rid, context_->txn_);
        if (res == INVALID_PAGE_ID) {
            return false;
        }
    }
    return true;
}

bool SmManager::delete_index(const std::string& tab_name, RmRecord& rec, Context* context_) {
    TabMeta& tab_ = db_.get_table(tab_name);
    // 遍历表的所有索引
    for (size_t i = 0; i < tab_.indexes.size(); ++i) {
        auto& index = tab_.indexes[i];
        auto ih = ihs_.at(get_ix_manager()->get_index_name(tab_.name, index.cols)).get();
        auto key = std::make_unique<char[]>(index.col_tot_len);
        int offset = 0;
        for (size_t j = 0; j < static_cast<size_t>(index.col_num); ++j) {
            memcpy(key.get() + offset, rec.data + index.cols[j].offset, index.cols[j].len);
            offset += index.cols[j].len;
        }
        // 删除索引项
        ih->delete_entry(key.get(), context_->txn_);
    }
    return true;
}