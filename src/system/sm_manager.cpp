/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @brief 创建一个新的数据库目录并初始化元数据和日志文件。
 *
 * 如果指定名称的数据库目录已存在，则抛出 DatabaseExistsError。成功时会在新目录下生成数据库元数据文件和日志文件，最后返回上级目录。系统调用失败时抛出 UnixError。
 *
 * @param db_name 数据库名称。
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
 * @brief 删除指定数据库及其所有相关文件和目录。
 *
 * 如果数据库目录不存在，则抛出 DatabaseNotFoundError。删除过程中如遇系统调用失败，则抛出 UnixError。
 *
 * @param db_name 数据库名称（对应文件夹名）。
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
 * @brief 打开指定数据库，加载其元数据并初始化所有表和索引的文件句柄。
 *
 * @param db_name 数据库名称，应与数据库文件夹同名。
 *
 * @throws DatabaseNotFoundError 如果数据库目录不存在。
 * @throws UnixError 如果切换目录失败。
 *
 * 此方法会进入数据库目录，读取数据库元数据文件，并为所有表和索引（索引功能为占位）打开文件句柄，准备后续操作。
 */
void SmManager::open_db(const std::string& db_name) {
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
            // !索引暂未实现，只是作为占位
            auto&& index_name =
                ix_manager_->get_index_name(table_name, index.cols);
            ihs_.emplace(index_name,
                         ix_manager_->open_index(index_name, index.cols));
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @brief 关闭当前数据库并将元数据写入磁盘。
 *
 * 刷新数据库元数据，关闭所有表和索引的文件句柄，清理内部状态，并切换回上级目录。
 * 若切换目录失败则抛出 UnixError 异常。
 */
void SmManager::close_db() {
    // 刷新元数据到磁盘
    flush_meta();

    // 关闭所有的文件句柄
    for (auto& fh : fhs_) {
        rm_manager_->close_file(fh.second.get());
    }

    // !关闭索引文件句柄，索引暂未实现，只是作为占位
    for (auto& ih : ihs_) {
        ix_manager_->close_index(ih.second.get());
    }

    db_.name_.clear();
    db_.tabs_.clear();
    fhs_.clear();

    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @brief 显示当前数据库中的所有表名，并将结果追加写入 output.txt 文件。
 *
 * 结果以表格形式输出到控制台，并同步写入 output.txt，便于测试和结果验证。
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

/**
 * @brief 显示指定数据表的字段元数据信息。
 *
 * 展示表的所有字段名称、类型及是否存在索引，并以表格形式输出到指定上下文。
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
        std::vector<std::string> field_info = {col.name, coltype2str(col.type),
                                               col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @brief 创建一个新表，并在数据库中注册其元数据。
 *
 * 如果指定表名已存在，则抛出 TableExistsError。根据字段定义生成表元数据，创建对应的记录文件，并将表信息写入数据库元数据。
 *
 * @param tab_name 新建表的名称。
 * @param col_defs 表字段定义列表。
 */
void SmManager::create_table(const std::string& tab_name,
                             const std::vector<ColDef>& col_defs,
                             Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto& col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col
    // meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @brief 删除指定的表及其所有相关索引和元数据。
 *
 * 如果表存在，先删除其所有索引，再关闭并删除表文件，最后从数据库元数据中移除该表并刷新元数据到磁盘。若 context 不为空，将在删除前对表加排他锁。若表不存在则抛出 TableNotFoundError 异常。
 *
 * @param tab_name 要删除的表名。
 * @param context 可选的上下文对象，用于加锁等操作。
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    // 检查表是否存在
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    if (context != nullptr) {
        context->lock_mgr_->lock_exclusive_on_table(context->txn_,
                                                    fhs_[tab_name]->GetFd());
    }

    // 获取表元数据
    TabMeta& tab_meta = db_.get_table(tab_name);

    // 删除表的所有索引
    for (auto& index : tab_meta.indexes) {
        drop_index(tab_name, index.cols, context);
    }

    // 关闭并删除表文件
    if (fhs_.count(tab_name)) {
        rm_manager_->close_file(fhs_[tab_name].get());
        fhs_.erase(tab_name);
    }

    rm_manager_->destroy_file(tab_name);

    // 从数据库元数据中删除表
    db_.tabs_.erase(tab_name);

    // 刷新元数据到磁盘
    flush_meta();
}

/**
                              * @brief 为指定表创建索引（占位方法，当前未实现）。
                              *
                              * @param tab_name 目标表名。
                              * @param col_names 组成索引的字段名列表。
                              * @param context 操作上下文。
                              */
void SmManager::create_index(const std::string& tab_name,
                             const std::vector<std::string>& col_names,
                             Context* context) {}

/**
                            * @brief 删除指定表上由字段名列表定义的索引（占位实现）。
                            *
                            * @param tab_name 目标表名。
                            * @param col_names 索引涉及的字段名列表。
                            * @param context 操作上下文。
                            *
                            * @note 当前方法为占位符，未实现实际索引删除逻辑。
                            */
void SmManager::drop_index(const std::string& tab_name,
                           const std::vector<std::string>& col_names,
                           Context* context) {}

/**
 * @brief 删除指定表上由给定字段组成的索引（占位实现）。
 *
 * 当前方法为占位符，尚未实现实际的索引删除逻辑。
 *
 * @param tab_name 目标表名。
 * @param cols 组成索引的字段元数据列表。
 */
void SmManager::drop_index(const std::string& tab_name,
                           const std::vector<ColMeta>& cols, Context* context) {

}