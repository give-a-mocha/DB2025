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

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "errors.h"
#include "sm_defs.h"

/**
 * @brief 字段(列)的元数据定义
 * @note 支持序列化和反序列化，用于元数据持久化
 */
struct ColMeta {
    std::string tab_name;  // 字段所属表名称
    std::string name;      // 字段名称
    ColType type;          // 字段类型
    int len;               // 字段长度
    int offset;            // 字段位于记录中的偏移量
    bool index;            /** unused */
    AggregateType agg_type{AggregateType::NONE};  // 聚合类型，默认为NONE

    friend std::ostream &operator<<(std::ostream &os, const ColMeta &col) {
        // ColMeta中有各个基本类型的变量，然后调用重载的这些变量的操作符<<（具体实现逻辑在defs.h）
        return os << col.tab_name << ' ' << col.name << ' ' << col.type << ' ' << col.len << ' ' << col.offset << ' '
                  << col.index << ' ' << col.agg_type;
    }

    friend std::istream &operator>>(std::istream &is, ColMeta &col) {
        return is >> col.tab_name >> col.name >> col.type >> col.len >> col.offset >> col.index >> col.agg_type;
    }
};

/**
 * @brief 索引的元数据定义
 * @note 主要用于：
 * 1. 索引的创建和维护
 * 2. 优化器使用索引的决策
 * 3. 支持联合索引
 */
struct IndexMeta {
    std::string tab_name;       // 索引所属表名称
    int col_tot_len;            // 索引字段长度总和
    int col_num;                // 索引字段数量
    std::vector<ColMeta> cols;  // 索引包含的字段

    friend std::ostream &operator<<(std::ostream &os, const IndexMeta &index) {
        os << index.tab_name << " " << index.col_tot_len << " " << index.col_num;
        for (auto &col : index.cols) {
            os << "\n" << col;
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, IndexMeta &index) {
        is >> index.tab_name >> index.col_tot_len >> index.col_num;
        for (int i = 0; i < index.col_num; ++i) {
            ColMeta col;
            is >> col;
            index.cols.push_back(col);
        }
        return is;
    }
};

/**
 * @brief 表的元数据定义
 * @note 支持以下操作：
 * 1. 复制构造，便于临时操作
 * 2. 列和索引的快速查找
 * 3. 序列化，用于持久化存储
 */
struct TabMeta {
    std::string name;                // 表名称
    std::vector<ColMeta> cols;       // 表包含的字段
    std::vector<IndexMeta> indexes;  // 表上建立的索引

    TabMeta() {}

    TabMeta(const TabMeta &other) {
        name = other.name;
        for (auto col : other.cols) cols.push_back(col);
    }

    /* 判断当前表中是否存在名为col_name的字段 */
    bool is_col(const std::string &col_name) const {
        auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) { return col.name == col_name; });
        return pos != cols.end();
    }

    /* 判断当前表上是否建有指定索引，索引包含的字段为col_names */
    bool is_index(const std::vector<std::string> &col_names) const {
        for (auto &index : indexes) {
            if (index.col_num == static_cast<int>(col_names.size())) {
                size_t i = 0;
                for (; i < static_cast<size_t>(index.col_num); ++i) {
                    if (index.cols[i].name.compare(col_names[i]) != 0) break;
                }
                if (i == static_cast<size_t>(index.col_num)) return true;
            }
        }

        return false;
    }

    /* 根据字段名称集合获取索引元数据 */
    std::vector<IndexMeta>::iterator get_index_meta(const std::vector<std::string> &col_names) {
        for (auto index = indexes.begin(); index != indexes.end(); ++index) {
            if ((*index).col_num != static_cast<int>(col_names.size())) continue;
            auto &index_cols = (*index).cols;
            size_t i = 0;
            for (; i < col_names.size(); ++i) {
                if (index_cols[i].name.compare(col_names[i]) != 0) break;
            }
            if (i == col_names.size()) return index;
        }
        throw IndexNotFoundError(name, col_names);
    }

    /* 根据字段名称获取字段元数据 */
    std::vector<ColMeta>::iterator get_col(const std::string &col_name) {
        auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) { return col.name == col_name; });
        if (pos == cols.end()) {
            throw ColumnNotFoundError(col_name);
        }
        return pos;
    }

    friend std::ostream &operator<<(std::ostream &os, const TabMeta &tab) {
        os << tab.name << '\n' << tab.cols.size() << '\n';
        for (auto &col : tab.cols) {
            os << col << '\n';  // col是ColMeta类型，然后调用重载的ColMeta的操作符<<
        }
        os << tab.indexes.size() << "\n";
        for (auto &index : tab.indexes) {
            os << index << "\n";
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, TabMeta &tab) {
        size_t n;
        is >> tab.name >> n;
        for (size_t i = 0; i < n; i++) {
            ColMeta col;
            is >> col;
            tab.cols.push_back(col);
        }
        is >> n;
        for (size_t i = 0; i < n; ++i) {
            IndexMeta index;
            is >> index;
            tab.indexes.push_back(index);
        }
        return is;
    }
};

/**
 * @brief 数据库的元数据定义
 * @note 实现特点：
 * 1. 使用map高效管理表信息
 * 2. 支持元数据持久化
 * 3. 提供表级别的原子操作
 */
class DbMeta {
    friend class SmManager;
    friend class RecoveryManager;

   private:
    std::string name_;                     // 数据库名称
    std::map<std::string, TabMeta> tabs_;  // 数据库中包含的表

    size_t log_offset_ = 0;  // 日志偏移量 用于故障恢复

   public:
    // DbMeta(std::string name) : name_(name) {}

    /* 判断数据库中是否存在指定名称的表 */
    bool is_table(const std::string &tab_name) const { return tabs_.find(tab_name) != tabs_.end(); }

    void SetTabMeta(const std::string &tab_name, const TabMeta &meta) { tabs_[tab_name] = meta; }

    /* 获取指定名称表的元数据 */
    TabMeta &get_table(const std::string &tab_name) {
        auto pos = tabs_.find(tab_name);
        if (pos == tabs_.end()) {
            throw TableNotFoundError(tab_name);
        }

        return pos->second;
    }

    size_t get_log_offset() const { return log_offset_; }

    // 重载操作符 <<
    friend std::ostream &operator<<(std::ostream &os, const DbMeta &db_meta) {
        os << db_meta.name_ << '\n' << db_meta.tabs_.size() << '\n';
        for (auto &entry : db_meta.tabs_) {
            os << entry.second << '\n';
        }
        os << db_meta.log_offset_ << '\n';  // 输出日志偏移量
        return os;
    }

    friend std::istream &operator>>(std::istream &is, DbMeta &db_meta) {
        size_t n;
        is >> db_meta.name_ >> n;
        for (size_t i = 0; i < n; i++) {
            TabMeta tab;
            is >> tab;
            db_meta.tabs_[tab.name] = tab;
        }
        is >> db_meta.log_offset_;
        return is;
    }
};
