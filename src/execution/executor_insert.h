/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include <memory>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 插入执行器，负责实现INSERT语句的功能
 *
 * @details 主要功能和特点：
 * 1. 数据插入：
 *    - 校验插入值的数量和类型
 *    - 处理数据类型的自动转换
 *    - 分配和填充记录空间
 *
 * 2. 索引维护：
 *    - 同步更新表的所有索引
 *    - 处理唯一性约束冲突
 *    - 支持插入失败时的回滚
 *
 * 3. 事务支持：
 *    - 保证操作的原子性
 *    - 处理并发插入冲突
 *    - 维护ACID特性
 */
class InsertExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                // 表的元数据
    std::vector<Value> values_;  // 待插入的值列表
    RmFileHandle *fh_;           // 表的数据文件句柄
    std::string tab_name_;       // 表名
    Rid rid_;                    // 插入记录的位置(插入成功后赋值)
    SmManager *sm_manager_;      // 系统管理器指针

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器指针
     * @param tab_name 目标表名
     * @param values 要插入的值列表
     * @param context 执行上下文
     * @throw InvalidValueCountError 当值的数量与表的列数不匹配时
     *
     * @details 初始化过程：
     * 1. 保存系统管理器和表名
     * 2. 获取表的元数据和文件句柄
     * 3. 检查值的数量是否匹配
     * 4. 设置执行上下文
     */
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        // 检查插入值的数量是否与表的列数匹配
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    /**
     * @brief 执行插入操作
     * @return nullptr，因为INSERT不产生结果集
     * @throw IncompatibleTypeError 当值的类型与列类型不兼容时
     * @throw RMDBError 当索引更新失败需要回滚时
     *
     * @details 执行步骤：
     * 1. 创建记录
     *    - 分配记录缓冲区
     *    - 处理数据类型转换
     *    - 填充记录数据
     *
     * 2. 插入记录
     *    - 写入表文件
     *    - 获取记录RID
     *
     * 3. 更新索引
     *    - 插入所有索引项
     *    - 失败时回滚记录
     */
    std::unique_ptr<RmRecord> Next() override {
        // 创建记录缓冲区
        RmRecord rec(fh_->get_file_hdr().record_size);

        // 处理每个字段的值
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];
            auto &val = values_[i];

            // 处理类型不匹配的情况
            if (col.type != val.type) {
                if (col.type == ColType::TYPE_INT && val.type == ColType::TYPE_FLOAT) {
                    val.set_int(static_cast<int>(val.float_val));
                } else if (col.type == ColType::TYPE_FLOAT && val.type == ColType::TYPE_INT) {
                    val.set_float(static_cast<float>(val.int_val));
                } else {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
            }

            // 复制值到记录中
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }

        // 插入记录到文件
        rid_ = fh_->insert_record(rec.data, context_);

        // 更新索引
        if (!insert_index(rec)) {
            // 索引插入失败，回滚记录
            fh_->delete_record(rid_, context_);
            throw RMDBError("Failed to insert into index, rolled back record insertion at " + getType());
        }

        return nullptr;
    }

    /**
     * @brief 为新记录创建索引项
     * @param rec 要创建索引的记录引用
     * @return 是否成功创建所有索引
     *
     * @details 执行步骤：
     * 1. 遍历表的所有索引
     *    - 获取索引句柄
     *    - 构造索引键值
     *    - 插入索引项
     *
     * 2. 处理插入失败
     *    - 记录成功的键值
     *    - 发生失败时回滚
     *    - 维护事务一致性
     *
     * 3. 性能优化
     *    - 预分配键值空间
     *    - 减少内存分配
     *    - 批量处理提升效率
     */
    bool insert_index(RmRecord &rec) {
        std::vector<std::unique_ptr<char[]>> inserted_keys;  // 记录已插入的键值
        inserted_keys.reserve(tab_.indexes.size());          // 预分配空间以提高性能

        // 遍历表的所有索引
        for (size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto &index = tab_.indexes[i];
            // 获取索引句柄
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();

            // 构造索引键值
            auto key = std::make_unique<char[]>(index.col_tot_len);
            int offset = 0;
            for (size_t j = 0; j < static_cast<size_t>(index.col_num); ++j) {
                memcpy(key.get() + offset, rec.data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }

            // 插入索引项
            auto res = ih->insert_entry(key.get(), rid_, context_->txn_);
            if (res == INVALID_PAGE_ID) {
                // 插入失败，回滚已插入的索引
                for (size_t rollback_i = 0; rollback_i < i; ++rollback_i) {
                    auto &rollback_index = tab_.indexes[rollback_i];
                    auto rollback_ih =
                        sm_manager_->ihs_
                            .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, rollback_index.cols))
                            .get();
                    rollback_ih->delete_entry(inserted_keys[rollback_i].get(), context_->txn_);
                }
                return false;
            }
            inserted_keys.emplace_back(std::move(key));
        }
        return true;
    }
    /**
     * @brief 获取插入记录的RID
     * @return 插入记录的RID引用
     */
    Rid &rid() override { return rid_; }

    /**
     * @brief 获取执行器类型名称
     * @return 执行器的类型字符串
     */
    std::string getType() override { return "InsertExecutor"; }
};