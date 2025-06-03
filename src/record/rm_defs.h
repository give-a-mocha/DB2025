/**
 * @file rm_defs.h
 * @author RMDB Development Team
 * @brief 记录管理器的基础数据结构定义
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * 该文件定义了记录管理器的核心数据结构：
 * 1. 物理存储格式
 *    - 文件头：存储表级元数据
 *    - 页面头：存储页面级元数据
 *    - 记录结构：存储实际数据
 *
 * 2. 元组版本控制
 *    - 时间戳管理
 *    - 删除标记
 *    - MVCC支持
 *
 * 3. 内存管理
 *    - 记录的分配和释放
 *    - 序列化和反序列化
 *    - 引用计数和生命周期
 */

#pragma once

#include "defs.h"
#include "storage/buffer_pool_manager.h"

/** @brief 表示无效的页面号 */
constexpr int RM_NO_PAGE = -1;

/** @brief 文件头页面的页号，固定为0 */
constexpr int RM_FILE_HDR_PAGE = 0;

/** @brief 第一个存储记录的页面号，从1开始 */
constexpr int RM_FIRST_RECORD_PAGE = 1;

/** @brief 单条记录的最大大小(字节)
 *  @note 考虑到页面大小和存储效率，限制记录大小
 */
constexpr int RM_MAX_RECORD_SIZE = 512;

/**
 * @brief 元组元数据结构体
 *
 * 用于存储每个元组(记录)的元信息:
 * 1. 时间戳：标识元组的版本
 * 2. 删除标记：表示元组是否已被删除
 *
 * 主要用于:
 * 1. 支持MVCC并发控制
 * 2. 实现记录的软删除
 */
struct TupleMeta {
    timestamp_t ts_;   // 元组的时间戳
    bool is_deleted_;  // 元组是否被删除的标记

    /**
     * @brief 比较两个元组元数据是否相等
     * @param a 第一个元组元数据
     * @param b 第二个元组元数据
     * @return 两个元组元数据是否完全相等
     */
    friend auto operator==(const TupleMeta& a, const TupleMeta& b) {
        return a.ts_ == b.ts_ && a.is_deleted_ == b.is_deleted_;
    }

    /**
     * @brief 比较两个元组元数据是否不相等
     * @param a 第一个元组元数据
     * @param b 第二个元组元数据
     * @return 两个元组元数据是否存在不同
     */
    friend auto operator!=(const TupleMeta& a, const TupleMeta& b) { return !(a == b); }
};

/* 文件头，记录表数据文件的元信息，写入磁盘中文件的第0号页面 */
/**
 * @brief 表数据文件的文件头
 *
 * 文件头存储在数据文件的第0页，包含表的整体信息：
 * 1. 记录格式信息
 *    - record_size: 单条记录大小
 *    - num_records_per_page: 每页最大记录数
 *    - bitmap_size: 位图大小(字节)
 *
 * 2. 存储统计信息
 *    - num_pages: 总页面数
 *    - record_num: 当前记录总数
 *
 * 3. 空间管理信息
 *    - first_free_page_no: 第一个有空闲空间的页号
 *
 * @note 文件头信息在表创建时初始化，并随表的修改而更新
 */
struct RmFileHdr {
    int record_size;           // 每条记录的固定大小
    int num_pages;             // 已分配的页面总数
    int num_records_per_page;  // 每页的最大记录数
    int first_free_page_no;    // 空闲页面链表的头部
    int bitmap_size;           // 每页位图的字节数
    int record_num;            // 表中的当前记录数
};

/**
 * @brief 数据页面的页头结构
 *
 * 每个数据页面的开始部分存储页面元信息：
 * 1. next_free_page_no
 *    - 指向下一个有空闲空间的页面
 *    - 形成空闲页面链表
 *    - -1表示链表结束
 *
 * 2. num_records
 *    - 当前页面中的记录数
 *    - 用于快速判断页面是否已满
 *    - 辅助位图管理
 *
 * @note 页头后紧接着是位图区域和记录存储区域
 */
struct RmPageHdr {
    int next_free_page_no;  // 空闲页面链表指针
    int num_records;        // 当前记录数
};

/* 表中的记录 */
/**
 * @brief 记录类，表示表中的一条记录
 *
 * 负责管理记录的数据存储和生命周期：
 * 1. 数据的分配和释放
 * 2. 数据的序列化和反序列化
 * 3. 数据的拷贝和移动
 */
struct RmRecord {
    char* data;               // 记录的数据
    int size;                 // 记录的大小
    bool allocated_ = false;  // 是否已经为数据分配空间

    RmRecord() = default;

    RmRecord(const RmRecord& other) {
        size = other.size;
        data = new char[size];
        memcpy(data, other.data, size);
        allocated_ = true;
    };

    RmRecord& operator=(const RmRecord& other) {
        size = other.size;
        data = new char[size];
        memcpy(data, other.data, size);
        allocated_ = true;
        return *this;
    };

    /**
     * @brief 构造指定大小的记录
     * @param size_ 记录的大小(字节)
     * @throw InvalidRecordSizeError 如果size超出限制
     *
     * @details 分配过程：
     * 1. 验证记录大小的合法性
     * 2. 分配指定大小的内存
     * 3. 设置分配标记
     *
     * @note 分配的内存会在析构时自动释放
     */
    RmRecord(int size_) {
        size = size_;
        data = new char[size_];
        allocated_ = true;
    }

    /**
     * @brief 构造指定大小的记录并初始化数据
     * @param size_ 记录的大小(字节)
     * @param data_ 初始数据指针
     * @throw InvalidRecordSizeError 如果size超出限制
     * @throw std::bad_alloc 如果内存分配失败
     *
     * @details 创建过程：
     * 1. 分配内存空间
     * 2. 复制初始数据
     * 3. 设置分配标记
     *
     * @warning data_指向的内存必须至少有size_字节
     */
    RmRecord(int size_, char* data_) {
        size = size_;
        data = new char[size_];
        memcpy(data, data_, size_);
        allocated_ = true;
    }

    /**
     * @brief 设置记录的数据内容
     * @param data_ 要设置的数据
     */
    void SetData(char* data_) { memcpy(data, data_, size); }

    /**
     * @brief 从序列化数据中恢复记录
     * @param data_ 序列化的数据缓冲区
     *
     * @details 反序列化过程：
     * 1. 读取记录大小(前4字节)
     * 2. 释放现有数据(如果已分配)
     * 3. 分配新的内存空间
     * 4. 复制实际数据
     *
     * @note 序列化格式：
     * [size(4字节)][data(size字节)]
     */
    void Deserialize(const char* data_) {
        size = *reinterpret_cast<const int*>(data_);
        if (allocated_) {
            delete[] data;
        }
        data = new char[size];
        memcpy(data, data_ + sizeof(int), size);
    }

    ~RmRecord() {
        if (allocated_) {
            delete[] data;
        }
        allocated_ = false;
        data = nullptr;
    }
};
