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

#include "common/config.h"

/**
 * @brief 页面标识符结构体
 *
 * @details 用于唯一标识一个页面：
 * 1. fd：文件描述符，标识页面所在的物理文件
 * 2. page_no：页号，标识文件内的具体页面
 *
 * @note 设计特点：
 * 1. 支持比较操作，用于容器排序
 * 2. 提供哈希函数，支持哈希表存储
 * 3. 可序列化为字符串，便于调试
 */
struct PageId {
    int fd;  //  Page所在的磁盘文件开启后的文件描述符, 来定位打开的文件在内存中的位置
    page_id_t page_no = INVALID_PAGE_ID;

    friend bool operator==(const PageId &x, const PageId &y) { return x.fd == y.fd && x.page_no == y.page_no; }
    bool operator<(const PageId &x) const {
        if (fd < x.fd) return true;
        return page_no < x.page_no;
    }

    std::string toString() { return "{fd: " + std::to_string(fd) + " page_no: " + std::to_string(page_no) + "}"; }

    inline int64_t Get() const { return (static_cast<int64_t>(fd << 16) | page_no); }
};

/**
 * @brief PageId的自定义哈希算法
 *
 * @details 实现方式：
 * 1. 将fd左移16位与page_no组合
 * 2. 生成唯一的哈希值
 *
 * @note 主要用于：
 * 1. 构建unordered_map<PageId, frame_id_t>
 * 2. 实现O(1)的页面查找
 */
struct PageIdHash {
    size_t operator()(const PageId &x) const { return (x.fd << 16) | x.page_no; }
};

template <>
struct std::hash<PageId> {
    size_t operator()(const PageId &obj) const { return std::hash<int64_t>()(obj.Get()); }
};

/**
 * @brief 数据库的基本存储单元
 *
 * @details Page类的设计特点：
 * 1. 存储管理
 *    - 固定大小的数据块(PAGE_SIZE)
 *    - 包含页面头部和数据区域
 *    - 支持LSN(日志序列号)管理
 *
 * 2. 状态维护
 *    - 脏页标记(is_dirty_)
 *    - 引用计数(pin_count_)
 *    - 页面ID管理
 *
 * 3. 内存布局
 *    - OFFSET_PAGE_START: 页面起始位置
 *    - OFFSET_LSN: LSN存储位置
 *    - OFFSET_PAGE_HDR: 页面头部起始位置
 *
 * @note 重要说明：
 * 1. Page对象可能同时存在于磁盘和缓冲池
 * 2. 通过pin_count_控制页面驻留
 * 3. 使用is_dirty_标记是否需要写回
 *
 * @thread_safety 线程安全由BufferPoolManager保证
 */
class Page {
    friend class BufferPoolManager;

   public:
    Page() { reset_memory(); }

    ~Page() = default;

    PageId get_page_id() const { return id_; }

    inline char *get_data() { return data_; }

    bool is_dirty() const { return is_dirty_; }

    static constexpr size_t OFFSET_PAGE_START = 0;
    static constexpr size_t OFFSET_LSN = 0;
    static constexpr size_t OFFSET_PAGE_HDR = 4;

    inline lsn_t get_page_lsn() { return *reinterpret_cast<lsn_t *>(get_data() + OFFSET_LSN); }

    inline void set_page_lsn(lsn_t page_lsn) { memcpy(get_data() + OFFSET_LSN, &page_lsn, sizeof(lsn_t)); }

   private:
    void reset_memory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); }  // 将data_的PAGE_SIZE个字节填充为0

    /** page的唯一标识符 */
    PageId id_;

    /** The actual data that is stored within a page.
     *  该页面在bufferPool中的偏移地址
     */
    char data_[PAGE_SIZE] = {};

    /** 脏页判断 */
    bool is_dirty_ = false;

    /** The pin count of this page. */
    int pin_count_ = 0;
};