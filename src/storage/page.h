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

#include <shared_mutex>
#include "common/config.h"
#include "common/debug_shared_mutex.h"

/**
 * @brief 页面标识符结构体
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
 */
class Page {
    friend class BufferPoolManager;

   public:
    Page() { reset_memory(); }

    ~Page() = default;

    PageId get_page_id() const { return id_; }

    inline char *get_data() { return data_; }

    bool is_dirty() const { return is_dirty_; }

    void wlatch() { 
        rw_latch_.lock();
        // rw_latch_debug_->lock();
    }

    void wunlatch() {
        rw_latch_.unlock(); 
        // rw_latch_debug_->unlock();
    }

    void rlatch() {
        rw_latch_.lock_shared(); 
        // rw_latch_debug_->lock_shared();
    }

    void runlatch() {
        rw_latch_.unlock_shared(); 
        // rw_latch_debug_->unlock_shared();
    }

    static constexpr size_t OFFSET_PAGE_START = 0;
    static constexpr size_t OFFSET_LSN = 0;
    static constexpr size_t OFFSET_PAGE_HDR = 4;

    inline lsn_t get_page_lsn() { return *reinterpret_cast<lsn_t *>(get_data() + OFFSET_LSN); }

    inline void set_page_lsn(lsn_t page_lsn) { memcpy(get_data() + OFFSET_LSN, &page_lsn, sizeof(lsn_t)); }

    void reset_latch() { 
        rw_latch_.~shared_mutex();  // 销毁旧的锁
        new (&rw_latch_) std::shared_mutex();  // 重新构造新的锁
        // 重新初始化调试锁
        // rw_latch_debug_ = std::make_unique<DebugSharedMutex>("page_" + std::to_string(id_.page_no));
    }

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

    /** The read-write latch of this page. */
    std::shared_mutex rw_latch_;

    // std::unique_ptr<DebugSharedMutex> rw_latch_debug_;
};