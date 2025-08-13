/**
 * @file rm_file_handle.h
 * @author RMDB Development Team
 * @brief 表记录文件的页面句柄和管理器
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * 该文件定义了记录管理器的核心数据结构：
 * 1. RmPageHandle：单个页面的句柄
 *    - 提供对页面内容的访问接口
 *    - 管理页面的内部布局
 *    - 维护页面的位图和槽位
 *
 * 2. RmFileHandle：表数据文件的管理器
 *    - 提供记录的CRUD操作
 *    - 管理页面的分配和回收
 *    - 维护文件的元数据
 *    - 处理并发访问和事务
 */

#pragma once

#include <assert.h>

#include <memory>

#include "bitmap.h"
#include "common/context.h"
#include "rm_defs.h"
#include "storage/page_guard.h"

extern DiskManager disk_manager;
extern BufferPoolManager buffer_pool_manager;

class RmManager;

/**
 * @brief 表数据文件中单个页面的句柄类
 *
 * 对表数据文件中的页面进行封装，提供对页面内容的访问和管理。
 * 页面内部结构分为三个部分：
 * 1. 页面元信息（page_hdr）：存储页面的基本信息
 * 2. 位图（bitmap）：记录每个槽位是否被使用
 * 3. 数据槽位（slots）：存储实际的记录数据
 */
struct RmPageHandle {
    const int record_size;
    RmPageHdr *page_hdr;  // 页面头部信息，指向页面数据的第一部分，存储页面级别的元数据
    char *bitmap;         // 页面的位图区域，用于跟踪槽位的使用情况
    char *slots;          // 实际记录存储区域，每个槽位存储一条记录

    /**
     * @brief 页面句柄构造函数
     * @param fhdr_ 文件头指针
     * @param page_ 页面指针
     *
     * @details 页面内部布局：
     * +-------------------------+
     * |     Page Header        |  -- page_hdr指向这里
     * +-------------------------+
     * |      Bitmap            |  -- bitmap指向这里
     * +-------------------------+
     * |       Slots            |  -- slots指向这里
     * |         ...            |
     * +-------------------------+
     */
    RmPageHandle(const RmFileHdr *fhdr_, char *data_) : record_size(fhdr_->record_size) {
        // 1. 初始化页面头指针（跳过页面预留的头部空间）
        page_hdr = reinterpret_cast<RmPageHdr *>(data_ + OFFSET_PAGE_HDR);
        // 2. 初始化位图指针（跳过页面头部结构）
        bitmap = data_ + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        // 3. 初始化记录槽位指针（跳过位图区域）
        slots = bitmap + fhdr_->bitmap_size;
    }

    // 返回指定slot_no的slot存储收地址
    /**
     * @brief 获取指定槽位的记录指针
     * @param slot_no 槽位号
     * @return char* 槽位对应的记录指针
     * @note 计算方式：
     * 1. 起始地址为slots（记录存储区的起始位置）
     * 2. 偏移量为：槽位号 * 记录大小
     * @warning 调用前应确保槽位号有效，否则可能导致越界访问
     */
    char *get_slot(int slot_no) const { return slots + slot_no * (TUPLE_META_SIZE + record_size); }

    TupleMeta get_tuple_meta(int slot_no) const {
        // 返回指定槽位的元组元数据指针
        return *reinterpret_cast<TupleMeta *>(get_slot(slot_no));
    }

    std::pair<TupleMeta, std::unique_ptr<RmRecord>> get_tuple(int slot_no) const {
        // 返回指定槽位的元组元数据和记录数据指针
        char *slot_data = get_slot(slot_no);
        TupleMeta meta = *reinterpret_cast<TupleMeta *>(slot_data);
        char *record_data = slot_data + TUPLE_META_SIZE;
        auto record = std::make_unique<RmRecord>(record_size, record_data);
        return {meta, std::move(record)};
    }
};

/**
 * @brief 表数据文件的管理器类
 *
 * 负责管理单个表的数据文件，提供记录的增删改查操作。
 * 主要功能：
 * 1. 管理文件中的所有数据页面
 * 2. 提供记录级别的操作接口
 * 3. 维护文件的元数据信息
 * 4. 协调磁盘管理器和缓冲池管理器
 */
class RmFileHandle {
    friend class RmScan;     // 允许记录扫描器访问内部成员
    friend class RmManager;  // 允许记录管理器访问内部成员
    friend class SmManager;  // 允许系统管理器访问内部成员

   private:
    int fd_;              // 文件描述符，唯一标识打开的文件
    RmFileHdr file_hdr_;  // 文件头结构，包含：
                          // - record_size: 记录大小
                          // - num_pages: 总页面数
                          // - num_records_per_page: 每页记录数
                          // - first_free_page_no: 第一个可用页面号
                          // - bitmap_size: 每页位图大小

    std::mutex latch_;  // 互斥锁，用于获取下一个空闲页的保护

   public:
    /**
     * @brief 文件句柄构造函数
     * @param disk_manager 磁盘管理器
     * @param buffer_pool_manager 缓冲池管理器
     * @param fd 文件描述符
     * @warning 确保文件头页面(page_no=0)已经正确初始化，
     * 否则可能导致文件结构损坏
     */
    RmFileHandle(int fd) : fd_(fd) {
        // 1. 从磁盘读取文件头信息到内存中
        disk_manager.read_page(fd, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));

        // 2. 设置页面分配的起始编号，确保不会重复分配已使用的页面号
        disk_manager.set_fd2pageno(fd, file_hdr_.num_pages);
    }

    const RmFileHdr &get_file_hdr() const { return file_hdr_; }
    int GetFd() { return fd_; }

    /**
     * @brief 获取指定记录
     * @param rid 记录ID
     * @param context 事务上下文，包含事务信息和锁管理器
     * @return 返回记录对象的智能指针
     * @throw RecordNotFoundError 如果记录不存在
     * @note 返回的是记录的副本，对其修改不会影响原始数据
     */
    std::pair<TupleMeta, std::unique_ptr<RmRecord>> get_record(const Rid &rid) const;

    int get_record_size() const {
        return file_hdr_.record_size;  // 返回每条记录的大小
    }

    /**
     * @brief 插入新记录
     * @param buf 记录数据缓冲区
     * @param context 事务上下文，包含事务信息和锁管理器
     * @return 新记录的RID（页面号和槽位号）
     * @throw OutOfSpaceError 如果没有足够的空间
     * @note 系统自动分配插入位置，返回的RID用于后续访问
     */
    Rid insert_record(TupleMeta &new_meta, char *buf);

    /**
     * @brief 在指定位置插入记录
     * @param rid 指定的记录ID
     * @param buf 记录数据
     */
    void insert_record_force(const Rid &rid, TupleMeta &new_meta, char *buf);

    /**
     * @brief 删除记录
     * @param rid 要删除的记录ID
     * @param context 事务上下文
     */
    void delete_record(const Rid &rid);
    /**
     * @brief 更新记录
     * @param rid 要更新的记录ID
     * @param buf 新的记录数据
     * @param context 事务上下文
     */
    void update_record(const Rid &rid, TupleMeta &new_meta, char *buf);

    /**
     * @brief 更新指定记录的 TupleMeta
     * @param rid 要更新的记录ID
     * @param new_meta 新的 TupleMeta 数据
     * @throw RecordNotFoundError 如果记录不存在 (Optional, depending on desired behavior)
     */
    void update_tuple_meta(const Rid &rid, const TupleMeta &new_meta);

    auto AcquirePageReadLock(const Rid &rid) const -> ReadPageGuard;

    auto AcquirePageWriteLock(const Rid &rid) -> WritePageGuard;

    auto GetNewWritePageGuard() -> WritePageGuard;

    auto GetNewRid() -> Rid;

    auto GetTupleWithLockAcquired(const Rid &rid,
                                  const char *page_data) const -> std::pair<TupleMeta, std::unique_ptr<RmRecord>>;

    auto GetTupleMetaWithLockAcquired(const Rid &rid, const char *page_data) const -> TupleMeta;

    auto UpdateTupleWithLockAcquired(const Rid &rid, TupleMeta &meta, char *buf, char *page_data) -> void;

    auto UpdateTupleMetaWithLockAcquired(const Rid &rid, const TupleMeta &new_meta, char *page_data) -> void;
};