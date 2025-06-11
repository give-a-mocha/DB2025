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
    const RmFileHdr *file_hdr;  // 当前页面所在文件的文件头指针，包含表的元数据信息
    Page *page;                 // 底层页面对象，负责实际的数据存储
    RmPageHdr *page_hdr;        // 页面头部信息，指向页面数据的第一部分，存储页面级别的元数据
    char *bitmap;               // 页面的位图区域，用于跟踪槽位的使用情况
    char *slots;                // 实际记录存储区域，每个槽位存储一条记录

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
    RmPageHandle(const RmFileHdr *fhdr_, Page *page_) : file_hdr(fhdr_), page(page_) {
        // 1. 初始化页面头指针（跳过页面预留的头部空间）
        page_hdr = reinterpret_cast<RmPageHdr *>(page->get_data() + page->OFFSET_PAGE_HDR);
        // 2. 初始化位图指针（跳过页面头部结构）
        bitmap = page->get_data() + sizeof(RmPageHdr) + page->OFFSET_PAGE_HDR;
        // 3. 初始化记录槽位指针（跳过位图区域）
        slots = bitmap + file_hdr->bitmap_size;
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
    char* get_slot(int slot_no) const {
        return slots + slot_no * file_hdr->record_size;
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
    friend class RmScan;    // 允许记录扫描器访问内部成员
    friend class RmManager; // 允许记录管理器访问内部成员

   private:
   DiskManager *disk_manager_;          // 磁盘管理器，负责文件的创建、删除和读写
   BufferPoolManager *buffer_pool_manager_;  // 缓冲池管理器，负责页面的缓存和淘汰
   int fd_;                             // 文件描述符，唯一标识打开的文件
   RmFileHdr file_hdr_;                 // 文件头结构，包含：
                                       // - record_size: 记录大小
                                       // - num_pages: 总页面数
                                       // - num_records_per_page: 每页记录数
                                       // - first_free_page_no: 第一个可用页面号
                                       // - bitmap_size: 每页位图大小

   public:
   /**
    * @brief 文件句柄构造函数
    * @param disk_manager 磁盘管理器
    * @param buffer_pool_manager 缓冲池管理器
    * @param fd 文件描述符
    * @warning 确保文件头页面(page_no=0)已经正确初始化，
    * 否则可能导致文件结构损坏
    */
   RmFileHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
       : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
       // 1. 从磁盘读取文件头信息到内存中
       disk_manager_->read_page(fd, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
       
       // 2. 设置页面分配的起始编号，确保不会重复分配已使用的页面号
       disk_manager_->set_fd2pageno(fd, file_hdr_.num_pages);
   }

    RmFileHdr get_file_hdr() { return file_hdr_; }
    int GetFd() { return fd_; }

    /**
     * @brief 检查指定RID位置是否存在有效记录
     * @param rid 记录ID，包含页面号和槽位号
     * @return true 如果记录存在，false 否则
     * @note 此函数不会加载记录数据，只检查位图
     * @warning 确保提供的RID在合法范围内，否则可能导致越界访问
     */
    bool is_record(const Rid &rid) const {
        RmPageHandle page_handle = fetch_page_handle(rid.page_no);
        return Bitmap::is_set(page_handle.bitmap, rid.slot_no);
    }

    /**
     * @brief 获取指定记录
     * @param rid 记录ID
     * @param context 事务上下文，包含事务信息和锁管理器
     * @return 返回记录对象的智能指针
     * @throw RecordNotFoundError 如果记录不存在
     * @note 返回的是记录的副本，对其修改不会影响原始数据
     */
    std::unique_ptr<RmRecord> get_record(const Rid &rid, Context *context) const;

    /**
     * @brief 插入新记录
     * @param buf 记录数据缓冲区
     * @param context 事务上下文，包含事务信息和锁管理器
     * @return 新记录的RID（页面号和槽位号）
     * @throw OutOfSpaceError 如果没有足够的空间
     * @note 系统自动分配插入位置，返回的RID用于后续访问
     */
     Rid insert_record(char *buf, Context *context);

    /**
     * @brief 在指定位置插入记录
     * @param rid 指定的记录ID
     * @param buf 记录数据
     */
    void insert_record(const Rid &rid, char *buf);

    /**
     * @brief 删除记录
     * @param rid 要删除的记录ID
     * @param context 事务上下文
     */
    void delete_record(const Rid &rid, Context *context);

    /**
     * @brief 更新记录 
     * @param rid 要更新的记录ID
     * @param buf 新的记录数据
     * @param context 事务上下文
     */
    void update_record(const Rid &rid, char *buf, Context *context);

    /**
     * @brief 创建新的页面句柄
     * @return 新页面的句柄
     * @throw OutOfMemoryError 如果无法分配新页面
     * @note 新页面的所有槽位初始状态为未使用
     */
    RmPageHandle create_new_page_handle();

    /**
     * @brief 获取指定页面的句柄
     * @param page_no 页面号
     * @return 页面句柄
     * @throw InvalidPageError 如果页面号无效
     * @note 页面会被固定在缓冲池中，使用完后应调用release_page_handle释放
     */
    RmPageHandle fetch_page_handle(int page_no) const;

    private:
    /**
     * @brief 创建页面句柄的内部方法
     * @return 新创建的页面句柄
     * @throw OutOfMemoryError 如果无法分配新页面
     * @note 这是一个底层方法，通常通过create_new_page_handle调用
     */
    RmPageHandle create_page_handle();

    /**
     * @brief 释放页面句柄
     * @param page_handle 要释放的页面句柄
     * @note 释放后不应继续使用该页面句柄
     * @warning 如果页面在事务中被修改，应等事务提交后再释放
     */
    void release_page_handle(RmPageHandle &page_handle);
};