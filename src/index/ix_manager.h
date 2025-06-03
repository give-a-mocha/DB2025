/**
 * @file ix_manager.h
 * @author RMDB Development Team
 * @brief 索引管理器的实现
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * 索引管理器负责B+树索引的创建和维护：
 * 1. 物理存储结构
 *    - 文件头页：存储B+树的元数据
 *    - 叶子链表头页：管理所有叶子节点
 *    - 数据页：存储B+树节点
 *
 * 2. B+树特性
 *    - 所有叶子节点通过双向链表连接
 *    - 支持联合索引(多列索引)
 *    - 动态计算B+树的阶数
 *
 * 3. 并发控制
 *    - 支持事务的隔离性
 *    - 提供加锁和解锁接口
 */

#pragma once

#include <memory>
#include <string>

#include "system/sm_meta.h"
#include "ix_defs.h"
#include "ix_index_handle.h"

/**
 * @brief 索引管理器类
 *
 * 提供了索引的创建、打开、关闭等操作：
 * 1. 文件管理
 *    - 创建索引文件
 *    - 删除索引文件
 *    - 打开/关闭索引文件
 *
 * 2. 文件组织
 *    - 维护文件头信息
 *    - 管理页面分配
 *    - 处理文件格式
 *
 * 3. 缓冲管理
 *    - 协调内存与磁盘交互
 *    - 处理页面固定和解固定
 */
class IxManager {
   private:
    DiskManager *disk_manager_;            // 磁盘管理器
    BufferPoolManager *buffer_pool_manager_;  // 缓冲池管理器

   public:
    IxManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager)
        : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager) {}

    /**
     * @brief 生成索引文件名
     * @param filename 表名
     * @param index_cols 索引列名数组
     * @return 索引文件名(格式：表名_列名1_列名2_..._列名n.idx)
     *
     * @note 文件命名约定：
     * 1. 使用下划线连接表名和列名
     * 2. 以.idx作为文件扩展名
     * 3. 支持多列联合索引
     */
    std::string get_index_name(const std::string &filename, const std::vector<std::string>& index_cols) {
        std::string index_name = filename;
        for(size_t i = 0; i < index_cols.size(); ++i) 
            index_name += "_" + index_cols[i];
        index_name += ".idx";

        return index_name;
    }
    /*
    * @description: 获取索引文件名通过表名和索引列元数据
    */
    std::string get_index_name(const std::string &filename, const std::vector<ColMeta>& index_cols) {
        std::string index_name = filename;
        for(size_t i = 0; i < index_cols.size(); ++i) 
            index_name += "_" + index_cols[i].name;
        index_name += ".idx";

        return index_name;
    }

    /*
    * @description: 检查索引文件是否存在通过表名和索引列元数据
    */
    bool exists(const std::string &filename, const std::vector<ColMeta>& index_cols) {
        auto ix_name = get_index_name(filename, index_cols);
        return disk_manager_->is_file(ix_name);
    }

    /*
    * @description: 检查索引文件是否存在通过表名和索引列
    */
    bool exists(const std::string &filename, const std::vector<std::string>& index_cols) {
        auto ix_name = get_index_name(filename, index_cols);
        return disk_manager_->is_file(ix_name);
    }

    /**
     * @brief 创建索引文件
     * @param filename 表名
     * @param index_cols 索引列元数据数组
     * @throw InvalidColLengthError 如果索引键太长
     *
     * @details 创建过程：
     * 1. 计算B+树参数
     *    - 计算索引键的总长度
     *    - 动态确定B+树的阶数
     *    - 验证长度限制
     *
     * 2. 初始化文件结构
     *    - 创建并写入文件头
     *    - 初始化叶子链表头页
     *    - 创建根节点页面
     *
     * 3. 页面布局(按页号)：
     *    - 页面0：文件头
     *    - 页面1：叶子链表头
     *    - 页面2：根节点
     *
     * @note B+树阶数计算公式：
     * (页面大小 - 页面头大小) = (键长 + RID大小) * (阶数 + 1)
     */
    void create_index(const std::string &filename, const std::vector<ColMeta>& index_cols) {
        std::string ix_name = get_index_name(filename, index_cols);
        // 创建索引文件
        disk_manager_->create_file(ix_name);
        // 打开索引文件
        int fd = disk_manager_->open_file(ix_name);

        // 创建文件头并写入文件
        // 理论上我们有：|页面头部大小| + (|属性总长度| + |记录ID大小|) * n <= 页面大小
        // |page_hdr| + (|attr| + |rid|) * n <= PAGE_SIZE
        // 但是我们额外保留一个槽位以便于插入和删除操作，即：
        // |页面头部大小| + (|属性总长度| + |记录ID大小|) * (n + 1) <= 页面大小
        // |page_hdr| + (|attr| + |rid|) * (n + 1) <= PAGE_SIZE
        int col_tot_len = 0;
        int col_num = index_cols.size();
        for(auto& col: index_cols) {
            col_tot_len += col.len;
        }
        if (col_tot_len > IX_MAX_COL_LEN) {
            throw InvalidColLengthError(col_tot_len);
        }
        // 根据 |page_hdr| + (|attr| + |rid|) * (n + 1) <= PAGE_SIZE 求得n的最大值btree_order
        // 即 n <= btree_order，那么btree_order就是每个结点最多可插入的键值对数量（实际还多留了一个空位，但其不可插入）
        int btree_order = static_cast<int>((PAGE_SIZE - sizeof(IxPageHdr)) / (col_tot_len + sizeof(Rid)) - 1);
        assert(btree_order > 2);

        // Create file header and write to file
        IxFileHdr* fhdr = new IxFileHdr(IX_NO_PAGE, IX_INIT_NUM_PAGES, IX_INIT_ROOT_PAGE,
                                col_num, col_tot_len, btree_order, (btree_order + 1) * col_tot_len,
                                IX_INIT_ROOT_PAGE, IX_INIT_ROOT_PAGE);
        for(int i = 0; i < col_num; ++i) {
            fhdr->col_types_[i] = (index_cols[i].type);
            fhdr->col_lens_[i] = (index_cols[i].len);
        }
        
        char* data = new char[fhdr->tot_len_];
        fhdr->serialize(data);

        disk_manager_->write_page(fd, IX_FILE_HDR_PAGE, data, fhdr->tot_len_);

        char page_buf[PAGE_SIZE];  // 在内存中初始化page_buf中的内容，然后将其写入磁盘
        memset(page_buf, 0, PAGE_SIZE);
        // 注意leaf header页号为1，也标记为叶子结点，其前一个/后一个叶子均指向root node
        // Create leaf list header page and write to file
        {
            memset(page_buf, 0, PAGE_SIZE);
            auto phdr = reinterpret_cast<IxPageHdr *>(page_buf);
            *phdr = {
                .next_free_page_no = IX_NO_PAGE,
                .parent = IX_NO_PAGE,
                .num_key = 0,
                .is_leaf = true,
                .prev_leaf = IX_INIT_ROOT_PAGE,
                .next_leaf = IX_INIT_ROOT_PAGE,
            };
            disk_manager_->write_page(fd, IX_LEAF_HEADER_PAGE, page_buf, PAGE_SIZE);
        }
        // 注意root node页号为2，也标记为叶子结点，其前一个/后一个叶子均指向leaf header
        // Create root node and write to file
        {
            memset(page_buf, 0, PAGE_SIZE);
            auto phdr = reinterpret_cast<IxPageHdr *>(page_buf);
            *phdr = {
                .next_free_page_no = IX_NO_PAGE,
                .parent = IX_NO_PAGE,
                .num_key = 0,
                .is_leaf = true,
                .prev_leaf = IX_LEAF_HEADER_PAGE,
                .next_leaf = IX_LEAF_HEADER_PAGE,
            };
            // Must write PAGE_SIZE here in case of future fetch_node()
            disk_manager_->write_page(fd, IX_INIT_ROOT_PAGE, page_buf, PAGE_SIZE);
        }

        disk_manager_->set_fd2pageno(fd, IX_INIT_NUM_PAGES - 1);  // DEBUG

        // Close index file
        disk_manager_->close_file(fd);
    }

    void destroy_index(const std::string &filename, const std::vector<ColMeta>& index_cols) {
        std::string ix_name = get_index_name(filename, index_cols);
        disk_manager_->destroy_file(ix_name);
    }

    void destroy_index(const std::string &filename, const std::vector<std::string>& index_cols) {
        std::string ix_name = get_index_name(filename, index_cols);
        disk_manager_->destroy_file(ix_name);
    }
    void destroy_index(const std::string &index_name) {
        disk_manager_->destroy_file(index_name);
    }

    /**
     * @brief 打开索引文件并创建索引句柄
     * @param filename 表名
     * @param index_cols 索引列元数据
     * @return 索引句柄的智能指针
     *
     * @note 索引句柄的作用：
     * 1. 提供B+树的操作接口
     * 2. 维护文件的打开状态
     * 3. 管理内存中的索引页面
     *
     * @warning 使用完索引后必须调用close_index关闭
     */
    std::unique_ptr<IxIndexHandle> open_index(const std::string &filename, const std::vector<ColMeta>& index_cols) {
        
        std::string ix_name = get_index_name(filename, index_cols);
        int fd = disk_manager_->open_file(ix_name);
        return std::make_unique<IxIndexHandle>(disk_manager_, buffer_pool_manager_, fd);
    }

    std::unique_ptr<IxIndexHandle> open_index(const std::string &filename, const std::vector<std::string>& index_cols) {
        std::string ix_name = get_index_name(filename, index_cols);
        int fd = disk_manager_->open_file(ix_name);
        return std::make_unique<IxIndexHandle>(disk_manager_, buffer_pool_manager_, fd);
    }

    /**
     * @brief 关闭索引文件
     * @param ih 要关闭的索引句柄
     *
     * @details 关闭过程：
     * 1. 将文件头信息序列化到磁盘
     * 2. 刷新所有脏页
     * 3. 关闭文件描述符
     * 4. 清理内存资源
     *
     * @warning 关闭后不能再使用该索引句柄
     */
    void close_index(const IxIndexHandle *ih) {
        char* data = new char[ih->file_hdr_->tot_len_];
        ih->file_hdr_->serialize(data);
        disk_manager_->write_page(ih->fd_, IX_FILE_HDR_PAGE, data, ih->file_hdr_->tot_len_);
        // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
        buffer_pool_manager_->flush_all_pages(ih->fd_);
        disk_manager_->close_file(ih->fd_);
        delete[] data;
    }

    /**
     * @brief 删除索引
     * @param ih 要删除的索引句柄
     *
     * @details 删除过程：
     * 1. 从缓冲池中移除所有相关页面
     * 2. 关闭文件描述符
     *
     * @note 这是一个底层操作，通常通过destroy_index调用
     * @warning 确保没有其他事务正在使用该索引
     */
    void drop_index(const IxIndexHandle *ih) {
        buffer_pool_manager_->delete_all_pages(ih->fd_);
        disk_manager_->close_file(ih->fd_);
    }
};