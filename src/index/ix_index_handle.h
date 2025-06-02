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

#include "ix_defs.h"
#include "transaction/transaction.h"

/**
 * @brief 索引操作的类型枚举
 *
 * 用于指定对B+树进行的操作类型，主要用于并发控制时的锁定策略
 */
enum class Operation {
    FIND = 0,   // 查找操作
    INSERT,     // 插入操作
    DELETE      // 删除操作
};

static const bool binary_search = false;  // 是否使用二分查找

/**
 * @brief 比较两个键值的大小
 *
 * @param a 第一个键值
 * @param b 第二个键值
 * @param type 键值的数据类型
 * @param col_len 键值的长度
 * @return int -1表示a<b，0表示a=b，1表示a>b
 * @throw InternalError 当遇到不支持的数据类型时抛出
 */
inline int ix_compare(const char *a, const char *b, ColType type, int col_len) {
    switch (type) {
        case ColType::TYPE_INT: {
            int ia = *reinterpret_cast<int *>(const_cast<char *>(a));
            int ib = *reinterpret_cast<int *>(const_cast<char *>(b));
            return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
        }
        case ColType::TYPE_FLOAT: {
            float fa = *reinterpret_cast<float *>(const_cast<char *>(a));
            float fb = *reinterpret_cast<float *>(const_cast<char *>(b));
            return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
        }
        case ColType::TYPE_STRING:
            return strncmp(a, b, col_len);
        default:
            throw InternalError("Unexpected data type");
    }
}

inline int ix_compare(const char* a, const char* b, const std::vector<ColType>& col_types, const std::vector<int>& col_lens) {
    int offset = 0;
    for(size_t i = 0; i < col_types.size(); ++i) {
        int res = ix_compare(a + offset, b + offset, col_types[i], col_lens[i]);
        if(res != 0) return res;
        offset += col_lens[i];
    }
    return 0;
}

/**
 * @brief B+树节点管理类
 *
 * 该类负责管理B+树中的单个节点，提供节点级别的操作接口，包括：
 * 1. 节点内键值对的插入、删除和查找
 * 2. 节点属性的访问和修改（如父节点、兄弟节点的设置）
 * 3. 节点内部数据的组织和管理
 */
class IxNodeHandle {
    friend class IxIndexHandle; // 允许 IxIndexHandle 类访问 IxNodeHandle 的私有成员
    friend class IxScan;        // 允许 IxScan 类访问 IxNodeHandle 的私有成员

   private:
    const IxFileHdr *file_hdr;      // 节点所在文件的头部信息
    Page *page;                     // 存储节点的页面
    IxPageHdr *page_hdr;            // page->data的第一部分，指针指向首地址，长度为sizeof(IxPageHdr)
    char *keys;                     // page->data的第二部分，指针指向首地址，长度为file_hdr->keys_size，每个key的长度为file_hdr->col_len
    Rid *rids;                      // page->data的第三部分，指针指向首地址

   public:
    // 默认构造函数
    IxNodeHandle() = default;

    // 构造函数，通过文件头和页面初始化节点句柄
    IxNodeHandle(const IxFileHdr *file_hdr_, Page *page_) : file_hdr(file_hdr_), page(page_) {
        // 将页面数据区的起始地址转换为 IxPageHdr 指针
        page_hdr = reinterpret_cast<IxPageHdr *>(page->get_data());
        // keys 指向 IxPageHdr 之后的数据区
        keys = page->get_data() + sizeof(IxPageHdr);
        // rids 指向存储所有键的区域 (file_hdr->keys_size_) 之后的数据区
        rids = reinterpret_cast<Rid *>(keys + file_hdr->keys_size_);
    }

    // 获取节点中当前键的数量
    int get_size() { return page_hdr->num_key; }

    // 设置节点中键的数量
    void set_size(int size) { page_hdr->num_key = size; }

    // 获取节点能容纳的最大键数量 (B+树的阶相关)btree_order_就是B+树的阶
    int get_max_size() { return file_hdr->btree_order_ + 1; }

    // 获取节点的最小键数量 (通常是最大数量的一半)
    int get_min_size() { return get_max_size() / 2; }

    // 获取指定索引位置的键的整数值 (假设键是int类型)
    int key_at(int i) { return *(int *)get_key(i); }

    /* 得到第i个孩子结点的page_no */
    page_id_t value_at(int i) { return get_rid(i)->page_no; }

    // 获取节点所在页面的页面号
    page_id_t get_page_no() { return page->get_page_id().page_no; }

    // 获取节点所在页面的 PageId 对象
    PageId get_page_id() { return page->get_page_id(); }

    // 获取下一个叶节点的页面号 (仅叶节点有效)
    page_id_t get_next_leaf() { return page_hdr->next_leaf; }

    // 获取前一个叶节点的页面号 (仅叶节点有效)
    page_id_t get_prev_leaf() { return page_hdr->prev_leaf; }

    // 获取父节点的页面号
    page_id_t get_parent_page_no() { return page_hdr->parent; }

    // 判断当前节点是否为叶节点
    bool is_leaf_page() { return page_hdr->is_leaf; }

    // 判断当前节点是否为根节点
    bool is_root_page() { return get_parent_page_no() == INVALID_PAGE_ID; }

    // 设置下一个叶节点的页面号
    void set_next_leaf(page_id_t page_no) { page_hdr->next_leaf = page_no; }

    // 设置前一个叶节点的页面号
    void set_prev_leaf(page_id_t page_no) { page_hdr->prev_leaf = page_no; }

    // 设置父节点的页面号
    void set_parent_page_no(page_id_t parent) { page_hdr->parent = parent; }

    // 获取指定索引位置的键的原始字节指针
    char *get_key(int key_idx) const { return keys + key_idx * file_hdr->col_tot_len_; }

    // 获取指定索引位置的 Rid 对象的指针
    Rid *get_rid(int rid_idx) const { return &rids[rid_idx]; }

    // 将提供的键 (原始字节) 设置到指定索引位置
    void set_key(int key_idx, const char *key) { memcpy(keys + key_idx * file_hdr->col_tot_len_, key, file_hdr->col_tot_len_); }

    // 将提供的 Rid 对象设置到指定索引位置
    void set_rid(int rid_idx, const Rid &rid) { rids[rid_idx] = rid; }

    // 查找第一个大于或等于目标键的索引 (具体实现在 .cpp)
    int lower_bound(const char *target) const;

    // 查找第一个严格大于目标键的索引 (具体实现在 .cpp)
    int upper_bound(const char *target) const;

    // 在指定位置插入n个键值对 (具体实现在 .cpp)
    void insert_pairs(int pos, const char *key, const Rid *rid, int n);

    // 在内部节点中查找给定键应导向的子节点页面ID (具体实现在 .cpp)
    page_id_t internal_lookup(const char *key);

    // 在叶节点中查找给定键，并返回对应的Rid指针 (具体实现在 .cpp)
    bool leaf_lookup(const char *key, Rid **value);

    // 插入单个键值对 (具体实现在 .cpp)
    int insert(const char *key, const Rid &value);

    // 用于在结点中的指定位置插入单个键值对
    void insert_pair(int pos, const char *key, const Rid &rid) { insert_pairs(pos, key, &rid, 1); }

    // 删除指定位置的键值对 (具体实现在 .cpp)
    void erase_pair(int pos);

    // 删除与给定键关联的条目 (具体实现在 .cpp)
    int remove(const char *key);

    /**
     * @brief 用于内部节点移除最后一个键并返回其唯一的子节点 (通常在根节点收缩时使用)
     * @return 唯一的子节点的页面ID
     */
    page_id_t remove_and_return_only_child() {
        assert(get_size() == 1); // 断言当前节点只有一个键
        page_id_t child_page_no = value_at(0); // 获取该键对应的子节点页面ID
        erase_pair(0); // 删除该键值对
        assert(get_size() == 0); // 断言删除后节点为空
        return child_page_no;
    }

    /**
     * @brief 由parent调用，寻找child，返回child在parent中的rid_idx∈[0,page_hdr->num_key)
     * @param child
     * @return int
     */
    int find_child(IxNodeHandle *child) {
        int rid_idx;
        // 遍历父节点的所有子节点指针
        for (rid_idx = 0; rid_idx < page_hdr->num_key; rid_idx++) {
            // 比较子节点的页面号是否与目标child节点的页面号匹配
            if (get_rid(rid_idx)->page_no == child->get_page_no()) {
                break; // 找到匹配的子节点，跳出循环
            }
        }
        // 断言确保找到了子节点 (即rid_idx在有效范围内)
        assert(rid_idx < page_hdr->num_key);
        return rid_idx;
    }
};

/**
 * @brief B+树索引管理类
 *
 * 该类是B+树索引的主要管理类，负责：
 * 1. 整个B+树的增删改查操作
 * 2. 维护B+树的结构平衡（分裂、合并等）
 * 3. 管理并发访问控制
 * 4. 磁盘文件和缓冲池的交互
 *
 * 主要功能包括：
 * - 插入/删除键值对
 * - 等值查询和范围查询
 * - 节点分裂和合并
 * - 根节点管理
 */
class IxIndexHandle {
    friend class IxScan;    // 允许 IxScan 类访问 IxIndexHandle 的私有成员
    friend class IxManager; // 允许 IxManager 类访问 IxIndexHandle 的私有成员

   private:
    DiskManager *disk_manager_;           // 指向磁盘管理器，用于物理页面的读写
    BufferPoolManager *buffer_pool_manager_; // 指向缓冲池管理器，用于管理内存中的页面
    int fd_;                                    // 存储B+树索引的文件描述符
    IxFileHdr* file_hdr_;                       // 指向索引文件头部，包含索引的元数据 (例如根页面号)
                                                // root_page 初始化为2 (第0页存FILE_HDR_PAGE，第1页存LEAF_HEADER_PAGE)
    std::mutex root_latch_;                     // 用于保护根页面并发访问的互斥锁

   public:
    /**
     * @brief 构造函数
     * @param disk_manager 磁盘管理器
     * @param buffer_pool_manager 缓冲池管理器
     * @param fd 索引文件描述符
     */
    IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd);

    /**
     * @brief 根据键值查找对应的记录ID
     * @param key 要查找的键值
     * @param result 存储找到的记录ID
     * @param transaction 当前事务的指针
     * @return 是否成功找到记录
     */
    bool get_value(const char *key, std::vector<Rid> *result, Transaction *transaction);

    /**
     * @brief 查找包含指定键的叶节点
     * @param key 目标键值
     * @param operation 操作类型（查找/插入/删除）
     * @param transaction 当前事务
     * @param find_first 是否查找第一个叶节点
     * @return pair<叶节点句柄, 根节点是否加锁>
     */
    std::pair<IxNodeHandle *, bool> find_leaf_page(const char *key, Operation operation, Transaction *transaction,
                                                  bool find_first = false);

    /**
     * @brief 插入键值对
     * @param key 键值
     * @param value 记录ID
     * @param transaction 当前事务
     * @return 插入位置的页面ID
     */
    page_id_t insert_entry(const char *key, const Rid &value, Transaction *transaction);

    /**
     * @brief 分裂节点
     * @param node 需要分裂的节点
     * @return 新创建的兄弟节点
     */
    IxNodeHandle *split(IxNodeHandle *node);

    /**
     * @brief 将分裂产生的新键值对插入父节点
     * @param old_node 原节点
     * @param key 新键值
     * @param new_node 新节点
     * @param transaction 当前事务
     */
    void insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node, Transaction *transaction);

    /**
     * @brief 删除键值对
     * @param key 要删除的键值
     * @param transaction 当前事务
     * @return 是否成功删除
     */
    bool delete_entry(const char *key, Transaction *transaction);

    /**
     * @brief 处理节点键值过少的情况
     * @param node 当前节点
     * @param transaction 当前事务
     * @param root_is_latched 根节点是否加锁
     * @return 是否需要继续处理
     */
    bool coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction = nullptr,
                                 bool *root_is_latched = nullptr);

    /**
     * @brief 调整根节点
     * @param old_root_node 当前的根节点
     * @return 是否进行了调整
     */
    bool adjust_root(IxNodeHandle *old_root_node);

    /**
     * @brief 节点间键值重分配
     * @param neighbor_node 相邻节点
     * @param node 当前节点
     * @param parent 父节点
     * @param index 当前节点在父节点中的索引
     */
    void redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index);

    /**
     * @brief 合并节点
     * @param neighbor_node 相邻节点指针
     * @param node 当前节点指针
     * @param parent 父节点指针
     * @param index 当前节点在父节点中的索引
     * @param transaction 当前事务
     * @param root_is_latched 根节点是否加锁
     * @return 是否成功合并
     */
    bool coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                   Transaction *transaction, bool *root_is_latched);

    /**
     * @brief 查找大于等于指定键的第一个位置
     * @param key 目标键值
     * @return 索引ID（包含页面号和槽位号）
     */
    Iid lower_bound(const char *key);

    /**
     * @brief 查找严格大于指定键的第一个位置
     * @param key 目标键值
     * @return 索引ID
     */
    Iid upper_bound(const char *key);

    /**
     * @brief 获取最后一个叶节点的末尾位置
     * @return 索引ID
     */
    Iid leaf_end() const;

    /**
     * @brief 获取第一个叶节点的起始位置
     * @return 索引ID
     */
    Iid leaf_begin() const;

   private:
    // 辅助函数
    // 更新文件头中的根页面号
    void update_root_page_no(page_id_t root) { file_hdr_->root_page_ = root; }

    // 检查B+树是否为空 (即根页面号是否为无效页面)
    bool is_empty() const { return file_hdr_->root_page_ == IX_NO_PAGE; }

    // 节点获取与创建
    // 根据页面号从缓冲池获取一个页面，并包装成 IxNodeHandle 返回
    IxNodeHandle *fetch_node(int page_no) const;

    // 创建一个新的B+树节点页面，并包装成 IxNodeHandle 返回
    IxNodeHandle *create_node();

    // 数据结构维护
    // 维护节点 (node) 的父节点指针 (在节点分裂或移动后可能需要)
    void maintain_parent(IxNodeHandle *node);

    // 从叶节点链表中移除一个叶节点 (leaf)
    void erase_leaf(IxNodeHandle *leaf);

    // 释放不再使用的 IxNodeHandle (通常意味着 unpin 对应的页面)
    void release_node_handle(IxNodeHandle &node);

    // 维护父节点 (node) 中指向其子节点 (child_idx) 的指针
    void maintain_child(IxNodeHandle *node, int child_idx);

    // 索引测试相关
    // 根据 Iid (页面号和槽号) 获取对应的 Rid (通常用于测试或调试)
    Rid get_rid(const Iid &iid) const;
};