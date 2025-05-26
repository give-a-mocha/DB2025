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

enum class Operation { FIND = 0, INSERT, DELETE };  // 三种操作：查找、插入、删除

static const bool binary_search = false;

/*
* @return: a < b 返回-1
*          a = b 返回0
*          a > b 返回1
*/
inline int ix_compare(const char *a, const char *b, ColType type, int col_len) {
    switch (type) {
        case TYPE_INT: {
            int ia = *(int *)a;
            int ib = *(int *)b;
            return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
        }
        case TYPE_FLOAT: {
            float fa = *(float *)a;
            float fb = *(float *)b;
            return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
        }
        case TYPE_STRING:
            return memcmp(a, b, col_len);
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

/* 管理B+树中的每个节点 */
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

/* B+树索引的整体句柄，管理整个B+树的操作 */
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
    // 构造函数
    IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd);

    // 搜索操作
    // 根据键获取对应的一个或多个记录ID (Rid)
    bool get_value(const char *key, std::vector<Rid> *result, Transaction *transaction);

    // 查找包含给定键的叶节点。
    // Operation 指定操作类型 (查找、插入、删除)，用于并发控制。
    // 返回一个 pair，[leaf node] and [root_is_latched] 返回目标叶子结点以及根结点是否加锁
    std::pair<IxNodeHandle *, bool> find_leaf_page(const char *key, Operation operation, Transaction *transaction,
                                                 bool find_first = false);

    // 插入操作
    // 向B+树中插入一个键值对 (key, value)
    page_id_t insert_entry(const char *key, const Rid &value, Transaction *transaction);

    // 分裂一个已满的节点 (node)，返回新创建的兄弟节点句柄
    IxNodeHandle *split(IxNodeHandle *node);

    // 将分裂产生的键 (key) 和新节点 (new_node) 插入到旧节点 (old_node) 的父节点中
    void insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node, Transaction *transaction);

    // 删除操作
    // 从B+树中删除与给定键 (key) 关联的条目
    bool delete_entry(const char *key, Transaction *transaction);

    // 当节点 (node) 的键数量低于下限时，尝试与兄弟节点合并 (coalesce) 或进行键的重新分配 (redistribute)
    // root_is_latched 用于指示根节点的锁是否已被持有 (用于并发控制)
    bool coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction = nullptr,
                                bool *root_is_latched = nullptr);
    // 如果根节点在删除操作后变为空或只有一个子节点，则调整根节点
    bool adjust_root(IxNodeHandle *old_root_node);

    // 从兄弟节点 (neighbor_node) 向当前节点 (node) 重新分配键，parent 是它们的父节点，index 是 node 在 parent 中的索引
    void redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index);

    // 将当前节点 (node) 与其兄弟节点 (neighbor_node) 合并。parent 是它们的父节点，index 是 node 在 parent 中的索引
    bool coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                  Transaction *transaction, bool *root_is_latched);

    // 范围查询相关
    // 查找第一个大于或等于 key 的键的位置 (Iid: 包含page_no和slot_no)
    Iid lower_bound(const char *key);

    // 查找第一个严格大于 key 的键的位置
    Iid upper_bound(const char *key);

    // 获取B+树中最后一个叶节点的末尾位置 (用于范围扫描的结束)
    Iid leaf_end() const;

    // 获取B+树中第一个叶节点的起始位置 (用于范围扫描的开始)
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