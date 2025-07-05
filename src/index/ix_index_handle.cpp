/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_index_handle.h"
#include "ix_scan.h"
#include "common/TraceStack.hpp"

/**
 * @brief 在当前节点中查找第一个大于等于target的key的位置
 * @details 使用二分查找算法在当前节点中寻找第一个大于等于目标值的键的位置
 * @param target 目标键值
 * @return key_idx 键值对数组的下标
 *         - 范围：[0,num_key)
 *         - 如果返回num_key，表示target大于节点中的所有key
 */
int IxNodeHandle::lower_bound(const char *target) const {
    TRACE_FUNCTION
    // Todo:
    // 查找当前节点中第一个大于等于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式，如顺序遍历、二分查找等；使用ix_compare()函数进行比较
    int l = 0, r = page_hdr->num_key - 1;
    int ans = page_hdr->num_key;
    while (l <= r) {
        int mid = (l + r) >> 1;
        if (ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_) >= 0) {
            ans = mid;
            r = mid - 1;
        } else {
            l = mid + 1;
        }
    }
    return ans;
}

/**
 * @brief 在当前节点中查找第一个严格大于target的key的位置
 * @details 使用二分查找算法查找第一个严格大于目标值的键的位置
 * @param target 目标键值
 * @return key_idx 键值对数组的下标
 *         - 范围：[1,num_key)
 *         - 如果返回num_key，表示target大于等于节点中所有key
 */
int IxNodeHandle::upper_bound(const char *target) const {
    TRACE_FUNCTION
    // Todo:
    // 查找当前节点中第一个大于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式：顺序遍历、二分查找等；使用ix_compare()函数进行比较
    int l = 1, r = page_hdr->num_key - 1;
    int ans = page_hdr->num_key;
    while (l <= r) {
        int mid = (l + r) >> 1;
        if (ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_) > 0) {
            ans = mid;
            r = mid - 1;
        } else {
            l = mid + 1;
        }
    }
    return ans;
}

/**
 * @brief 在叶子节点中查找指定key对应的键值对
 * @details 在叶子节点中定位目标键的具体位置，并返回对应的记录ID
 * @param key 要查找的目标键值
 * @param[out] value 用于存储查找到的记录ID的指针
 * @return bool
 *         - true：成功找到目标键值对
 *         - false：未找到目标键值对
 */
bool IxNodeHandle::leaf_lookup(const char *key, Rid **value) {
    TRACE_FUNCTION
    // Todo:
    // 1. 在叶子节点中获取目标key所在位置
    // 2. 判断目标key是否存在
    // 3. 如果存在，获取key对应的Rid，并赋值给传出参数value
    // 提示：可以调用lower_bound()和get_rid()函数。
    int pos = lower_bound(key);
    if (pos < page_hdr->num_key && ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        *value = get_rid(pos);
        return true;
    }
    return false;
}

/**
 * @brief 在内部节点中查找目标key应该插入的子节点位置
 * @details 通过比较键值确定目标key应当插入到哪个子树中
 * @param key 目标键值
 * @return page_id_t 包含目标key的子节点页面编号
 */
page_id_t IxNodeHandle::internal_lookup(const char *key) {
    TRACE_FUNCTION
    // Todo:
    // 1. 查找当前非叶子节点中目标key所在孩子节点（子树）的位置
    // 2. 获取该孩子节点（子树）所在页面的编号
    // 3. 返回页面编号
    int pos = upper_bound(key);
    return get_rid(pos - 1)->page_no;
}

/**
 * @brief 向节点的指定位置插入多个连续的键值对
 * @details 在节点中插入n个键值对，通过内存移动和复制实现高效插入
 * @param pos 插入位置的起始下标
 * @param (key, rid) 要插入的键值对数组的起始地址
 * @param n 要插入的键值对数量
 * @note 插入过程的内存布局：
 * 原始数据: [0,pos)           [pos,num_key)
 *                               key_slot
 *                               /      \
 *                              /        \
 * 插入后:   [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                         key           key_slot
 * @warning
 * - 调用前必须确保节点有足够空间容纳n个新键值对
 * - pos必须在有效范围内[0,num_key]
 * - 需要同时维护keys和rids数组
 */
void IxNodeHandle::insert_pairs(int pos, const char *key, const Rid *rid, int n) {
    TRACE_FUNCTION
    // Todo:
    // 1. 判断pos的合法性
    // 2. 通过key获取n个连续键值对的key值，并把n个key值插入到pos位置
    // 3. 通过rid获取n个连续键值对的rid值，并把n个rid值插入到pos位置
    // 4. 更新当前节点的键数量
    if (pos < 0 || pos > page_hdr->num_key) {
        throw RangeError("IxNodeHandle::insert_pairs: pos is out of range");
    }
    char *key_start = get_key(pos);
    int num = page_hdr->num_key - pos;
    int idx_length = file_hdr->col_tot_len_;
    std::memmove(key_start + n * idx_length, key_start, num * idx_length);
    std::memcpy(key_start, key, n * idx_length);
    Rid *rid_start = get_rid(pos);
    std::memmove(rid_start + n, rid_start, num * sizeof(Rid));
    std::memcpy(rid_start, rid, n * sizeof(Rid));

    page_hdr->num_key += n;
    return;
}

/**
 * @brief 向节点中插入单个键值对
 * @details 检查是否存在重复键值，不存在则插入新的键值对
 * @param key 要插入的键
 * @param value 要插入的记录ID
 * @return int 插入完成后节点中的键值对总数
 * @note
 * - 首先使用lower_bound找到适合的插入位置
 * - 如果键值已存在则不执行插入
 * - 通过insert_pairs实现实际的插入操作
 * @warning 调用前要确保节点有足够的空间存储新的键值对
 */
int IxNodeHandle::insert(const char *key, const Rid &value) {
    TRACE_FUNCTION
    // Todo:
    // 1. 查找要插入的键值对应该插入到当前节点的哪个位置
    // 2. 如果key重复则不插入
    // 3. 如果key不重复则插入键值对
    // 4. 返回完成插入操作之后的键值对数量
    int pos = lower_bound(key);
    if (pos < page_hdr->num_key && ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        return page_hdr->num_key;
    }
    insert_pairs(pos, key, &value, 1);
    return page_hdr->num_key;
}

/**
 * @brief 删除节点中指定位置的键值对
 * @details 通过内存移动实现高效的键值对删除
 * @param pos 要删除的键值对位置
 * @warning
 * - pos必须在有效范围内[0,num_key)
 * - 删除后要保证节点结构的完整性
 */
void IxNodeHandle::erase_pair(int pos) {
    TRACE_FUNCTION
    // Todo:
    // 1. 删除该位置的key
    // 2. 删除该位置的rid
    // 3. 更新结点的键值对数量
    if (pos < 0 || pos >= page_hdr->num_key) {
        throw RangeError("IxNodeHandle::erase_pair: pos is out of range");
    }
    // 计算需要向前移动的元素数量
    int num = page_hdr->num_key - 1 - pos;

    if (num > 0) {
        char *dest_key_ptr = get_key(pos);
        char *src_key_ptr = get_key(pos + 1);
        int key_len = file_hdr->col_tot_len_;
        std::memmove(dest_key_ptr, src_key_ptr, num * key_len);

        Rid *dest_rid_ptr = get_rid(pos);
        Rid *src_rid_ptr = get_rid(pos + 1);
        std::memmove(dest_rid_ptr, src_rid_ptr, num * sizeof(Rid));
    }

    page_hdr->num_key--;
    return;
}

/**
 * @brief 删除节点中指定key的键值对
 * @details 查找并删除指定key对应的键值对
 * @param key 要删除的键值
 * @return int 删除后节点中剩余的键值对数量
 * @note
 * - 使用lower_bound找到目标键的位置
 * - 如果找到目标键则调用erase_pair进行删除
 * - 如果未找到则不进行任何操作
 * @warning 删除操作可能导致节点键值对数量低于最小值，需要额外处理
 */
int IxNodeHandle::remove(const char *key) {
    TRACE_FUNCTION
    // Todo:
    // 1. 查找要删除键值对的位置
    // 2. 如果要删除的键值对存在，删除键值对
    // 3. 返回完成删除操作后的键值对数量
    int pos = lower_bound(key);
    if (pos < page_hdr->num_key && ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        erase_pair(pos);
    }
    return page_hdr->num_key;
}

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    // init file_hdr_
    char *buf = new char[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
    file_hdr_ = new IxFileHdr();
    file_hdr_->deserialize(buf);
    // disk_manager管理的fd对应的文件中，设置从file_hdr_->num_pages开始分配page_no
    int now_page_no = disk_manager_->get_fd2pageno(fd);
    disk_manager_->set_fd2pageno(fd, now_page_no + 1);
    delete[] buf;
}

/**
 * @brief 用于查找指定键所在的叶子结点
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return [leaf node] and [root_is_latched] 返回目标叶子结点以及根结点是否加锁
 * @note need to Unlatch and unpin the leaf node outside!
 * 注意：用了FindLeafPage之后一定要unlatch叶结点，否则下次latch该结点会堵塞！
 */
Page *IxIndexHandle::find_leaf_page(const char *key, Operation operation, Transaction *transaction) {
    TRACE_FUNCTION
    // Todo:
    // 1. 获取根节点
    // 2. 从根节点开始不断向下查找目标key
    // 3. 找到包含该key值的叶子结点停止查找，并返回叶子节点
    // WARN("START find_leaf_page");
    if (operation == Operation::FIND) {
        root_latch_.lock();
    }
    PageId page_id = {fd_, file_hdr_->root_page_};
    Page *page = buffer_pool_manager_->fetch_page(page_id);
    auto node = new IxNodeHandle(file_hdr_, page);
    if (operation == Operation::FIND) {
        page->rlatch();
        root_latch_.unlock();  // 查找操作，释放根节点锁
    } else {
        page->wlatch();
        if (!is_page_safe(node, operation)) {
            transaction->append_index_latch_page_set(nullptr);
        } else {
            root_latch_.unlock();
        }
    }
    // INFO("Page : {}", page->get_page_id().page_no);
    while (!node->is_leaf_page()) {
        // 如果是非叶子结点，则继续向下查找
        page_id = {fd_, node->internal_lookup(key)};
        Page *child_page = buffer_pool_manager_->fetch_page(page_id);
        auto child_node = new IxNodeHandle(file_hdr_, child_page);
        if (operation == Operation::FIND) {
            child_page->rlatch();
            page->runlatch();
            buffer_pool_manager_->unpin_page(page->get_page_id(), false);
        } else {
            child_page->wlatch();
            transaction->append_index_latch_page_set(page);
            if (is_page_safe(child_node, operation)) {
                UnlockAncestors(transaction);
            }
        }
        delete node;        // 释放上一个节点内存
        node = child_node;  // 更新当前节点为子节点
        page = child_page;  // 更新当前页面为子页面
        // INFO("Page : {}", page->get_page_id().page_no);
    }
    delete node;
    return page;
}

Page *IxIndexHandle::find_leaf_page_without_lock(const char *key, Operation operation) {
    TRACE_FUNCTION
    PageId page_id = {fd_, file_hdr_->root_page_};
    Page *page = buffer_pool_manager_->fetch_page(page_id);
    auto node = new IxNodeHandle(file_hdr_, page);
    while (!node->is_leaf_page()) {
        // 如果是非叶子结点，则继续向下查找
        page_id = {fd_, node->internal_lookup(key)};
        Page *child_page = buffer_pool_manager_->fetch_page(page_id);
        auto child_node = new IxNodeHandle(file_hdr_, child_page);
        buffer_pool_manager_->unpin_page(page->get_page_id(), false);
        delete node;        // 释放上一个节点内存
        node = child_node;  // 更新当前节点为子节点
        page = child_page;  // 更新当前页面为子页面
    }
    delete node;
    return page;
}

/**
 * @brief 查找指定键值对应的记录
 * @details 定位并获取与给定键值关联的所有记录ID
 * @param key 查找的目标键值
 * @param result 存储查找结果的向量容器
 * @param transaction 事务指针，用于并发控制
 * @return bool
 *         - true：成功找到目标键值对
 *         - false：未找到目标键值对
 */
bool IxIndexHandle::get_value(const char *key, std::vector<Rid> *result, Transaction *transaction) {
    TRACE_FUNCTION
    // Todo:
    // 1. 获取目标key值所在的叶子结点
    // 2. 在叶子节点中查找目标key值的位置，并读取key对应的rid
    // 3. 把rid存入result参数中
    // 提示：使用完buffer_pool提供的page之后，记得unpin page；记得处理并发的上锁

    root_latch_.lock();
    if (is_empty()) {
        root_latch_.unlock();
        return false;  // 如果索引为空，直接返回false
    }
    root_latch_.unlock();  // 查找操作，释放根节点锁
    Page *leaf_page = find_leaf_page(key, Operation::FIND, transaction);
    auto leaf_node = new IxNodeHandle(file_hdr_, leaf_page);
    Rid *rid;
    bool ok = leaf_node->leaf_lookup(key, &rid);
    if (ok) {
        if (rid->slot_no == IX_NO_SLOT) {
            // 这是一个溢出页的引用，需要读取溢出页中的所有RID
            get_all_rids_from_overflow_page(rid->page_no, result);
        } else {
            // 这是一个普通的RID
            result->push_back(*rid);
        }
    }
    leaf_page->runlatch();  // 读取完毕后释放叶子结点的读锁
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
    delete leaf_node;  // 释放叶子结点内存
    return ok;
}

bool IxIndexHandle::get_value_without_lock(const char *key, std::vector<Rid> *result) {
    TRACE_FUNCTION

    Page *leaf_page = find_leaf_page_without_lock(key, Operation::FIND);
    auto leaf_node = new IxNodeHandle(file_hdr_, leaf_page);
    Rid *rid;
    bool ok = leaf_node->leaf_lookup(key, &rid);
    if (ok) {
        if (rid->slot_no == IX_NO_SLOT) {
            // 这是一个溢出页的引用，需要读取溢出页中的所有RID
            get_all_rids_from_overflow_page(rid->page_no, result);
        } else {
            // 这是一个普通的RID
            result->push_back(*rid);
        }
    }
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
    delete leaf_node;  // 释放叶子结点内存
    return ok;
}

/**
 * @brief 将节点分裂成两个节点
 * @details 将一个满节点分裂成两个节点，键值对平均分配
 * @param node 需要分裂的节点
 * @return IxNodeHandle* 新创建的右侧节点
 * @warning
 * - 调用者必须在使用完毕后unpin两个节点
 * - 分裂可能触发父节点的递归分裂
 */
IxNodeHandle *IxIndexHandle::split(IxNodeHandle *node) {
    TRACE_FUNCTION
    // Todo:
    // 1. 将原结点的键值对平均分配，右半部分分裂为新的右兄弟结点
    //    需要初始化新节点的page_hdr内容
    // 2. 如果新的右兄弟结点是叶子结点，更新新旧节点的prev_leaf和next_leaf指针
    //    为新节点分配键值对，更新旧节点的键值对数记录
    // 3. 如果新的右兄弟结点不是叶子结点，更新该结点的所有孩子结点的父节点信息(使用IxIndexHandle::maintain_child())

    IxNodeHandle *new_node = create_node();
    new_node->page_hdr->is_leaf = node->is_leaf_page();
    new_node->page_hdr->parent = node->get_parent_page_no();
    new_node->page_hdr->num_key = 0;
    // 如果是叶子节点更新新旧节点的prev_leaf和next_leaf指针
    if (node->is_leaf_page()) {
        new_node->set_next_leaf(node->get_next_leaf());
        new_node->set_prev_leaf(node->get_page_no());
        node->set_next_leaf(new_node->get_page_no());
        if (file_hdr_->last_leaf_ == node->get_page_no()) {
            file_hdr_->last_leaf_ = new_node->get_page_no();
        } else {
            auto next_node = fetch_node(new_node->get_next_leaf());
            next_node->set_prev_leaf(new_node->get_page_no());
            buffer_pool_manager_->unpin_page(next_node->get_page_id(), true);
            delete next_node;  // 释放next_node内存
        }
    }
    // 计算分裂点
    int mid = node->get_max_size() / 2;
    int num = node->get_size() - mid;  // 右半部分的键值对数量
    new_node->insert_pairs(0, node->get_key(mid), node->get_rid(mid), num);
    node->page_hdr->num_key = mid;  // 最后再更新
    for (int i = 0; i < num; i++) {
        maintain_child(new_node, i);
    }
    return new_node;
}

/**
 * @brief 在节点分裂后将新节点的信息插入到父节点
 * @details 处理节点分裂后的父节点更新操作，可能触发递归分裂
 * @param old_node 原始节点
 * @param key 要插入到父节点的键值
 * @param new_node 分裂产生的新右兄弟节点
 * @param transaction 事务指针
 * @warning
 * - 必须正确维护节点间的父子关系
 * - 分裂可能一直递归到根节点
 * - 调用者负责unpin所有节点
 */
void IxIndexHandle::insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node) {
    TRACE_FUNCTION
    // Todo:
    // 1. 分裂前的结点（原结点, old_node）是否为根结点，如果为根结点需要分配新的root
    // 2. 获取原结点（old_node）的父亲结点
    // 3. 获取key对应的rid，并将(key, rid)插入到父亲结点
    // 4. 如果父亲结点仍需要继续分裂，则进行递归插入
    // 提示：记得unpin page
    if (old_node->is_root_page()) {
        // 如果old_node是根结点，则需要创建新的根结点
        IxNodeHandle *new_root = create_node();
        new_root->page_hdr->is_leaf = false;
        new_root->page_hdr->parent = INVALID_PAGE_ID;
        new_root->page_hdr->num_key = 0;
        // 根节点的第一个键应该是第一个子节点的最小键，第二个键是传入的分裂键
        new_root->insert_pair(0, old_node->get_key(0), Rid{old_node->get_page_no(), -1});
        new_root->insert_pair(1, key, Rid{new_node->get_page_no(), -1});

        old_node->page_hdr->parent = new_root->get_page_no();
        new_node->page_hdr->parent = new_root->get_page_no();
        // 更新root page
        update_root_page_no(new_root->get_page_no());
        buffer_pool_manager_->unpin_page(new_root->get_page_id(), true);
        delete new_root;  // 释放new_root内存
    } else {
        // 如果不是根结点，则获取父亲结点
        IxNodeHandle *parent_node = fetch_node(old_node->get_parent_page_no());

        // 插入新键值对到父亲结点
        int pos = parent_node->find_child(old_node);
        parent_node->insert_pair(pos + 1, key, Rid{new_node->get_page_no(), -1});

        // 如果父亲结点已满，则需要继续分裂
        if (parent_node->get_size() >= parent_node->get_max_size()) {
            IxNodeHandle *split_new_node = split(parent_node);
            insert_into_parent(parent_node, split_new_node->get_key(0), split_new_node);
            buffer_pool_manager_->unpin_page(split_new_node->get_page_id(), true);
            delete split_new_node;  // 释放split_new_node内存
        }
        buffer_pool_manager_->unpin_page(parent_node->get_page_id(), true);
        delete parent_node;  // 释放parent_node内存
    }
    // 注意：old_node和new_node的unpin应该由调用方负责
}

void IxIndexHandle::create_new_root(const char *key, const Rid &value) {
    IxNodeHandle *new_node = create_node();
    new_node->page_hdr->is_leaf = true;
    new_node->page_hdr->parent = INVALID_PAGE_ID;
    new_node->page_hdr->num_key = 0;
    new_node->insert_pair(0, key, value);
    update_root_page_no(new_node->get_page_no());
    file_hdr_->last_leaf_ = new_node->get_page_no();
    buffer_pool_manager_->unpin_page(new_node->get_page_id(), true);
    delete new_node;  // 释放new_node内存
}

bool IxIndexHandle::insert_into_leaf(const char *key, const Rid &value, IxNodeHandle *leaf_node) {
    TRACE_FUNCTION
    int pos = leaf_node->lower_bound(key);
    if (pos < leaf_node->page_hdr->num_key &&
        ix_compare(leaf_node->get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0) {
        return false;
    }
    // 插入键值对
    leaf_node->insert_pair(pos, key, value);
    // 如果叶子节点已满，则需要分裂
    if (leaf_node->get_size() >= leaf_node->get_max_size()) {
        IxNodeHandle *new_node = split(leaf_node);
        insert_into_parent(leaf_node, new_node->get_key(0), new_node);
        // 更新file_hdr_.last_leaf_，在split中处理了
        buffer_pool_manager_->unpin_page(new_node->get_page_id(), true);
        delete new_node;  // 释放new_node内存
    }
    return true;
}

/**
 * @brief 向B+树中插入键值对
 * @details 完成键值对的插入操作，必要时进行节点分裂
 * @param key 要插入的键
 * @param value 要插入的记录ID
 * @param transaction 事务指针，用于并发控制
 * @return page_id_t
 *         - 插入位置所在叶节点的页面号
 *         - INVALID_PAGE_ID 表示插入失败
 * @warning
 * - 需要正确处理并发控制
 * - 必须维护最右叶子节点信息
 * - 要及时释放节点的内存资源
 */
page_id_t IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) {
    TRACE_FUNCTION
    // Todo:
    // 1. 查找key值应该插入到哪个叶子节点
    // 2. 在该叶子节点中插入键值对
    // 3. 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
    // 提示：记得unpin page；若当前叶子节点是最右叶子节点，则需要更新file_hdr_.last_leaf；记得处理并发的上锁

    root_latch_.lock();
    if (is_empty()) {
        create_new_root(key, value);
        root_latch_.unlock();
        return file_hdr_->root_page_;
    } else {
        auto leaf_page = find_leaf_page(key, Operation::INSERT, transaction);
        auto leaf_node = new IxNodeHandle(file_hdr_, leaf_page);
        bool ok = insert_into_leaf(key, value, leaf_node);
        auto res = leaf_node->get_page_no();
        UnlockAncestors(transaction);
        leaf_page->wunlatch();  // 插入完毕后释放叶子
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), true);
        delete leaf_node;  // 释放叶子结点内存
        // 返回插入到的叶结点的page_no
        return ok ? res : INVALID_PAGE_ID;
    }
}

page_id_t IxIndexHandle::insert_entry_without_lock(const char *key, const Rid &value) {
    TRACE_FUNCTION
    if (is_empty()) {
        create_new_root(key, value);
        return file_hdr_->root_page_;
    } else {
        auto leaf_page = find_leaf_page_without_lock(key, Operation::INSERT);
        auto leaf_node = new IxNodeHandle(file_hdr_, leaf_page);
        bool ok = insert_into_leaf(key, value, leaf_node);
        auto res = leaf_node->get_page_no();
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), true);
        delete leaf_node;  // 释放叶子结点内存
        // 返回插入到的叶结点的page_no
        return ok ? res : INVALID_PAGE_ID;
    }
}

/**
 * @brief 替换B+树中key位置rid值
 * @details 完成键值对的替换操作,使用前应该通过get_value()确认key存在
 * @param key 要插入的键
 * @param value 要插入的记录ID
 * @param transaction 事务指针，用于并发控制
 * @return page_id_t
 *         - 插入位置所在叶节点的页面号
 *         - INVALID_PAGE_ID 表示插入失败
 * @warning
 * - 需要正确处理并发控制
 * - 必须维护最右叶子节点信息
 * - 要及时释放节点的内存资源
 */
page_id_t IxIndexHandle::insert_entry_force(const char *key, const Rid &value, Transaction *transaction) {
    TRACE_FUNCTION
    auto leaf_page = find_leaf_page(key, Operation::INSERT, transaction);
    auto leaf_node = new IxNodeHandle(file_hdr_, leaf_page);
    int pos = leaf_node->lower_bound(key);
    auto rid_ = leaf_node->get_rid(pos);
    if (rid_->slot_no == IX_NO_SLOT) {
        insert_into_overflow_page(value, rid_->page_no);
    } else {
        page_id_t overflow_page_no = create_overflow_page();
        insert_into_overflow_page(*rid_, overflow_page_no);
        insert_into_overflow_page(value, overflow_page_no);
        // 更新原RID为溢出页引用
        leaf_node->set_rid(pos, Rid{overflow_page_no, IX_NO_SLOT});
    }
    auto res = leaf_node->get_page_no();
    UnlockAncestors(transaction);
    leaf_page->wunlatch();  // 插入完毕后释放叶子
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), true);
    delete leaf_node;  // 释放叶子结点内存
    // 返回插入到的叶结点的page_no
    return res;
}

/**
 * @brief 创建新的溢出页
 * @return 新创建的溢出页页号
 * @note 溢出页用于存储重复键值对应的多个RID
 */
page_id_t IxIndexHandle::create_overflow_page() {
    TRACE_FUNCTION
    file_hdr_->num_pages_++;

    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    if (page == nullptr) {
        throw RMDBError("Failed to create overflow page");
    }

    // 初始化溢出页头部
    IxOverflowPageHdr *hdr = reinterpret_cast<IxOverflowPageHdr *>(page->get_data());
    *hdr = IxOverflowPageHdr();  // 使用默认构造函数初始化

    buffer_pool_manager_->unpin_page(new_page_id, true);  // 标记为dirty
    return new_page_id.page_no;
}

/**
 * @brief 向溢出页中插入RID
 * @param value 要插入的RID
 * @param page_no 溢出页页号
 * @return 是否插入成功
 * @note 如果当前溢出页已满，会自动链接到新的溢出页
 */
bool IxIndexHandle::insert_into_overflow_page(const Rid &value, page_id_t page_no) {
    TRACE_FUNCTION
    Page *page = buffer_pool_manager_->fetch_page({fd_, page_no});
    if (page == nullptr) {
        throw RMDBError("Failed to fetch overflow page");
    }

    // 获取溢出页头部和RID数组
    IxOverflowPageHdr *hdr = reinterpret_cast<IxOverflowPageHdr *>(page->get_data());
    Rid *rids = reinterpret_cast<Rid *>(page->get_data() + sizeof(IxOverflowPageHdr));

    // 计算页面最大可存储的RID数量
    int max_rids = IX_OVERFLOW_PAGE_MAX_RIDS();

    if (hdr->num_rids < max_rids) {
        // 当前页面有空间，直接插入
        rids[hdr->num_rids] = value;
        hdr->num_rids++;
        buffer_pool_manager_->unpin_page(page->get_page_id(), true);  // 标记为dirty
        return true;
    } else if (hdr->next_overflow_page != IX_NO_PAGE) {
        // 当前页面已满，但有下一个溢出页，递归插入
        buffer_pool_manager_->unpin_page(page->get_page_id(), false);
        return insert_into_overflow_page(value, hdr->next_overflow_page);
    } else {
        // 当前页面已满且没有下一个溢出页，创建新的溢出页
        page_id_t new_overflow_page = create_overflow_page();
        hdr->next_overflow_page = new_overflow_page;
        buffer_pool_manager_->unpin_page(page->get_page_id(), true);  // 标记为dirty，因为修改了next_overflow_page

        // 向新创建的溢出页插入RID
        return insert_into_overflow_page(value, new_overflow_page);
    }
}

/**
 * @brief 从溢出页中删除RID，并自动清理空页面
 * @param value 要删除的RID
 * @param page_no 溢出页页号
 * @param prev_page_no 前一个溢出页的页号，用于维护链表（IX_NO_PAGE表示这是第一个溢出页）
 * @return 删除后的下一个溢出页页号（如果当前页被删除）或当前页号
 */
page_id_t IxIndexHandle::remove_from_overflow_page(const Rid &value, page_id_t page_no, page_id_t prev_page_no) {
    TRACE_FUNCTION
    if (page_no == IX_NO_PAGE) {
        return IX_NO_PAGE;
    }

    Page *page = buffer_pool_manager_->fetch_page({fd_, page_no});
    if (page == nullptr) {
        return page_no;
    }

    IxOverflowPageHdr *hdr = reinterpret_cast<IxOverflowPageHdr *>(page->get_data());
    Rid *rids = reinterpret_cast<Rid *>(page->get_data() + sizeof(IxOverflowPageHdr));

    // 在当前页面中查找要删除的RID
    for (int i = 0; i < hdr->num_rids; i++) {
        if (rids[i].page_no == value.page_no && rids[i].slot_no == value.slot_no) {
            // 找到要删除的RID，用最后一个RID覆盖它
            rids[i] = rids[hdr->num_rids - 1];
            hdr->num_rids--;

            // 检查页面是否变空
            if (hdr->num_rids == 0) {
                page_id_t next_page = hdr->next_overflow_page;

                // 更新前一个页面的next指针（如果存在前一个页面）
                if (prev_page_no != IX_NO_PAGE) {
                    Page *prev_page = buffer_pool_manager_->fetch_page({fd_, prev_page_no});
                    if (prev_page != nullptr) {
                        IxOverflowPageHdr *prev_hdr = reinterpret_cast<IxOverflowPageHdr *>(prev_page->get_data());
                        prev_hdr->next_overflow_page = next_page;
                        buffer_pool_manager_->unpin_page(prev_page->get_page_id(), true);
                    }
                }

                // 删除当前空页面
                buffer_pool_manager_->unpin_page(page->get_page_id(), false);
                buffer_pool_manager_->delete_page({fd_, page_no});
                file_hdr_->num_pages_--;

                return next_page;  // 返回下一个页面的页号
            } else {
                buffer_pool_manager_->unpin_page(page->get_page_id(), true);  // 标记为dirty
                return page_no;  // 页面没有被删除，返回当前页号
            }
        }
    }

    // 在当前页面中没找到，继续在下一个溢出页中查找
    if (hdr->next_overflow_page != IX_NO_PAGE) {
        page_id_t new_next = remove_from_overflow_page(value, hdr->next_overflow_page, page_no);
        if (new_next != hdr->next_overflow_page) {
            // 下一个页面被删除了，更新当前页面的next指针
            hdr->next_overflow_page = new_next;
            buffer_pool_manager_->unpin_page(page->get_page_id(), true);  // 标记为dirty
        } else {
            buffer_pool_manager_->unpin_page(page->get_page_id(), false);
        }
        return page_no;
    }

    buffer_pool_manager_->unpin_page(page->get_page_id(), false);
    return page_no;
}

/**
 * @brief 获取溢出页中的所有RID
 * @param page_no 溢出页页号
 * @param result 存储结果的向量
 * @note 会遍历整个溢出页链表
 */
void IxIndexHandle::get_all_rids_from_overflow_page(page_id_t page_no, std::vector<Rid> *result) const {
    TRACE_FUNCTION
    Page *page = buffer_pool_manager_->fetch_page({fd_, page_no});
    if (page == nullptr) {
        return;
    }

    IxOverflowPageHdr *hdr = reinterpret_cast<IxOverflowPageHdr *>(page->get_data());
    Rid *rids = reinterpret_cast<Rid *>(page->get_data() + sizeof(IxOverflowPageHdr));

    // 添加当前页面的所有RID
    for (int i = 0; i < hdr->num_rids; i++) {
        result->push_back(rids[i]);
    }

    // 递归处理下一个溢出页
    if (hdr->next_overflow_page != IX_NO_PAGE) {
        get_all_rids_from_overflow_page(hdr->next_overflow_page, result);
    }

    buffer_pool_manager_->unpin_page(page->get_page_id(), false);
}

/**
 * @brief 释放溢出页链表
 * @param page_no 溢出页链表的起始页号
 * @note 释放从指定页开始的整个溢出页链表
 */
void IxIndexHandle::release_overflow_page_chain(page_id_t page_no) {
    TRACE_FUNCTION
    if (page_no == IX_NO_PAGE) {
        return;
    }

    Page *page = buffer_pool_manager_->fetch_page({fd_, page_no});
    if (page == nullptr) {
        return;
    }

    IxOverflowPageHdr *hdr = reinterpret_cast<IxOverflowPageHdr *>(page->get_data());
    page_id_t next_page = hdr->next_overflow_page;

    buffer_pool_manager_->unpin_page(page->get_page_id(), false);
    buffer_pool_manager_->delete_page({fd_, page_no});
    file_hdr_->num_pages_--;

    // 递归释放下一个溢出页
    if (next_page != IX_NO_PAGE) {
        release_overflow_page_chain(next_page);
    }
}

/**
 * @brief 从B+树中删除指定键值对
 * @details 删除键值对并处理可能的节点合并或重分配
 * @param key 要删除的键值
 * @param transaction 事务指针
 * @return bool
 *         - true：成功删除目标键值对
 *         - false：未找到目标键值对
 * @warning
 * - 删除可能触发节点合并
 * - 需要正确处理并发访问
 * - 注意资源的释放
 */
bool IxIndexHandle::delete_entry(const char *key, Transaction *transaction) {
    TRACE_FUNCTION
    // Todo:
    // 1. 获取该键值对所在的叶子结点
    // 2. 在该叶子结点中删除键值对
    // 3. 如果删除成功需要调用CoalesceOrRedistribute来进行合并或重分配操作，并根据函数返回结果判断是否有结点需要删除
    // 4. 如果需要并发，并且需要删除叶子结点，则需要在事务的delete_page_set中添加删除结点的对应页面；记得处理并发的上锁
    root_latch_.lock();
    if (is_empty()) {
        root_latch_.unlock();
        return false;  // 如果索引为空，直接返回false
    }

    Page *leaf_page = find_leaf_page(key, Operation::DELETE, transaction);
    auto leaf_node = new IxNodeHandle(file_hdr_, leaf_page);

    int pos = leaf_node->lower_bound(key);

    // 检查键是否存在
    if (pos >= leaf_node->get_size() ||
        ix_compare(leaf_node->get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) != 0) {
        // 没有这个键
        UnlockAncestors(transaction);
        leaf_page->wunlatch();  // 删除完毕后释放叶子结
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
        delete leaf_node;  // 释放叶子结点内存
        return false;
    }

    // 键存在，获取对应的RID
    auto rid_ = leaf_node->get_rid(pos);

    // 检查是否为溢出页引用
    if (rid_->slot_no == IX_NO_SLOT && rid_->page_no != IX_NO_PAGE) {
        release_overflow_page_chain(rid_->page_no);
    }

    // 删除键
    leaf_node->erase_pair(pos);
    coalesce_or_redistribute(leaf_node);
    UnlockAncestors(transaction);
    leaf_page->wunlatch();
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), true);
    delete leaf_node;
    return true;
}

bool IxIndexHandle::delete_entry_without_lock(const char *key) {
    TRACE_FUNCTION
    // 1. 获取目标key值所在的叶子结点 (不带锁)
    Page *leaf_page = find_leaf_page_without_lock(key, Operation::DELETE); // 使用DELETE操作类型
    auto leaf_node = new IxNodeHandle(file_hdr_, leaf_page);

    // 2. 在叶子节点中查找目标key值的位置
    int pos = leaf_node->lower_bound(key);

    // 检查键是否存在
    if (pos >= leaf_node->get_size() ||
        ix_compare(leaf_node->get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) != 0) {
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
        delete leaf_node;  // 释放叶子结点内存
        return false;
    }
    // 键存在，获取对应的RID
    auto rid_ = leaf_node->get_rid(pos);

    // 删除键
    leaf_node->erase_pair(pos);
    coalesce_or_redistribute(leaf_node);
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), true);
    delete leaf_node;
    return true;
}

/**
 * @brief 从B+树中删除指定键值对
 * @return bool
 *         - true：成功删除目标键值对
 *         - false：未找到目标键值对
 * @warning
 * -
 */
bool IxIndexHandle::delete_entry_with_rid(const char *key, const Rid &rid, Transaction *transaction) {
    TRACE_FUNCTION
    root_latch_.lock();
    if (is_empty()) {
        root_latch_.unlock();
        return false;  // 如果索引为空，直接返回false
    }

    Page *leaf_page = find_leaf_page(key, Operation::DELETE, transaction);
    auto leaf_node = new IxNodeHandle(file_hdr_, leaf_page);

    int pos = leaf_node->lower_bound(key);

    // 检查键是否存在
    if (pos >= leaf_node->get_size() ||
        ix_compare(leaf_node->get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) != 0) {
        // 没有这个键
        UnlockAncestors(transaction);
        leaf_page->wunlatch();  // 删除完毕后释放叶子结
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
        delete leaf_node;  // 释放叶子结点内存
        return false;
    }

    // 键存在，获取对应的RID
    auto rid_ = leaf_node->get_rid(pos);
    bool is_dirty = false;
    bool need_delete = true;
    // 检查是否为溢出页引用
    if (rid_->slot_no == IX_NO_SLOT && rid_->page_no != IX_NO_PAGE) {
        auto next_page_no = remove_from_overflow_page(rid, rid_->page_no, IX_NO_PAGE);
        is_dirty = (next_page_no != rid_->page_no);
        leaf_node->set_rid(pos, Rid{next_page_no, IX_NO_SLOT});
        if (next_page_no != IX_NO_PAGE) {
            need_delete = false;
        }
    }
    if (need_delete) {
        // 删除键
        leaf_node->erase_pair(pos);
        is_dirty = true;  // 删除操作需要标记为脏页
        coalesce_or_redistribute(leaf_node);
    }
    UnlockAncestors(transaction);
    leaf_page->wunlatch();
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), is_dirty);
    delete leaf_node;
    return true;
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * @param node 执行完删除操作的结点
 * @param transaction 事务指针
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点
 */
bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle *node) {
    TRACE_FUNCTION
    // Todo:
    // 1. 判断node结点是否为根节点
    //    1.1 如果是根节点，需要调用AdjustRoot() 函数来进行处理，返回根节点是否需要被删除
    //    1.2 如果不是根节点，并且不需要执行合并或重分配操作，则直接返回false，否则执行2
    // 2. 获取node结点的父亲结点
    // 3. 寻找node结点的兄弟结点（优先选取前驱结点）
    // 4. 如果node结点和兄弟结点的键值对数量之和，能够支撑两个B+树结点（即node.size+neighbor.size >=
    // NodeMinSize*2)，则只需要重新分配键值对（调用Redistribute函数）
    // 5. 如果不满足上述条件，则需要合并两个结点，将右边的结点合并到左边的结点（调用Coalesce函数）
    if (node->is_root_page()) {
        // 如果是根结点，则需要调用adjust_root函数来处理
        return adjust_root(node);
    } else if (node->get_size() >= node->get_min_size()) {
        // 如果不是根结点，并且不需要执行合并或重分配操作，则直接返回false
        // 维护父结点,键情况，因为如果删除的是第一个需要更新父亲的键
        maintain_parent(node);
        return false;
    }
    // 获取node结点的父亲结点
    IxNodeHandle *parent_node = fetch_node(node->get_parent_page_no());

    // 因为删掉了一个键值对，所以只能通过比较页号
    int rank = parent_node->find_child(node);
    if (rank > 0) {
        // 如果rank > 0，说明node结点有前驱结点,尝试和前驱结点进行重分配
        page_id_t neighbor_page_id = parent_node->value_at(rank - 1);
        Page *neighbor_page = buffer_pool_manager_->fetch_page({fd_, neighbor_page_id});
        IxNodeHandle *neighbor_node = new IxNodeHandle(file_hdr_, neighbor_page);
        if (neighbor_node->get_size() + node->get_size() >= node->get_min_size() * 2) {
            // 如果可以重分配，则调用redistribute函数
            neighbor_page->wlatch();
            redistribute(neighbor_node, node, parent_node, rank);
            neighbor_page->wunlatch();
            buffer_pool_manager_->unpin_page(neighbor_page->get_page_id(), true);
            buffer_pool_manager_->unpin_page(parent_node->get_page_id(), true);
            delete parent_node;  // 释放parent_node内存
            delete neighbor_node;
            return false;
        }
        buffer_pool_manager_->unpin_page(neighbor_page->get_page_id(), true);
        delete neighbor_node;
    }
    if (rank != parent_node->get_size() - 1) {
        // 如果rank != parent_node->get_size() - 1，说明node结点有后继结点,尝试和后继结点进行重分配
        page_id_t neighbor_page_id = parent_node->value_at(rank + 1);
        Page *neighbor_page = buffer_pool_manager_->fetch_page({fd_, neighbor_page_id});
        IxNodeHandle *neighbor_node = new IxNodeHandle(file_hdr_, neighbor_page);
        if (neighbor_node->get_size() + node->get_size() >= node->get_min_size() * 2) {
            // 如果可以重分配，则调用redistribute函数
            neighbor_page->wlatch();
            redistribute(neighbor_node, node, parent_node, rank);
            neighbor_page->wunlatch();
            buffer_pool_manager_->unpin_page(neighbor_page->get_page_id(), true);
            buffer_pool_manager_->unpin_page(parent_node->get_page_id(), true);
            delete parent_node;  // 释放parent_node内存
            delete neighbor_node;
            return false;
        }
        buffer_pool_manager_->unpin_page(neighbor_page->get_page_id(), true);
        delete neighbor_node;
    }
    // 如果都不满足，则需要合并两个结点
    if (rank > 0) {
        // 如果rank > 0，说明node结点有前驱结点,和前驱结点进行结合
        page_id_t neighbor_page_id = parent_node->value_at(rank - 1);
        Page *neighbor_page = buffer_pool_manager_->fetch_page({fd_, neighbor_page_id});
        IxNodeHandle *neighbor_node = new IxNodeHandle(file_hdr_, neighbor_page);
        // 如果可以合并，则调用coalesce函数
        neighbor_page->wlatch();
        coalesce(&neighbor_node, &node, &parent_node, rank);
        neighbor_page->wunlatch();
        buffer_pool_manager_->unpin_page(neighbor_page->get_page_id(), true);
        buffer_pool_manager_->unpin_page(parent_node->get_page_id(), true);
        delete parent_node;  // 释放parent_node内存
        delete neighbor_node;
        return true;
    }
    if (rank != parent_node->get_size() - 1) {
        // 如果rank != parent_node->get_size() - 1，说明node结点有后继结点,和后继结点进行结合
        page_id_t neighbor_page_id = parent_node->value_at(rank + 1);
        Page *neighbor_page = buffer_pool_manager_->fetch_page({fd_, neighbor_page_id});
        IxNodeHandle *neighbor_node = new IxNodeHandle(file_hdr_, neighbor_page);
        // 如果可以合并，则调用coalesce函数
        neighbor_page->wlatch();
        coalesce(&neighbor_node, &node, &parent_node, rank);
        neighbor_page->wunlatch();
        buffer_pool_manager_->unpin_page(neighbor_page->get_page_id(), true);
        buffer_pool_manager_->unpin_page(parent_node->get_page_id(), true);
        delete parent_node;  // 释放parent_node内存
        delete neighbor_node;
        return true;
    }
    return false;
}

/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note size of root page can be less than min size and this method is only called within coalesce_or_redistribute()
 */
bool IxIndexHandle::adjust_root(IxNodeHandle *old_root_node) {
    TRACE_FUNCTION
    // Todo:
    // 1. 如果old_root_node是内部结点，并且大小为1，则直接把它的孩子更新成新的根结点
    // 2. 如果old_root_node是叶结点，且大小为0，则直接更新root page
    // 3. 除了上述两种情况，不需要进行操作
    if (old_root_node->is_leaf_page()) {
        if (old_root_node->get_size() == 0) {
            // 如果是叶子结点且大小为0，则更新root page
            update_root_page_no(IX_NO_PAGE);
            release_node_handle(*old_root_node);
            return true;
        }
    } else {
        if (old_root_node->get_size() == 1) {
            IxNodeHandle *new_root_node = fetch_node(old_root_node->value_at(0));
            update_root_page_no(new_root_node->get_page_no());
            new_root_node->page_hdr->parent = INVALID_PAGE_ID;  // 更新新根结点的父亲结点为无效
            buffer_pool_manager_->unpin_page(new_root_node->get_page_id(), true);
            delete new_root_node;  // 释放新根结点内存
            release_node_handle(*old_root_node);
            return true;
        }
    }
    return false;
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 * Redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
 * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
 *
 * @param neighbor_node sibling page of input "node"
 * @param node input from method coalesceOrRedistribute()
 * @param parent the parent of "node" and "neighbor_node"
 * @param index node在parent中的rid_idx
 */
void IxIndexHandle::redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
    TRACE_FUNCTION
    // Todo:
    // 1. 通过index判断neighbor_node是否为node的前驱结点
    // 2. 从neighbor_node中移动一个键值对到node结点中
    // 3. 更新父节点中的相关信息，并且修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
    // 注意：neighbor_node的位置不同，需要移动的键值对不同，需要分类讨论
    if (index != parent->get_size() - 1 && parent->value_at(index + 1) == neighbor_node->get_page_no()) {
        // neighbor_node是node的后继结点
        // 将neighbor_node的第一个键值对移动到node的末尾
        node->insert_pairs(node->get_size(), neighbor_node->get_key(0), neighbor_node->get_rid(0),
                           1);                       // 在node末尾插入
        maintain_child(node, node->get_size() - 1);  // 更新node的孩子结点的父节点信息
        neighbor_node->erase_pair(0);                // 删除neighbor_node的第一个键值对
        // 更新parent结点中的相关信息
        parent->set_key(index + 1, neighbor_node->get_key(0));
    } else {
        // neighbor_node是node的前驱结点
        // 将neighbor_node的最后一个键值对移动到node的开头
        int last_index = neighbor_node->get_size() - 1;
        node->insert_pairs(0, neighbor_node->get_key(last_index), neighbor_node->get_rid(last_index),
                           1);                  // 在node开头插入
        maintain_child(node, 0);                // 更新node的孩子结点的父节点信息
        neighbor_node->erase_pair(last_index);  // 删除neighbor_node的最后一个键值对
        // 更新parent结点中的相关信息
        parent->set_key(index, node->get_key(0));
    }
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 */
bool IxIndexHandle::coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index) {
    TRACE_FUNCTION
    // Todo:
    // 1. 用index判断neighbor_node是否为node的前驱结点，若不是则交换两个结点，让neighbor_node作为左结点，node作为右结点
    // 2. 把node结点的键值对移动到neighbor_node中，并更新node结点孩子结点的父节点信息（调用maintain_child函数）
    // 3. 释放和删除node结点，并删除parent中node结点的信息，返回parent是否需要被删除
    // 提示：如果是叶子结点且为最右叶子结点，需要更新file_hdr_.last_leaf

    // 传入时保证了前驱关系
    int last_size = (*neighbor_node)->get_size();
    (*neighbor_node)->insert_pairs(last_size, (*node)->get_key(0), (*node)->get_rid(0), (*node)->get_size());
    for (int i = 0; i < (*node)->get_size(); i++) {
        maintain_child(*neighbor_node, last_size + i);
    }
    // 如果是叶子节点更新链
    if ((*node)->is_leaf_page()) {
        (*neighbor_node)->set_next_leaf((*node)->get_next_leaf());
        // 如果是最右边叶子节点
        if ((*node)->get_page_no() == file_hdr_->last_leaf_) {
            file_hdr_->last_leaf_ = (*neighbor_node)->get_page_no();
        } else {
            // 如果不是最右边叶子节点，则更新下一个叶子节点的prev_leaf指针
            auto next_node = fetch_node((*node)->get_next_leaf());
            next_node->set_prev_leaf((*neighbor_node)->get_page_no());
            buffer_pool_manager_->unpin_page(next_node->get_page_id(), true);
            delete next_node;  // 释放next_node内存
        }
    }

    // 释放node节点
    release_node_handle(**node);
    // 更新parent结点中的相关信息
    (*parent)->erase_pair(index);
    return coalesce_or_redistribute(*parent);
}

/**
 * @brief 将索引项ID转换为记录ID
 * @details 根据索引项ID中的页号和槽号找到对应的记录ID
 * @param iid 索引项ID，包含页号和槽号
 * @return Rid 对应的记录ID
 * @warning
 * - iid和rid存储不同含义：
 *   - iid是索引内部的位置标识
 *   - rid是实际记录的存储位置
 * - 使用后必须释放节点资源
 */
Rid IxIndexHandle::get_rid(const Iid &iid) const {
    TRACE_FUNCTION
    IxNodeHandle *node = fetch_node(iid.page_no);
    if (iid.slot_no >= node->get_size()) {
        buffer_pool_manager_->unpin_page(node->get_page_id(), false);
        delete node;  // 异常情况也要释放内存
        throw IndexEntryNotFoundError();
    }
    Rid result = *node->get_rid(iid.slot_no);
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);  // unpin it!
    delete node;                                                   // 释放内存
    return result;
}

/**
 * @brief 查找第一个大于等于给定键值的索引项
 * @details 结合查找叶子节点和节点内查找两个操作
 * @param key 目标键值
 * @return Iid 找到的索引项ID
 * @note 类型转换说明：
 * - 上层传入int类型键值通过(const char *)&key转换
 * - 使用时可通过*(int *)key转回int类型
 * @warning
 * - 必须正确处理未找到的情况
 * - 需要及时释放节点资源
 */
Iid IxIndexHandle::lower_bound(const char *key) {
    TRACE_FUNCTION
    //! DO
    if (is_empty()) {
        return {-1, -1};
    }
    auto leaf_page = find_leaf_page(key, Operation::FIND, nullptr);
    auto leaf_node = new IxNodeHandle(file_hdr_, leaf_page);
    int pos = leaf_node->lower_bound(key);
    Iid res{};
    if (pos == leaf_node->get_size()) {
        if (leaf_node->get_page_no() == file_hdr_->last_leaf_) {
            res = leaf_end();  // 返回叶子结点的结束位置
        } else {
            res = {leaf_node->get_next_leaf(), 0};
        }
    } else {
        res = {leaf_node->get_page_no(), pos};
    }
    leaf_page->runlatch();
    buffer_pool_manager_->unpin_page(leaf_page->get_page_id(), false);
    delete leaf_node;  // 释放叶子结点内存
    return res;
}

/**
 * @brief 查找第一个严格大于给定键值的索引项
 * @details 结合查找叶子节点和节点内查找两个操作
 * @param key 目标键值
 * @return Iid 找到的索引项ID
 */
Iid IxIndexHandle::upper_bound(const char *key) {
    TRACE_FUNCTION
    //! DO
    if (is_empty()) {
        return {-1, -1};
    }
    auto leaf_page = find_leaf_page(key, Operation::FIND, nullptr);
    auto leaf_node = new IxNodeHandle(file_hdr_, leaf_page);
    int pos = leaf_node->upper_bound(key);
    Iid res{};
    if (pos == leaf_node->get_size()) {
        if (leaf_node->get_page_no() == file_hdr_->last_leaf_) {
            res = leaf_end();  // 返回叶子结点的结束位置
        } else {
            res = {leaf_node->get_next_leaf(), 0};
        }
    } else {
        res = {leaf_node->get_page_no(), pos};
    }
    leaf_page->runlatch();
    buffer_pool_manager_->unpin_page(leaf_page->get_page_id(), false);
    delete leaf_node;  // 释放叶子结点内存
    return res;
}

/**
 * @brief 获取B+树最后一个叶子节点末尾的位置
 * @details 用于表示索引扫描的结束位置
 * @return Iid 最后叶子节点的结束位置
 * @note
 * - 返回最后叶子节点的size位置
 * - 常用作IxScan的终止位置
 * - 可用于范围查询的边界
 * @warning 使用后必须及时释放节点资源
 */
Iid IxIndexHandle::leaf_end() const {
    TRACE_FUNCTION
    IxNodeHandle *node = fetch_node(file_hdr_->last_leaf_);
    Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->get_size()};
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);  // unpin it!
    delete node;                                                   // 释放内存
    return iid;
}

/**
 * @brief 获取B+树第一个叶子节点的起始位置
 * @details 用于表示索引扫描的起始位置
 * @return Iid 第一个叶子节点的起始位置
 * @note
 * - 返回值：{first_leaf, 0}
 * - 常用作IxScan的起始位置
 * - 可用于范围查询的起点
 */
Iid IxIndexHandle::leaf_begin() const {
    TRACE_FUNCTION
    Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
    return iid;
}

/**
 * @brief 根据页号获取对应的索引节点
 * @details 从缓冲池中获取或加载指定页面
 * @param page_no 目标页面号
 * @return IxNodeHandle* 索引节点句柄
 * @warning
 * - 必须在外部调用unpin_page
 * - 必须释放返回的节点内存
 */
IxNodeHandle *IxIndexHandle::fetch_node(int page_no) const {
    TRACE_FUNCTION
    Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    if (page == nullptr) {
        throw FileNotOpenError(fd_);
    }
    IxNodeHandle *node = new IxNodeHandle(file_hdr_, page);
    return node;
}

/**
 * @brief 创建一个新的索引节点
 * @details 分配新页面并初始化为索引节点
 * @return IxNodeHandle* 新创建的节点句柄
 */
IxNodeHandle *IxIndexHandle::create_node() {
    TRACE_FUNCTION
    IxNodeHandle *node;
    file_hdr_->num_pages_++;

    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    node = new IxNodeHandle(file_hdr_, page);
    return node;
}

/**
 * @brief 更新节点到根节点路径上的键值
 * @details 递归更新父节点的首个键值直到根节点
 * @param node 起始节点
 * @warning
 * - 必须正确处理资源释放
 * - 需要正确维护内存中的数据
 */
void IxIndexHandle::maintain_parent(IxNodeHandle *node) {
    TRACE_FUNCTION
    IxNodeHandle *curr = node;
    while (curr->get_parent_page_no() != IX_NO_PAGE) {
        // Load its parent
        IxNodeHandle *parent = fetch_node(curr->get_parent_page_no());
        int rank = parent->find_child(curr);
        char *parent_key = parent->get_key(rank);
        char *child_first_key = curr->get_key(0);
        if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0) {
            // assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
            buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
            delete parent;
            break;
        }
        memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_);  // 修改了parent node
        curr = parent;

        // assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
        buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
        delete parent;  // 释放parent内存
    }
}

/**
 * @brief 删除叶子节点前更新双向链表
 * @details 维护叶子节点双向链表的完整性
 * @param leaf 待删除的叶子节点
 * @warning
 * - 必须在实际删除叶子节点前调用
 * - 需要正确处理资源释放
 */
void IxIndexHandle::erase_leaf(IxNodeHandle *leaf) {
    TRACE_FUNCTION
    assert(leaf->is_leaf_page());

    IxNodeHandle *prev = fetch_node(leaf->get_prev_leaf());
    prev->set_next_leaf(leaf->get_next_leaf());
    buffer_pool_manager_->unpin_page(prev->get_page_id(), true);
    delete prev;  // 释放prev内存

    IxNodeHandle *next = fetch_node(leaf->get_next_leaf());
    next->set_prev_leaf(leaf->get_prev_leaf());  // 注意此处是SetPrevLeaf()
    buffer_pool_manager_->unpin_page(next->get_page_id(), true);
    delete next;  // 释放next内存
}

/**
 * @brief 删除节点时更新文件头信息
 * @details 更新索引文件的页面数量统计
 * @param node 要删除的节点
 */
void IxIndexHandle::release_node_handle(IxNodeHandle &node) { file_hdr_->num_pages_--; }

/**
 * @brief 维护节点与子节点的父子关系
 * @details 设置子节点的父节点指针
 * @param node 父节点
 * @param child_idx 子节点在父节点中的索引
 * @warning 必须正确处理资源释放
 */
void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx) {
    TRACE_FUNCTION
    if (!node->is_leaf_page()) {
        //  Current node is inner node, load its child and set its parent to current node
        int child_page_no = node->value_at(child_idx);
        IxNodeHandle *child = fetch_node(child_page_no);
        child->set_parent_page_no(node->get_page_no());
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
        delete child;  // 释放child内存
    }
}

bool IxIndexHandle::is_page_safe(IxNodeHandle *node, Operation operation) {
    TRACE_FUNCTION
    auto size = node->get_size();

    switch (operation) {
        case Operation::INSERT:
            return size < node->get_max_size() - 1;
        case Operation::DELETE:
            if (node->is_root_page()) {
                return node->is_leaf_page() ? size > 1 : size > 2;
            }
            return size > node->get_min_size();
        case Operation::FIND:
            return true;
        default:
            throw std::invalid_argument("Unknown operation type");
    }
}

void IxIndexHandle::UnlockAncestors(Transaction *transaction) {
    TRACE_FUNCTION
    auto pages = transaction->get_index_latch_page_set();
    for (auto &page : *pages) {
        if (page == nullptr) {
            root_latch_.unlock();
        } else {
            page->wunlatch();
            buffer_pool_manager_->unpin_page(page->get_page_id(), false);
        }
    }
    pages->clear();  // 清空已解锁的页面集合
}

/**
 * @brief 调试函数：以树形结构打印B+树
 * @details 递归遍历B+树的所有节点，以树形结构显示页号和键的个数
 */
void IxIndexHandle::debug_print_tree() {
    TRACE_FUNCTION
    INFO("\n=== B+ Tree Structure Debug ===\n");
    if (is_empty()) {
        INFO("Tree is empty!\n");
        return;
    }

    // 打印树的基本信息
    INFO("Root page: {}", file_hdr_->root_page_);
    INFO("Last leaf: {}", file_hdr_->last_leaf_);
    INFO("Total pages: {}", file_hdr_->num_pages_);
    INFO("Key length: {}\n", file_hdr_->col_tot_len_);

    // 从根节点开始递归打印
    debug_print_node(file_hdr_->root_page_, 0);

    INFO("\n=== End of B+ Tree Structure Debug ===\n");
    debug_print_leaf_chain();
    INFO("====================================\n");
}

/**
 * @brief 递归打印节点信息
 * @param page_no 节点页号
 * @param depth 当前节点深度，用于控制缩进
 */
void IxIndexHandle::debug_print_node(page_id_t page_no, int depth) {
    TRACE_FUNCTION
    if (page_no == IX_NO_PAGE || page_no == INVALID_PAGE_ID) {
        return;
    }

    std::string res = std::string(depth * 2, ' ');  // 根据深度生成缩进字符串

    IxNodeHandle *node = fetch_node(page_no);

    // 打印节点基本信息
    res += "Page[" + std::to_string(page_no) + "] ";
    res += (node->is_leaf_page() ? "LEAF " : "INTERNAL ");
    res += "Keys:" + std::to_string(node->get_size()) + "/" + std::to_string(node->get_max_size()) + " ";
    res += "Parent:" + std::to_string(node->get_parent_page_no());

    if (node->is_leaf_page()) {
        res += " Prev:" + std::to_string(node->get_prev_leaf()) + " Next:" + std::to_string(node->get_next_leaf());
    }
    INFO(res);

    // 如果是内部节点，递归打印子节点
    if (!node->is_leaf_page()) {
        for (int i = 0; i < node->get_size(); i++) {
            page_id_t child_page = node->value_at(i);
            debug_print_node(child_page, depth + 1);
        }
    }

    buffer_pool_manager_->unpin_page(node->get_page_id(), false);
    delete node;
}

/**
 * @brief 打印叶子节点链表
 * @details 从第一个叶子节点开始，沿着链表打印所有叶子节点的连接关系
 */
void IxIndexHandle::debug_print_leaf_chain() {
    TRACE_FUNCTION
    if (is_empty()) {
        INFO("Leaf chain is empty.\n");
        return;
    }
    std::string res = "Leaf chain: ";
    auto now = fetch_node(file_hdr_->root_page_);
    while (!now->is_leaf_page()) {
        auto next_page = now->value_at(0);
        now = fetch_node(next_page);
    }
    while (now->get_page_no() != file_hdr_->last_leaf_) {
        res += "[" + std::to_string(now->get_page_no()) + "] ";
        auto next_page = now->get_next_leaf();
        now = fetch_node(next_page);
    }
    res += "[" + std::to_string(now->get_page_no()) + "]";
    res += "\n";
    INFO(res);
}