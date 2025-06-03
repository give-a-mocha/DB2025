/**
 * @file ix_defs.h
 * @author RMDB Development Team
 * @brief B+树索引的关键数据结构定义
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * 定义了B+树索引的核心数据结构：
 * 1. 文件组织
 *    - 文件头：存储索引的元数据
 *    - 页面头：存储节点的元数据
 *    - 叶节点链表：支持范围查询
 *
 * 2. 内存布局
 *    - 序列化/反序列化支持
 *    - 页面内数据紧凑存储
 *    - 变长字段处理
 *
 * 3. B+树参数
 *    - 阶数动态计算
 *    - 页面大小限制
 *    - 键值大小限制
 */

#pragma once

#include <vector>

#include "defs.h"
#include "storage/buffer_pool_manager.h"

/** @brief 无效页面号，用于表示空指针或错误状态 */
constexpr int IX_NO_PAGE = -1;

/** @brief 文件头页面号，固定为0，存储索引的元数据 */
constexpr int IX_FILE_HDR_PAGE = 0;

/** @brief 叶节点链表头页面号，固定为1，用于快速遍历所有叶节点 */
constexpr int IX_LEAF_HEADER_PAGE = 1;

/** @brief 初始根节点页面号，固定为2，在创建索引时使用 */
constexpr int IX_INIT_ROOT_PAGE = 2;

/** @brief 初始页面数，包括文件头、叶节点链表头和根节点 */
constexpr int IX_INIT_NUM_PAGES = 3;

/** @brief 索引键的最大长度(字节)，用于防止键值过大导致的性能问题 */
constexpr int IX_MAX_COL_LEN = 512;

/**
 * @brief 索引文件头，存储B+树的元数据信息
 *
 * 文件头包含以下信息：
 * 1. 空间管理
 *    - first_free_page_no_: 空闲页面链表头
 *    - num_pages_: 总页面数
 *
 * 2. 树结构信息
 *    - root_page_: 根节点页号
 *    - first_leaf_: 首叶节点页号
 *    - last_leaf_: 尾叶节点页号
 *
 * 3. 索引键信息
 *    - col_num_: 包含的列数
 *    - col_types_: 每列的类型
 *    - col_lens_: 每列的长度
 *    - col_tot_len_: 键值的总长度
 *
 * 4. B+树参数
 *    - btree_order_: 树的阶数(最大子节点数-1)
 *    - keys_size_: 每个节点键值区域的大小
 */
class IxFileHdr {
   public:
    page_id_t first_free_page_no_;    // 空闲页面链表的头节点页号
    int num_pages_;                    // 文件中的总页面数
    page_id_t root_page_;             // 根节点页号
    int col_num_;                      // 联合索引的列数
    std::vector<ColType> col_types_;  // 各列的数据类型
    std::vector<int> col_lens_;       // 各列的长度(字节数)
    int col_tot_len_;                 // 键值的总长度
    int btree_order_;                 // B+树的阶，决定节点分裂的阈值
    int keys_size_;                   // 节点键值区域大小=(btree_order+1)*col_tot_len
    page_id_t first_leaf_;            // 首叶节点页号，用于范围扫描
    page_id_t last_leaf_;             // 尾叶节点页号，用于反向扫描
    int tot_len_;                     // 文件头结构的总长度(字节数)

    IxFileHdr() { tot_len_ = col_num_ = 0; }

    /*
     * @description: IxFileHdr的构造函数, 把vertor类型扩容，不使用push_back
     */
    IxFileHdr(page_id_t first_free_page_no, int num_pages, page_id_t root_page, int col_num, int col_tot_len,
              int btree_order, int keys_size, page_id_t first_leaf, page_id_t last_leaf)
        : first_free_page_no_(first_free_page_no),
          num_pages_(num_pages),
          root_page_(root_page),
          col_num_(col_num),
          col_tot_len_(col_tot_len),
          btree_order_(btree_order),
          keys_size_(keys_size),
          first_leaf_(first_leaf),
          last_leaf_(last_leaf) {
        update_tot_len();
        col_types_.resize(col_num);
        col_lens_.resize(col_num);
    }
    void serialize(char *dest) {
        int offset = 0;  // 初始化偏移量，用于在目标缓冲区中定位写入位置
        // 将tot_len_（结构体总长度）序列化到dest缓冲区
        memcpy(dest + offset, &tot_len_, sizeof(int));
        offset += sizeof(int);  // 更新偏移量
        // 将first_free_page_no_（第一个空闲页号）序列化到dest缓冲区
        memcpy(dest + offset, &first_free_page_no_, sizeof(page_id_t));
        offset += sizeof(page_id_t);  // 更新偏移量
        // 将num_pages_（页面数量）序列化到dest缓冲区
        memcpy(dest + offset, &num_pages_, sizeof(int));
        offset += sizeof(int);  // 更新偏移量
        // 将root_page_（根页号）序列化到dest缓冲区
        memcpy(dest + offset, &root_page_, sizeof(page_id_t));
        offset += sizeof(page_id_t);  // 更新偏移量
        // 将col_num_（列数量）序列化到dest缓冲区
        memcpy(dest + offset, &col_num_, sizeof(int));
        offset += sizeof(int);  // 更新偏移量
        // 循环序列化每个列的类型 (col_types_)
        for (int i = 0; i < col_num_; ++i) {
            memcpy(dest + offset, &col_types_[i], sizeof(ColType));
            offset += sizeof(ColType);  // 更新偏移量
        }
        // 循环序列化每个列的长度 (col_lens_)
        for (int i = 0; i < col_num_; ++i) {
            memcpy(dest + offset, &col_lens_[i], sizeof(int));
            offset += sizeof(int);  // 更新偏移量
        }
        // 将col_tot_len_（列总长度）序列化到dest缓冲区
        memcpy(dest + offset, &col_tot_len_, sizeof(int));
        offset += sizeof(int);  // 更新偏移量
        // 将btree_order_（B+树的阶）序列化到dest缓冲区
        memcpy(dest + offset, &btree_order_, sizeof(int));
        offset += sizeof(int);  // 更新偏移量
        // 将keys_size_（键的总大小）序列化到dest缓冲区
        memcpy(dest + offset, &keys_size_, sizeof(int));
        offset += sizeof(int);  // 更新偏移量
        // 将first_leaf_（第一个叶子页号）序列化到dest缓冲区
        memcpy(dest + offset, &first_leaf_, sizeof(page_id_t));
        offset += sizeof(page_id_t);  // 更新偏移量
        // 将last_leaf_（最后一个叶子页号）序列化到dest缓冲区
        memcpy(dest + offset, &last_leaf_, sizeof(page_id_t));
        offset += sizeof(page_id_t);  // 更新偏移量
        // 断言：检查最终的偏移量是否等于结构体的总长度，确保序列化完整性
        assert(offset == tot_len_);
    }

    void deserialize(char *src) {
        int offset = 0;
        tot_len_ = *reinterpret_cast<const int *>(src + offset);
        offset += sizeof(int);
        first_free_page_no_ = *reinterpret_cast<const page_id_t *>(src + offset);
        offset += sizeof(int);
        num_pages_ = *reinterpret_cast<const int *>(src + offset);
        offset += sizeof(int);
        root_page_ = *reinterpret_cast<const page_id_t *>(src + offset);
        offset += sizeof(page_id_t);
        col_num_ = *reinterpret_cast<const int *>(src + offset);
        offset += sizeof(int);
        // std::cout << col_num_ << "\n";
        // 反序列化没有初始大小
        col_types_.resize(col_num_);
        col_lens_.resize(col_num_);
        for (int i = 0; i < col_num_; ++i) {
            col_types_[i] = *reinterpret_cast<const ColType *>(src + offset);
            offset += sizeof(ColType);
        }
        for (int i = 0; i < col_num_; ++i) {
            col_lens_[i] = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);
        }
        col_tot_len_ = *reinterpret_cast<const int *>(src + offset);
        offset += sizeof(int);
        btree_order_ = *reinterpret_cast<const int *>(src + offset);
        offset += sizeof(int);
        keys_size_ = *reinterpret_cast<const int *>(src + offset);
        offset += sizeof(int);
        first_leaf_ = *reinterpret_cast<const page_id_t *>(src + offset);
        offset += sizeof(page_id_t);
        last_leaf_ = *reinterpret_cast<const page_id_t *>(src + offset);
        offset += sizeof(page_id_t);
        assert(offset == tot_len_);
    }

   private:
    void update_tot_len() {
        tot_len_ = 0;
        tot_len_ += sizeof(page_id_t) * 4 + sizeof(int) * 6;
        tot_len_ += sizeof(ColType) * col_num_ + sizeof(int) * col_num_;
    }
};

/**
 * @brief B+树节点的页面头部结构
 *
 * 每个B+树节点页面的头部包含：
 * 1. 节点类型信息
 *    - is_leaf: 是否是叶节点
 *    - num_key: 当前键值对数量
 *
 * 2. 树结构信息
 *    - parent: 父节点页号
 *    - next_free_page_no: 保留字段
 *
 * 3. 叶节点链表信息(仅叶节点有效)
 *    - prev_leaf: 前一个叶节点页号
 *    - next_leaf: 后一个叶节点页号
 *
 * 页面布局：
 * +-------------+----------------+-------------+
 * | 页面头部    | 键值存储区域   | 指针存储区域 |
 * | IxPageHdr   |    keys       |    rids     |
 * +-------------+----------------+-------------+
 */
class IxPageHdr {
   public:
    page_id_t next_free_page_no;  // 保留字段，用于未来扩展
    page_id_t parent;             // 父节点页号，根节点为INVALID_PAGE_ID
    int num_key;                  // 当前键值对数量，范围[0,btree_order]
    bool is_leaf;                 // 是否是叶节点
    page_id_t prev_leaf;          // 前一个叶节点页号(仅叶节点有效)
    page_id_t next_leaf;          // 后一个叶节点页号(仅叶节点有效)
};

/**
 * @brief 索引项标识符，用于定位具体的键值对
 *
 * Iid (Index ID) 由两部分组成：
 * 1. page_no: 页面号，定位到具体的B+树节点
 * 2. slot_no: 槽位号，定位到节点内的具体位置
 *
 * 主要用途：
 * 1. 范围扫描的游标
 * 2. 定位插入位置
 * 3. 标识删除位置
 *
 * @note slot_no的范围是[0, num_key)
 */
class Iid {
   public:
    int page_no;    // B+树节点的页面号
    int slot_no;    // 节点内的槽位编号

    /**
     * @brief 判断两个索引项标识符是否相等
     * @param x 第一个标识符
     * @param y 第二个标识符
     * @return 两个标识符是否完全相同
     */
    friend bool operator==(const Iid &x, const Iid &y) { return x.page_no == y.page_no && x.slot_no == y.slot_no; }

    /**
     * @brief 判断两个索引项标识符是否不相等
     * @param x 第一个标识符
     * @param y 第二个标识符
     * @return 两个标识符是否存在不同
     */
    friend bool operator!=(const Iid &x, const Iid &y) { return !(x == y); }
};