/**
 * @file rm_scan.h
 * @author RMDB Development Team
 * @brief 表记录扫描器的实现
 * @version 0.1
 * @date 2023-12-01
 *
 * @copyright Copyright (c) 2023 Renmin University of China
 * @license Mulan PSL v2 (http://license.coscl.org.cn/MulanPSL2)
 *
 * 记录扫描器是一个高效的表记录遍历工具：
 * 1. 扫描原理
 *    - 使用页面级和槽位级的双层迭代
 *    - 通过位图快速定位有效记录
 *    - 支持顺序和跳页访问模式
 *
 * 2. 性能优化
 *    - 批量读取页面减少I/O
 *    - 使用缓冲池减少磁盘访问
 *    - 位图索引加速有效记录定位
 *
 * 3. 并发控制
 *    - 支持事务隔离
 *    - 兼容MVCC机制
 *    - 防止幻读和不可重复读
 */

#pragma once

#include "rm_defs.h"

class RmFileHandle;

/**
 * @brief 记录扫描器类，提供高效的表记录遍历功能
 *
 * RmScan实现了一个高效的表记录遍历接口，主要特点：
 * 1. 遍历功能
 *    - 顺序访问表中的有效记录
 *    - 自动跳过已删除和空闲槽位
 *    - 支持检查扫描完成状态
 *
 * 2. 扫描策略
 *    - 页面级遍历：按页面顺序扫描
 *    - 槽位级遍历：使用位图定位记录
 *    - 批量预读：减少I/O开销
 *
 * 3. 并发控制
 *    - 支持快照隔离
 *    - 维护一致性视图
 *    - 处理并发修改
 *
 * @warning 在事务中使用时需要注意：
 * 1. 可能看到未提交的修改
 * 2. 长时间扫描可能阻塞垃圾回收
 * 3. 大表扫描应考虑分批处理
 */
class RmScan : public RecScan {
   private:
    const RmFileHandle *file_handle_;  // 被扫描的文件句柄，提供对表数据的访问
    Rid rid_;                          // 当前记录的RID，包含页面号和槽位号
                                      // 用于追踪扫描位置，支持断点续扫

   public:
    /**
     * @brief 初始化记录扫描器
     * @param file_handle 要扫描的文件句柄
     *
     * @details 初始化过程：
     * 1. 设置起始位置(第一个有效记录)
     * 2. 初始化内部状态
     * 3. 准备缓冲区
     *
     * @note 扫描器不会立即加载数据，
     * 直到首次调用next()才会开始实际扫描
     */
    RmScan(const RmFileHandle *file_handle);

    /**
     * @brief 移动到下一条有效记录
     *
     * @details 移动过程：
     * 1. 在当前页面查找下一个有效槽位
     * 2. 如果当前页面没有更多记录，加载下一个页面
     * 3. 使用位图快速跳过无效槽位
     * 4. 更新RID和内部状态
     *
     * @note 性能优化：
     * 1. 使用位图索引加速查找
     * 2. 批量预读减少I/O
     * 3. 缓存热点页面
     */
    void next() override;

    /**
     * @brief 检查扫描是否完成
     * @return true 如果已到达文件末尾，false 否则
     *
     * @note 终止条件：
     * 1. 达到最后一个页面
     * 2. 当前页面的最后一个槽位
     * 3. 没有更多有效记录
     *
     * @warning 在并发环境下：
     * 1. 可能看不到扫描过程中新插入的记录
     * 2. 需要处理记录被并发删除的情况
     */
    bool is_end() const override;

    /**
     * @brief 获取当前记录的标识符
     * @return 当前记录的RID（页面号+槽位号）
     *
     * @details RID的组成：
     * 1. page_no: 页面号，标识记录所在页面
     * 2. slot_no: 槽位号，标识页面内的位置
     *
     * @note 应用场景：
     * 1. 用于后续查询或更新操作
     * 2. 支持断点续扫
     * 3. 记录访问路径优化
     */
    Rid rid() const override;
};
