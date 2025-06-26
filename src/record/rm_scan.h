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
     * @note 扫描器不会立即加载数据，
     * 直到首次调用next()才会开始实际扫描
     */
    RmScan(const RmFileHandle *file_handle);

    /**
     * @brief 移动到下一条有效记录
     */
    void next() override;

    /**
     * @brief 检查扫描是否完成
     * @return true 如果已到达文件末尾，false 否则
     */
    bool is_end() const override;

    /**
     * @brief 获取当前记录的标识符
     * @return 当前记录的RID（页面号+槽位号）
     */
    Rid rid() const override;
};
