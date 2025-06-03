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

/**
 * @brief B+树索引模块的主头文件
 * @details
 *
 * 核心组件：
 * 1. 索引管理器(IxManager)
 *    - 索引文件的创建、打开、关闭
 *    - 文件命名和管理规范
 *    - 提供文件级别的操作接口
 *
 * 2. 索引句柄(IxIndexHandle)
 *    - B+树核心操作的实现
 *    - 支持键值对的增删改查
 *    - 维护树的平衡结构
 *    - 处理节点分裂和合并
 *
 * 3. 索引扫描器(IxScan)
 *    - 支持范围查询操作
 *    - 利用叶子节点链表
 *    - 提供迭代器风格接口
 *
 * 设计特点：
 * 1. 数据结构
 *    - 基于B+树实现
 *    - 支持联合索引
 *    - 动态计算树的阶数
 *    - 优化的页面布局
 *
 * 2. 并发控制
 *    - 提供多粒度锁机制
 *    - 支持事务隔离级别
 *    - 使用B-link树优化
 *
 * 3. 性能优化
 *    - 与缓冲池管理器集成
 *    - 实现高效的页面管理
 *    - 支持批量操作优化
 */

#include "ix_manager.h"
#include "ix_scan.h"
