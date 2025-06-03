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
 * @brief 系统管理器模块的主头文件
 *
 * @details 该文件整合了系统管理器的所有组件：
 * 1. sm_defs.h
 *    - 系统常量定义
 *    - 数据类型定义
 *
 * 2. sm_manager.h
 *    - 系统管理器核心类
 *    - DDL语句的实现
 *
 * 3. sm_meta.h
 *    - 元数据结构定义
 *    - 表、列、索引的描述
 *
 * @note 设计目的：
 * 1. 提供统一的包含入口
 * 2. 简化模块间的依赖管理
 * 3. 确保组件的完整引入
 */

#include "sm_defs.h"     // 系统常量和类型定义
#include "sm_manager.h"  // 系统管理器实现
#include "sm_meta.h"     // 元数据结构定义
