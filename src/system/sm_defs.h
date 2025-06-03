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

#include <string>

#include "defs.h"

/**
 * @brief 系统管理器的常量和类型定义
 *
 * @details 定义系统中使用的关键常量：
 * 1. 字段类型和属性
 * 2. 元数据文件名称
 * 3. 系统限制参数
 *
 * @note 这些定义被整个系统管理器模块共享使用
 */

/** @brief 元数据文件名称，存储数据库的结构信息 */
static const std::string DB_META_NAME = "db.meta";

/** @brief WAL日志文件名称，用于故障恢复 */
static const std::string LOG_FILE_NAME = "log.log";

/**
 * @brief 字段数据类型枚举
 *
 * @details 支持以下基本类型：
 * - INT：整数类型，固定4字节
 * - FLOAT：浮点数类型，固定4字节
 * - CHAR：定长字符串类型，长度在创建时指定
 * - VARCHAR：变长字符串类型，最大长度在创建时指定
 *
 * @note 每种类型都需要考虑：
 * 1. 存储空间分配
 * 2. 序列化和反序列化
 * 3. 比较操作的实现
 */
