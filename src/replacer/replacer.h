/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "common/config.h"

/**
 * @brief 页面替换策略的抽象基类
 *
 * @details Replacer类定义了缓冲池页面替换策略的接口：
 * 1. 追踪页面的使用情况
 * 2. 实现页面的固定(pin)和解固定(unpin)
 * 3. 根据策略选择淘汰页面(victim)
 *
 * @note 设计特点：
 * 1. 纯虚接口，子类必须实现所有方法
 * 2. 支持并发控制
 * 3. 维护页面的引用状态
 *
 * @thread_safety 实现类需要保证线程安全
 */
class Replacer {
   public:
    Replacer() = default;
    virtual ~Replacer() = default;

    /**
     * @brief 根据替换策略选择一个淘汰帧
     *
     * @param[out] frame_id 被选中淘汰的帧ID；如果没有可淘汰的帧则为nullptr
     * @return true 成功找到可淘汰的帧
     * @return false 没有可淘汰的帧
     *
     * @note 实现要求：
     * 1. 按照策略选择最适合淘汰的帧
     * 2. 确保不会选中被固定(pinned)的帧
     * 3. 支持并发操作
     *
     * @thread_safety 线程安全
     */
    virtual bool victim(frame_id_t *frame_id) = 0;

    /**
     * @brief 固定一个帧，防止其被淘汰
     *
     * @param frame_id 要固定的帧ID
     *
     * @details 固定操作：
     * 1. 将帧标记为不可淘汰状态
     * 2. 从替换候选集中移除
     * 3. 更新相关的数据结构
     *
     * @note pin操作通常用于：
     * 1. 正在被访问的页面
     * 2. 包含重要数据的页面
     * 3. 暂时不能被替换的页面
     */
    virtual void pin(frame_id_t frame_id) = 0;

    /**
     * @brief 解除帧的固定状态，使其可以被淘汰
     *
     * @param frame_id 要解除固定的帧ID
     *
     * @details 解除固定操作：
     * 1. 将帧标记为可淘汰状态
     * 2. 加入替换候选集
     * 3. 根据策略更新访问信息
     *
     * @note unpin操作时机：
     * 1. 页面访问完成后
     * 2. 临时固定状态结束
     * 3. 不再需要保护的页面
     */
    virtual void unpin(frame_id_t frame_id) = 0;

    /**
     * @brief 获取当前可以被淘汰的帧数量
     *
     * @return size_t 可淘汰帧的数量
     *
     * @note 该数值反映了：
     * 1. 当前未被固定的页面数
     * 2. 潜在可替换的空间大小
     * 3. 缓冲池的使用状态
     */
    virtual size_t Size() = 0;
};
