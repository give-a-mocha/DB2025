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
     * @thread_safety 线程安全
     */
    virtual bool victim(frame_id_t *frame_id) = 0;

    /**
     * @brief 固定一个帧，防止其被淘汰
     *
     * @param frame_id 要固定的帧ID

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
