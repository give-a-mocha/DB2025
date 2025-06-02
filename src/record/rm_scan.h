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

#include "rm_defs.h"

class RmFileHandle;

/**
 * @brief 记录扫描器类，用于遍历表中的所有记录
 *
 * RmScan提供了一个遍历表中所有有效记录的接口。主要功能包括：
 * 1. 按顺序访问表中的所有记录
 * 2. 跳过已删除或空闲的记录槽位
 * 3. 提供判断是否达到文件末尾的方法
 */
class RmScan : public RecScan {
   private:
    const RmFileHandle *file_handle_;  // 被扫描的文件句柄
    Rid rid_;                          // 当前扫描位置的记录ID

   public:
    /**
     * @brief 构造函数，初始化记录扫描器
     * @param file_handle 要扫描的文件句柄
     */
    RmScan(const RmFileHandle *file_handle);

    /**
     * @brief 移动到下一条记录
     *
     * 该方法会跳过已删除和空闲的槽位，
     * 直到找到下一条有效记录或到达文件末尾
     */
    void next() override;

    /**
     * @brief 判断是否完成扫描
     * @return true 如果已到达文件末尾，false 否则
     */
    bool is_end() const override;

    /**
     * @brief 获取当前记录的ID
     * @return 当前记录的RID
     */
    Rid rid() const override;
};
