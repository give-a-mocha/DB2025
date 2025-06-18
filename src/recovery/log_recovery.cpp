/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_recovery.h"

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze() {
 
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo() {

}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {

}

void RecoveryManager::create_static_check_point() {
    // （1）停止接收新事务和正在运行事务
	// （2）将仍保留在日志缓冲区中的内容写到日志文件中；
	// （3）在日志文件中写入一个“检查点记录”；
	// （4）将当前数据库缓冲区中的内容写到数据库中；
	// （5）把日志文件中检查点记录的地址写到“重新启动文件”中。

	
}