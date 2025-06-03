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

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

#include "common/config.h"
#include "errors.h"

/**
 * @brief 磁盘管理器类
 *
 * @details DiskManager负责数据库系统的磁盘IO管理：
 * 1. 文件操作
 *    - 文件的创建、删除、打开、关闭
 *    - 目录的创建、删除和检查
 *    - 维护文件打开表
 *
 * 2. 页面操作
 *    - 页面的读取和写入
 *    - 新页面的分配
 *    - 页面号管理
 *
 * 3. 日志操作
 *    - WAL日志的读写
 *    - 日志文件管理
 *
 * @note 实现特点：
 * 1. 支持并发访问
 * 2. 使用原子操作保证一致性
 * 3. 异常安全的资源管理
 */
class DiskManager {
   public:
    explicit DiskManager();

    ~DiskManager() = default;

    void write_page(int fd, page_id_t page_no, const char *offset, int num_bytes);

    void read_page(int fd, page_id_t page_no, char *offset, int num_bytes);

    page_id_t allocate_page(int fd);

    void deallocate_page(page_id_t page_id);

    /**
     * @brief 目录相关操作
     *
     * @details 提供完整的目录管理功能：
     * 1. is_dir: 检查目录是否存在
     * 2. create_dir: 创建新目录
     * 3. destroy_dir: 删除目录及其内容
     *
     * @note 目录操作需要考虑：
     * 1. 权限检查
     * 2. 递归处理
     * 3. 错误恢复
     */
    bool is_dir(const std::string &path);

    void create_dir(const std::string &path);

    void destroy_dir(const std::string &path);

    /**
     * @brief 文件相关操作
     *
     * @details 核心文件管理功能：
     * 1. 基本操作
     *    - 创建/删除文件
     *    - 打开/关闭文件
     *    - 检查文件状态
     *
     * 2. 文件访问
     *    - 获取文件大小
     *    - 维护文件描述符映射
     *
     * @note 重要说明：
     * 1. 使用文件打开表跟踪活跃文件
     * 2. 防止重复打开和未关闭删除
     * 3. 支持文件大小动态增长
     */
    bool is_file(const std::string &path);

    void create_file(const std::string &path);

    void destroy_file(const std::string &path);

    int open_file(const std::string &path);

    void close_file(int fd);

    int get_file_size(const std::string &file_name);

    std::string get_file_name(int fd);

    int get_file_fd(const std::string &file_name);

    /**
     * @brief 日志相关操作
     *
     * @details WAL(预写式日志)操作：
     * 1. 日志读写
     *    - 追加写入新日志
     *    - 按偏移读取日志
     *
     * 2. 日志文件管理
     *    - 自动创建日志文件
     *    - 维护日志文件句柄
     *
     * @note 实现考虑：
     * 1. 顺序写入提高性能
     * 2. 原子性和持久性保证
     * 3. 支持日志截断和回收
     */
    int read_log(char *log_data, int size, int offset);

    void write_log(char *log_data, int size);

    void SetLogFd(int log_fd) { log_fd_ = log_fd; }

    int GetLogFd() { return log_fd_; }

    /**
     * @description: 设置文件已经分配的页面个数
     * @param {int} fd 文件对应的文件句柄
     * @param {int} start_page_no 已经分配的页面个数，即文件接下来从start_page_no开始分配页面编号
     */
    void set_fd2pageno(int fd, int start_page_no) { fd2pageno_[fd] = start_page_no; }

    /**
     * @description: 获得文件目前已分配的页面个数，即如果文件要分配一个新页面，需要从fd2pagenp_[fd]开始分配
     * @return {page_id_t} 已分配的页面个数
     * @param {int} fd 文件对应的句柄
     */
    page_id_t get_fd2pageno(int fd) { return fd2pageno_[fd]; }

    static constexpr int MAX_FD = 8192;

   private:
    // 文件打开列表，用于记录文件是否被打开
    std::unordered_map<std::string, int> path2fd_;  //<Page文件磁盘路径,Page fd>哈希表
    std::unordered_map<int, std::string> fd2path_;  //<Page fd,Page文件磁盘路径>哈希表

    int log_fd_ = -1;                             // WAL日志文件的文件句柄，默认为-1，代表未打开日志文件
    std::atomic<page_id_t> fd2pageno_[MAX_FD]{};  // 文件中已经分配的页面个数，初始值为0
};