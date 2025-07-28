/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/disk_manager.h"

#include <assert.h>    // for assert
#include <dirent.h>    // for DIR, opendir, readdir, closedir
#include <errno.h>     // for errno
#include <fcntl.h>     // for open flags
#include <sys/stat.h>  // for stat
#include <unistd.h>    // for lseek

#include "defs.h"

DiskManager::DiskManager() {}
/**
 * @brief 将数据写入文件的指定页面
 *
 * @param fd 磁盘文件的文件句柄
 * @param page_no 写入目标页面的page_id
 * @param offset 要写入磁盘的数据指针
 * @param num_bytes 要写入的数据大小
 *
 * @throw InternalError 写入失败时抛出以下异常：
 * 1. 无效的文件描述符
 * 2. 磁盘空间不足
 * 3. 其他IO错误
 *
 * @note 实现说明：
 * 1. 使用pwrite原子写入，避免竞态
 * 2. 检查写入字节数确保完整性
 * 3. 区分不同类型的IO错误
 */
void DiskManager::write_page(int fd, page_id_t page_no, const char *offset, int num_bytes) {
    // Todo:
    // !1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // !2.调用write()函数
    // !注意write返回值与num_bytes不等时 throw
    // !InternalError("DiskManager::write_page Error");
    if (fd < 0) {
        throw InternalError("Invalid file descriptor in write_page");
    }

    off_t write_offset = static_cast<off_t>(page_no) * PAGE_SIZE;

    ssize_t bytes_written = pwrite(fd, offset, num_bytes, write_offset);

    if (bytes_written != num_bytes) {
        if (errno == ENOSPC || errno == EDQUOT) {
            throw InternalError("Failed to write page due to no space");
        }
        throw InternalError("Failed to write page");
    }
}

/**
 * @brief 从文件的指定页面读取数据
 *
 * @param fd 磁盘文件的文件句柄
 * @param page_no 目标页面编号
 * @param offset 读取数据存储位置
 * @param num_bytes 要读取的字节数
 *
 * @throw InternalError 在以下情况抛出：
 * 1. 无效的文件描述符
 * 2. 读取过程中发生IO错误
 * 3. 未读取到预期数量的字节
 *
 * @note 实现说明：
 * 1. 使用pread避免多线程干扰
 * 2. 严格校验读取字节数
 * 3. 保证数据完整性
 */
void DiskManager::read_page(int fd, page_id_t page_no, char *offset, int num_bytes) {
    // Todo:
    // !1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // !2.调用read()函数
    // !注意read返回值与num_bytes不等时，throw
    // !InternalError("DiskManager::read_page Error");

    if (fd < 0) {
        throw InternalError("Invalid file descriptor in read_page");
    }

    off_t offset_in_file = static_cast<off_t>(page_no) * PAGE_SIZE;

    // 使用pread避免竞争条件，无需使用lseek
    ssize_t bytes_read = pread(fd, offset, num_bytes, offset_in_file);

    if (bytes_read != num_bytes) {
        throw InternalError("DiskManager::read_page Error");
    }
}

/**
 * @description: 分配一个新的页号
 * @return {page_id_t} 分配的新页号
 * @param {int} fd 指定文件的文件句柄
 */
page_id_t DiskManager::allocate_page(int fd) {
    // 简单的自增分配策略，指定文件的页面编号加1
    assert(fd >= 0 && fd < MAX_FD);
    return fd2pageno_[fd]++;
}

void DiskManager::rollback_page(int fd) {
    // 回滚指定文件的页面编号，减少1
    assert(fd >= 0 && fd < MAX_FD);
    if (fd2pageno_[fd] > 0) {
        fd2pageno_[fd]--;
    } else {
        throw InternalError("Cannot rollback page allocation below zero");
    }
}

void DiskManager::deallocate_page(__attribute__((unused)) page_id_t page_id) {}

bool DiskManager::is_dir(const std::string &path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

void DiskManager::create_dir(const std::string &path) {
    // Create a subdirectory
    std::string cmd = "mkdir " + path;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为path的目录
        throw UnixError();
    }
}

void DiskManager::destroy_dir(const std::string &path) {
    std::string cmd = "rm -r " + path;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @brief 检查指定路径的文件是否存在
 *
 * @param path 要检查的文件路径
 * @return true 文件存在且是普通文件
 * @return false 文件不存在或不是普通文件
 *
 * @details 实现步骤：
 * 1. 使用stat获取文件信息
 * 2. 检查文件类型是否为普通文件
 * 3. 处理stat可能的错误
 *
 * @note 使用场景：
 * 1. 创建文件前的检查
 * 2. 删除文件前的验证
 * 3. 打开文件前的确认
 */
bool DiskManager::is_file(const std::string &path) {
    // 用struct stat获取文件信息
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

/**
 * @description: 用于创建指定路径文件
 * @return {*}
 * @param {string} &path
 */
void DiskManager::create_file(const std::string &path) {
    // Todo:
    // !调用open()函数，使用O_CREAT模式
    // !注意不能重复创建相同文件

    // 检查文件是否已存在，如果存在则抛出错误，防止重复创建
    if (is_file(path)) {
        throw FileExistsError(path);
    }
    // 创建并打开文件
    // O_CREAT: 如果文件不存在则创建文件
    // O_RDWR: 以读写模式打开
    // S_IRUSR | S_IWUSR: 设置文件权限，只允许文件所有者读写
    int fd = open(path.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    // 检查文件创建是否成功，负值表示出错
    if (fd < 0) {
        throw UnixError();
    }
    // 关闭文件描述符
    close(fd);
}

/**
 * @description: 删除指定路径的文件
 * @param {string} &path 文件所在路径
 */
void DiskManager::destroy_file(const std::string &path) {
    // Todo:
    // !调用unlink()函数
    // !注意不能删除未关闭的文件

    // 检查文件是否存在
    if (!is_file(path)) {
        // 文件不存在，直接返回
        throw FileNotFoundError(path);
    }

    // 检查文件是否已打开
    if (path2fd_.find(path) != path2fd_.end()) {
        throw FileNotClosedError(path);
    }

    // 删除文件
    if (unlink(path.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开指定路径文件
 * @return {int} 返回打开的文件的文件句柄
 * @param {string} &path 文件所在路径
 */
int DiskManager::open_file(const std::string &path) {
    // Todo:
    // !调用open()函数，使用O_RDWR模式
    // !注意不能重复打开相同文件，并且需要更新文件打开列表

    auto it = path2fd_.find(path);
    if (it != path2fd_.end()) return it->second;

    // 文件未打开，调用系统的open函数以读写模式打开文件
    // O_RDWR表示以读写方式打开文件
    int fd = open(path.c_str(), O_RDWR);
    if (fd < 0) {
        throw FileNotFoundError(path);
    }

    // 文件打开成功，更新文件打开列表
    path2fd_.emplace(path, fd);
    fd2path_.emplace(fd, path);

    // 返回成功打开的文件描述符
    return fd;
}

/**
 * @description:用于关闭指定路径文件
 * @param {int} fd 打开的文件的文件句柄
 */
void DiskManager::close_file(int fd) {
    // Todo:
    // !调用close()函数
    // !注意不能关闭未打开的文件，并且需要更新文件打开列表

    auto it = fd2path_.find(fd);
    if (it == fd2path_.end()) {
        throw FileNotOpenError(fd);
    }

    // 文件大小在创建时已固定，关闭时不需要再调整
    if (close(fd) < 0) {
        throw UnixError();
    }
    path2fd_.erase(it->second);
    fd2path_.erase(it);
}

/**
 * @description: 获得文件的大小
 * @return {int} 文件的大小
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_size(const std::string &file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

/**
 * @description: 根据文件句柄获得文件名
 * @return {string} 文件句柄对应文件的文件名
 * @param {int} fd 文件句柄
 */
std::string DiskManager::get_file_name(int fd) {
    auto it = fd2path_.find(fd);
    if (it == fd2path_.end()) {
        throw FileNotOpenError(fd);
    }
    return it->second;
}

/**
 * @description:  获得文件名对应的文件句柄
 * @return {int} 文件句柄
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_fd(const std::string &file_name) {
    auto it = path2fd_.find(file_name);
    if (it == path2fd_.end()) {
        return open_file(file_name);
    }
    return it->second;
}

/**
 * @brief 读取日志文件内容
 *
 * @param log_data 读取内容的目标缓冲区
 * @param size 要读取的数据量
 * @param offset 读取的起始位置
 * @return int 实际读取的字节数，-1表示偏移超出文件大小
 *
 * @details 实现步骤：
 * 1. 检查并打开日志文件（如果未打开）
 * 2. 获取文件大小并验证偏移值
 * 3. 计算实际可读取的数据量
 * 4. 定位到指定偏移并读取数据
 *
 * @note 优化说明：
 * 1. 自动维护日志文件句柄
 * 2. 避免读取超出文件范围
 * 3. 确保原子性读取
 */
int DiskManager::read_log(char *log_data, int size, int offset) {
    // read log file from the previous end
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }
    int file_size = get_file_size(LOG_FILE_NAME);
    if (offset > file_size) {
        return -1;
    }

    size = std::min(size, file_size - offset);
    if (size == 0) return 0;
    lseek(log_fd_, offset, SEEK_SET);
    ssize_t bytes_read = read(log_fd_, log_data, size);
    assert(bytes_read == size);
    return bytes_read;
}

/**
 * @description: 写日志内容
 * @param {char} *log_data 要写入的日志内容
 * @param {int} size 要写入的内容大小
 */
void DiskManager::write_log(const char *log_data, int size) {
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }

    // write from the file_end
    lseek(log_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_fd_, log_data, size);
    if (bytes_write != size) {
        throw UnixError();
    }
}