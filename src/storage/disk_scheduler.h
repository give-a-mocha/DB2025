#pragma once

#include <future>
#include <optional>
#include <thread>

#include "common/channel.h"
#include "page.h"
#include "errors.h"
#include <unistd.h>
/**
 * @brief 代表一个需要 DiskManager 执行的写或读请求。
 */
struct DiskRequest {
    /** 标记请求是写操作还是读操作。 */
    bool is_write_;

    /**
     * @brief 指向内存区域的指针，该页面将：
     *   1. 从磁盘读入（读操作）。
     *   2. 写入到磁盘（写操作）。
     */
    char *data_;

	std::unique_ptr<char[]> owned_data_;

	/**
     * @brief 表示要读或写的字节数。
     */
	int num_bytes_;
    int fd_;  // 文件描述符
	int page_no_;  // 页号

    /** 当请求完成时，用于通知请求发起者的回调。 */
    std::promise<bool> callback_;
};

/**
 * @brief DiskScheduler 负责调度磁盘的读写操作。
 *
 * 通过使用一个合适的 DiskRequest 对象调用 DiskScheduler::Schedule() 来调度一个请求。
 * 调度器维护一个后台工作线程，该线程使用磁盘管理器来处理已调度的请求。
 * 后台线程在 DiskScheduler 的构造函数中创建，并在其析构函数中销毁。
 */
class DiskScheduler {
public:
    explicit DiskScheduler();

    /**
     * @brief 销毁 DiskScheduler 对象。
     */
    ~DiskScheduler();

    /**
     * @brief 调度一个请求给 DiskManager 执行。
     * @param r 要调度的请求。
     */
    void Schedule(DiskRequest r);

    void StartWorkerThread();

    /** 用于磁盘调度器的 Promise 类型别名 */
    using DiskSchedulerPromise = std::promise<bool>;

    /**
     * @brief 创建一个 Promise 对象。
     * @return std::promise<bool> 一个新的 promise 对象。
     */
    auto CreatePromise() -> DiskSchedulerPromise { return {}; };

private:
    void WritePage(int fd, page_id_t page_no, const char *offset, int num_bytes) {}
	void ReadPage(int fd, page_id_t page_no, char *offset, int num_bytes) {}
    /**
     * @brief 一个用于并发调度和处理请求的共享队列。
     * 当 DiskScheduler 的析构函数被调用时，一个 `std::nullopt` 会被放入队列中，
     * 以此向后台线程发信号，通知其停止执行。
     */
    Channel<std::optional<DiskRequest>> request_queue_;
    /** 负责向磁盘管理器发出已调度请求的后台线程。 */
    std::optional<std::thread> background_thread_;
};
