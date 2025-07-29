
#include "disk_scheduler.h"

DiskScheduler::DiskScheduler() {
    background_thread_.emplace([&] { StartWorkerThread(); });
}

// 析构函数，负责清理资源
DiskScheduler::~DiskScheduler() {
    // 向请求队列中放入一个 `std::nullopt`，以通知工作线程退出循环
    request_queue_.Put(std::nullopt);
    // 如果后台线程存在，则等待其执行完成
    if (background_thread_.has_value()) {
        background_thread_->join();
    }
}

// 调度一个磁盘请求
void DiskScheduler::Schedule(DiskRequest r) {
	// 将请求放入请求队列中
	request_queue_.Put(std::move(r));
}

// 后台工作线程的入口函数
void DiskScheduler::StartWorkerThread() {
  // 无限循环，直到收到退出信号
	while (true) {
		// 从请求队列中获取一个请求
		auto request_opt = request_queue_.Get();
		// 如果获取到的是 std::nullopt，说明收到了退出信号，退出循环
		if (!request_opt.has_value()) {
			break;
		}

		// 获取请求对象
		auto &request = request_opt.value();
		// 根据请求是读操作还是写操作，调用 DiskManager 的相应方法
		if (request.is_write_) {
			WritePage(request.fd_, request.page_no_, request.data_, request.num_bytes_);
		} else {
			ReadPage(request.fd_, request.page_no_, request.data_, request.num_bytes_);
		}
		// 操作完成后，通过 promise 通知请求的发起者
		request.callback_.set_value(true);
	}
}

void WritePage(int fd, page_id_t page_no, const char *offset, int num_bytes) {
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
void ReadPage(int fd, page_id_t page_no, char *offset, int num_bytes) {
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
