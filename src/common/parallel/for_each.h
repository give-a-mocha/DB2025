#pragma once

#ifndef FOR_EACH_H_
#define FOR_EACH_H_

#include <functional>

#include "ThreadPool.h"

namespace parallel {

template <typename Iter, typename Func>
void for_each(Iter begin, Iter end, Func&& func) {
    ThreadPool& pool = ThreadPool::getInstance();

    // 使用更合理的分块大小，而不是每个元素一个任务
    const size_t total_items = std::distance(begin, end);
    const size_t num_threads = pool.getThreadCount();
    const size_t chunk_size = std::max(size_t(1), total_items / (num_threads * 4));

    std::vector<std::future<void>> futures;

    while (begin != end) {
        auto chunk_end = begin;
        size_t current_chunk = 0;
        while (chunk_end != end && current_chunk < chunk_size) {
            ++chunk_end;
            ++current_chunk;
        }

        // 为每个数据块创建一个任务
        auto future = pool.enqueue([=, func_copy = func]() {
            for (auto it = begin; it != chunk_end; ++it) {
                func_copy(*it);
            }
        });

        futures.push_back(std::move(future));
        begin = chunk_end;
    }

    // 等待所有任务完成
    for (auto& future : futures) {
        future.get();
    }
}

}  // namespace parallel

#endif