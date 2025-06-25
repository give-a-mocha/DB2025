#pragma once

#ifndef PARALLEL_SORT_H
#define PARALLEL_SORT_H

#include <algorithm>
#include <future>
#include <vector>

#include "ThreadPool.h"

namespace parallel {

constexpr int fast_log2(size_t n) {
    if (n <= 1) return 0;

#if defined(__GNUC__) || defined(__clang__)
    return sizeof(size_t) * 8 - 1 - __builtin_clzl(n);
#elif defined(_MSC_VER)
    unsigned long index;
    _BitScanReverse64(&index, n);
    return static_cast<int>(index);
#else
    int result = 0;
    while (n >>= 1) ++result;
    return result;
#endif
}

// 确定何时切换到串行排序的阈值
constexpr size_t PARALLEL_THRESHOLD = 10000;

// 内部递归并行排序实现
template <typename RandomIt>
void parallel_sort_impl(RandomIt first, RandomIt last, int depth) {
    auto distance = std::distance(first, last);

    // 当元素数量较小或递归深度过高时，切换到标准库排序
    if (distance <= PARALLEL_THRESHOLD || depth <= 0) {
        std::sort(first, last);
        return;
    }

    // 选择枢轴元素的索引，而不是拷贝元素本身
    auto pivot_iter = first + distance / 2;
    
    // 使用引用来避免拷贝
    auto middle1 = std::partition(first, last, [&pivot_iter](const auto& elem) { 
        return elem < *pivot_iter; 
    });

    auto middle2 = std::partition(middle1, last, [&pivot_iter](const auto& elem) { 
        return !(*pivot_iter < elem); 
    });

    // 异步排序左子数组
    auto& pool = ThreadPool::getInstance();
    auto future = pool.submit([first, middle1, depth]() { 
        parallel_sort_impl(first, middle1, depth - 1); 
    });

    // 在当前线程排序右子数组
    parallel_sort_impl(middle2, last, depth - 1);

    // 等待左子数组排序完成
    future.wait();
}

template <typename RandomIt, typename Compare>
void parallel_sort_impl(RandomIt first, RandomIt last, int depth, Compare comp) {
    size_t distance = std::distance(first, last);

    // 当元素数量较小或递归深度过高时，切换到标准库排序
    if (distance <= PARALLEL_THRESHOLD || depth <= 0) {
        std::sort(first, last, comp);
        return;
    }

    // 选择枢轴元素的索引，而不是拷贝元素本身
    auto pivot_iter = first + distance / 2;
    
    // 使用引用来避免拷贝，并使用自定义比较器
    auto middle1 = std::partition(first, last, [&pivot_iter, &comp](const auto& elem) { 
        return comp(elem, *pivot_iter); 
    });

    auto middle2 = std::partition(middle1, last, [&pivot_iter, &comp](const auto& elem) { 
        return !comp(*pivot_iter, elem); 
    });

    // 异步排序左子数组
    auto& pool = ThreadPool::getInstance();
    auto future = pool.submit([first, middle1, depth, comp]() { 
        parallel_sort_impl(first, middle1, depth - 1, comp); 
    });

    // 在当前线程排序右子数组
    parallel_sort_impl(middle2, last, depth - 1, comp);

    // 等待左子数组排序完成
    future.wait();
}

// 对外暴露的排序接口
template <typename RandomIt>
void sort(RandomIt first, RandomIt last) {
    auto distance = std::distance(first, last);
    if (distance <= 1) return;

    // 根据可用线程数确定递归深度
    int max_depth = fast_log2(ThreadPool::getInstance().thread_count()) + 1;
    parallel_sort_impl(first, last, max_depth);
}

// 支持比较器的版本
template <typename RandomIt, typename Compare>
void sort(RandomIt first, RandomIt last, Compare comp) {
    auto distance = std::distance(first, last);
    if (distance <= 1) return;

    // 根据可用线程数确定递归深度
    int max_depth = fast_log2(ThreadPool::getInstance().thread_count()) + 1;
    parallel_sort_impl(first, last, max_depth, comp);
}

}  // namespace parallel

#endif  // PARALLEL_SORT_H
