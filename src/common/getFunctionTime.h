#pragma once

#include <atomic>
#include <cstddef>
#include <chrono>
#include <iostream>

namespace timedetail {

class TotTime {
   public:
    static TotTime& getInstance() {
        static TotTime instance;
        return instance;
    }

    void add(int64_t time) { tot_time.fetch_add(time); }

    int64_t get() { return tot_time.load(std::memory_order_relaxed); }

    void print() {
        std::clog << "\033[0m\033[1;31m" << "Total time: " << tot_time.load() / 1e6 << " ms" << "\033[0m" << std::endl;
    }

   private:
    std::atomic_int64_t tot_time = 0;
    TotTime() = default;
};

}  // namespace timedetail

class GetTime {
   private:
    std::chrono::high_resolution_clock::time_point start;

   public:
    GetTime() { start = std::chrono::high_resolution_clock::now(); }
    ~GetTime() {
        auto now = std::chrono::high_resolution_clock::now();
        auto use = std::chrono::duration_cast<std::chrono::nanoseconds>(now - start).count();
        timedetail::TotTime::getInstance().add(use);
    }
};
#ifndef AcquisitionTime
#define AcquisitionTime GetTime GetTimeTEMP;
#endif

#ifndef PrintFunctionTime
#define PrintFunctionTime timedetail::TotTime::getInstance().print();
#endif