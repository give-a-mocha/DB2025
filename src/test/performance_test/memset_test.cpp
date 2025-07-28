#include <bits/stdc++.h>

#include "../include/Timer.h"

void DoNotOptimize(void *p) { asm volatile("" : : "r,m"(p) : "memory"); }

void MemoryBarrier() { asm volatile("" : : : "memory"); }

int main() {
    constexpr size_t test_size = 1000 * 1024 * 1024;
    constexpr size_t test_count = 10000;

    {
        TIME_SCOPE("malloc + memset");
        for (int i = 1; i <= test_count; i++) {
            void *data = malloc(test_size);
            DoNotOptimize(data);
            memset(data, 0, test_size);
            free(data);
        }
    }

    {
        TIME_SCOPE("calloc");
        for (int i = 1; i <= test_count; i++) {
            void *data = calloc(test_size, 1);
            DoNotOptimize(data);
            free(data);
        }
    }
}