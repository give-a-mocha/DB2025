#pragma once

#ifndef DSU_HPP
#define DSU_HPP
#include <cstddef>
#include <vector>

class DSU {
   private:
    std::vector<int> f;
    int size;
    int n;

   public:
    DSU(int n) : n(n), size(n) {
        f.resize(n + 1);
        for (int i = 0; i <= n; ++i) {
            f[i] = i;
        }
    }

    int find(int x) { return f[x] = (x == f[x]) ? x : find(f[x]); }

    void merge(int a, int b) {
        int fa = find(a);
        int fb = find(b);
        if (fa == fb) return;
        f[fa] = fb;
        size--;
    }
    int get_size() const { return size; }
};
#endif