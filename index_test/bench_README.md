# 索引性能 & 蟹型协议并发对比脚本

基于 TPC-C 数据集，对 rmdb 的两个性能维度做量化对比：

1. **有/无索引** — 同一组 SQL 在 `drop index` 前后的耗时差距
2. **蟹型协议 (latch crabbing) vs 粗粒度根锁** — B+ 树在多并发下的吞吐与尾延迟

三个脚本：

| 文件 | 作用 |
| --- | --- |
| [bench_common.py](bench_common.py) | 共用：socket 客户端 + 延迟统计 + TPC-C 索引清单 |
| [index_bench.py](index_bench.py) | drop index 前后跑同一批查询，输出对比 |
| [concurrency_bench.py](concurrency_bench.py) | 多线程混合 (select+insert)，扫不同线程数，落 CSV |
| [compare_concurrency.py](compare_concurrency.py) | 把两次 concurrency_bench 的 CSV 摆一起 |

> 共同前提：rmdb server 已经 `make rmdb`、启动监听 `127.0.0.1:8765`，并通过
> `python TPCC-Tester/runner.py --prepare --w 1` 把 TPC-C 数据 load 进去。
> 下面所有命令的 `--w` 必须和实际 load 时一致。

---

## 一、索引性能 (有/无索引)

```bash
# server 已起、TPC-C 数据已 load
python3 index_test/index_bench.py --w 1 --queries 200 \
    --csv index_test/result/index_bench.csv
```

脚本会顺序做：

1. 用 4 类查询造一批 `--queries` 条 SQL（customer / stock / item 的点查，order_line 的范围查）
2. **Phase A**: 当前索引存在的状态下跑一遍，记录每条查询的延迟
3. `drop index` 把 TPC-C 八张表上的索引全部删掉
4. **Phase B**: 跑完全一样的 SQL（现在 planner 只能走全表扫描）
5. `create index` 还原所有索引，避免污染后续测试
6. 打印 `with index avg / no-idx avg / 加速比 / p95` 对比表

可选参数：

- `--queries N` (默认 200)：每个 phase 跑多少条。第 2 phase 是全表扫，N 大会很慢，第一次先用 100~500 试水
- `--skip-recreate`：不重建索引（脚本退出后 DB 仍处于无索引状态）
- `--seed`：固定查询序列，方便多次跑取均值
- `--csv path.csv`：把每个 phase 的统计写到 CSV

预期输出：在 1 仓库的数据量下，customer 点查的 avg 应当从 < 1ms 飙到几十 ms，加速比 50~200×。

---

## 二、蟹型协议 vs 粗粒度根锁 (并发对比)

这是两次跑：第一次用当前（crab）实现，第二次手动改一行源码重新编译后再跑。

### Step 1：跑 crab 版

当前 [src/system/sm_manager.cpp:564..598](../src/system/sm_manager.cpp) 默认调用：

```cpp
ih->insert_entry(key.get(), rid, txn);
ih->delete_entry(key.get(), txn);
ih->get_value(key.get(), rid, txn);
```

这三个走的是 [ix_index_handle.cpp](../src/index/ix_index_handle.cpp) 里 `find_leaf_page` 的 latch-coupling
(蟹行) 流程。先以 crab 形式跑一遍：

```bash
python3 index_test/concurrency_bench.py --w 1 --label crab \
    --threads 1,2,4,8,16,32 --duration 15 \
    --csv index_test/result/conc_crab.csv
```

### Step 2：切到粗粒度根锁，重编

`ix_index_handle` 里已经实现了一套对照版本：
[get_value_with_root_lock / insert_entry_with_root_lock / delete_entry_with_root_lock](../src/index/ix_index_handle.h)
（[ix_index_handle.cpp:358](../src/index/ix_index_handle.cpp#L358),
[:550](../src/index/ix_index_handle.cpp#L550),
[:629](../src/index/ix_index_handle.cpp#L629)），整棵树都被同一把 `root_latch_` 保护。

把 [src/system/sm_manager.cpp:564..598](../src/system/sm_manager.cpp) 三处调用改成
`*_with_root_lock`（去掉 `txn` 参数，签名见 ix_index_handle.h），重新 `make rmdb -j` ，
重新启动 server、重新 `prepare`。然后：

```bash
python3 index_test/concurrency_bench.py --w 1 --label root_lock \
    --threads 1,2,4,8,16,32 --duration 15 \
    --csv index_test/result/conc_root_lock.csv
```

### Step 3：对比

```bash
python3 index_test/compare_concurrency.py \
    --crab index_test/result/conc_crab.csv \
    --root index_test/result/conc_root_lock.csv
```

输出示例：

```
 threads |    crab thr    root thr  thr ratio |  crab p95   root p95  p95 ratio
       1 |     1820.3     1798.1      1.01x |     1.20      1.22      0.98x
       4 |     5610.8     2960.4      1.89x |     2.10      6.40      0.33x
      16 |    11020.4     2310.7      4.77x |     8.20     38.10      0.22x
      32 |    11540.1     2050.3      5.63x |    17.50     85.40      0.20x
```

**读法**：

- `thr ratio > 1` ⇒ 同样线程数下蟹型协议吞吐更高
- `p95 ratio < 1` ⇒ 蟹型协议尾延迟更低
- 单线程 (`threads=1`) 两者应当相近（差距小于 5%），是正常的；多线程下根锁版会先卡平
- 默认 workload 是 80% select + 20% insert into new_orders；insert 触发 leaf split/B+ 树重平衡，正是蟹型协议优势所在

如果想换 workload 比例，调 `--insert-ratio` (0~1)；纯读 workload 几乎看不到差距，纯写 workload (`0.5+`) 差距最显著。

---

## 三、常见坑

- **数据状态**：`index_bench.py` 在跑完后会重建索引；`concurrency_bench.py` 会往 `new_orders` 里塞行 (`no_o_id` 从 1,000,000 起，按 worker 划分窗口，不会撞键)。跑过若干次后想恢复原始状态，重新 `runner.py --prepare` 即可
- **`--w` 必须匹配**：脚本里随机抽 `c_w_id / s_w_id` 用的就是这个值，不一致会大量返回空结果
- **server 单连接**：每个 worker 自己开一条 TCP，确认 server 接受并发连接（rmdb 默认接受）
- **想用 multiprocessing 而不是 threading**：Python GIL 在 socket I/O 期间是释放的，threading 已经够压；要切换成进程的话可以把 `concurrency_bench.py` 里的 `threading.Thread` / `threading.Barrier` 换成 multiprocessing 的等价物
- **抓火焰图**：把 `concurrency_bench.py` 接到 [performance_testing/main.py](../performance_testing/main.py) 的 `start_test` 替换 TPCC runner 的位置即可
