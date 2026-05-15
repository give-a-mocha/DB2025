"""
B+ tree concurrency benchmark — crab protocol vs root-lock.

This script does NOT switch implementations on its own. To compare the two:

  1. Build the server with the crab (latch-coupling) variant — the default.
     `insert_entry / get_value / delete_entry` are called from
     src/system/sm_manager.cpp:564..598. Start the server, load TPC-C data via
     TPCC-Tester/runner.py --prepare, then:
        python index_test/concurrency_bench.py --w 1 --label crab \
            --csv index_test/result/conc_crab.csv

  2. Edit src/system/sm_manager.cpp to call the *_with_root_lock variants
     instead (insert_entry_with_root_lock / get_value_with_root_lock /
     delete_entry_with_root_lock), rebuild, restart, re-load. Then:
        python index_test/concurrency_bench.py --w 1 --label root_lock \
            --csv index_test/result/conc_root_lock.csv

  3. Diff the two CSVs — column `throughput_ops_s` per thread count is the headline.

Workload per worker
  - 80% point read on customer using the (c_w_id, c_d_id, c_id) index
  - 20% insert into new_orders using the (no_w_id, no_d_id, no_o_id) index
  Each worker owns a disjoint no_o_id range (BASE + worker_id * STEP) so inserts
  don't collide.  Auto-commit per statement; we are stressing the index latch
  path, not the txn manager.

Sweeps a list of thread counts and reports throughput + latency per level.
"""

import argparse
import csv
import os
import random
import sys
import threading
import time

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, THIS_DIR)

from bench_common import DBClient, is_error_response, latency_stats


CUST_PER_DIST = 3000
DIST_PER_WARE = 10
INSERT_BASE_NO_O_ID = 1_000_000  # well above the loaded max (3000)
INSERT_STEP = 1_000_000          # per-worker key window


def worker_loop(
    worker_id,
    host,
    port,
    cnt_w,
    duration_s,
    insert_ratio,
    seed,
    barrier,
    out,
    stop_flag,
):
    """One worker: open its own socket, spin until duration expires."""
    rng = random.Random(seed + worker_id)
    client = DBClient(host, port)
    next_no_o_id = INSERT_BASE_NO_O_ID + worker_id * INSERT_STEP
    latencies = []
    reads = 0
    inserts = 0
    errors = 0
    try:
        barrier.wait()  # release together so timing is consistent
        t_end = time.perf_counter() + duration_s
        while time.perf_counter() < t_end and not stop_flag["stop"]:
            if rng.random() < insert_ratio:
                d = rng.randint(1, DIST_PER_WARE)
                w = rng.randint(1, cnt_w)
                sql = f"insert into new_orders values({next_no_o_id}, {d}, {w});"
                next_no_o_id += 1
                op_kind = "insert"
            else:
                w = rng.randint(1, cnt_w)
                d = rng.randint(1, DIST_PER_WARE)
                c = rng.randint(1, CUST_PER_DIST)
                sql = (
                    f"select c_id, c_balance from customer "
                    f"where c_w_id={w} and c_d_id={d} and c_id={c};"
                )
                op_kind = "read"

            t0 = time.perf_counter()
            try:
                resp = client.exec(sql)
            except Exception as e:
                errors += 1
                stop_flag["stop"] = True
                print(f"  worker {worker_id} socket error: {e}", flush=True)
                break
            lat_ms = (time.perf_counter() - t0) * 1000.0
            latencies.append(lat_ms)
            if is_error_response(resp):
                errors += 1
            elif op_kind == "read":
                reads += 1
            else:
                inserts += 1
    finally:
        client.close()
    out[worker_id] = {
        "reads": reads,
        "inserts": inserts,
        "errors": errors,
        "latencies": latencies,
    }


def run_level(thread_count, args):
    """Run one concurrency level and return aggregated stats."""
    barrier = threading.Barrier(thread_count + 1)
    stop_flag = {"stop": False}
    out = {}
    threads = []
    for i in range(thread_count):
        t = threading.Thread(
            target=worker_loop,
            args=(
                i, args.host, args.port, args.w, args.duration,
                args.insert_ratio, args.seed, barrier, out, stop_flag,
            ),
            daemon=True,
        )
        t.start()
        threads.append(t)

    barrier.wait()
    t0 = time.perf_counter()
    for t in threads:
        t.join()
    wall = time.perf_counter() - t0

    total_reads = sum(o["reads"] for o in out.values())
    total_inserts = sum(o["inserts"] for o in out.values())
    total_errors = sum(o["errors"] for o in out.values())
    total_ops = total_reads + total_inserts
    all_lat = []
    for o in out.values():
        all_lat.extend(o["latencies"])
    stats = latency_stats(all_lat)

    throughput = total_ops / wall if wall > 0 else 0.0
    print(
        f"  threads={thread_count:>3}  wall={wall:6.2f}s  "
        f"ops={total_ops:>7}  thr={throughput:>9.1f} ops/s  "
        f"reads={total_reads}  inserts={total_inserts}  errors={total_errors}  "
        f"avg={stats['avg_ms']:.2f}ms  p95={stats['p95_ms']:.2f}  p99={stats['p99_ms']:.2f}"
    )
    return {
        "threads": thread_count,
        "wall_s": wall,
        "ops": total_ops,
        "reads": total_reads,
        "inserts": total_inserts,
        "errors": total_errors,
        "throughput_ops_s": throughput,
        **{k: stats[k] for k in ("avg_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms")},
    }


def write_csv(path, label, results):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "label", "threads", "wall_s", "ops", "reads", "inserts",
                "errors", "throughput_ops_s", "avg_ms", "p50_ms", "p95_ms",
                "p99_ms", "max_ms",
            ]
        )
        for r in results:
            w.writerow(
                [
                    label, r["threads"], f"{r['wall_s']:.3f}", r["ops"],
                    r["reads"], r["inserts"], r["errors"],
                    f"{r['throughput_ops_s']:.2f}", f"{r['avg_ms']:.3f}",
                    f"{r['p50_ms']:.3f}", f"{r['p95_ms']:.3f}",
                    f"{r['p99_ms']:.3f}", f"{r['max_ms']:.3f}",
                ]
            )
    print(f"\nCSV written: {path}")


def parse_threads(spec):
    return [int(x) for x in spec.split(",") if x.strip()]


def main():
    p = argparse.ArgumentParser(description="B+ tree concurrency benchmark")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--w", type=int, required=True, help="warehouse count loaded into the server")
    p.add_argument(
        "--threads", default="1,2,4,8,16,32",
        help="comma-separated list of thread counts to sweep",
    )
    p.add_argument("--duration", type=float, default=15.0, help="seconds per level")
    p.add_argument("--insert-ratio", type=float, default=0.2, help="probability of insert per op")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--label", required=True,
        help="tag for this run (e.g. 'crab' or 'root_lock') — written into the CSV",
    )
    p.add_argument("--csv", default="", help="optional CSV output path")
    args = p.parse_args()

    thread_levels = parse_threads(args.threads)
    print(
        f"Concurrency bench — label={args.label}  duration={args.duration}s  "
        f"insert_ratio={args.insert_ratio}  threads={thread_levels}"
    )

    # quick warmup: one connection runs a few selects so caches are primed.
    warmup = DBClient(args.host, args.port)
    for _ in range(20):
        warmup.exec("select c_id from customer where c_w_id=1 and c_d_id=1 and c_id=1;")
    warmup.close()

    results = []
    for n in thread_levels:
        r = run_level(n, args)
        results.append(r)

    if args.csv:
        write_csv(args.csv, args.label, results)

    # Headline summary
    print("\n=== Summary (label={}) ===".format(args.label))
    print(f"{'threads':>8} {'throughput_ops_s':>18} {'avg_ms':>10} {'p95_ms':>10} {'p99_ms':>10}")
    for r in results:
        print(
            f"{r['threads']:>8} {r['throughput_ops_s']:>18.1f} "
            f"{r['avg_ms']:>10.2f} {r['p95_ms']:>10.2f} {r['p99_ms']:>10.2f}"
        )


if __name__ == "__main__":
    main()
