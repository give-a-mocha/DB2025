"""
Index performance benchmark — TPC-C tables, with vs without index.

Flow:
  1. Connect to a running rmdb server that already has TPC-C data loaded.
  2. Phase A: run a fixed batch of point/range queries against the indexed tables
     and record per-query latency.
  3. Drop the relevant TPC-C indexes (customer / stock / item / order_line / orders).
  4. Phase B: re-run the exact same batch — now planner falls back to full scan.
  5. Recreate the indexes (so the server is left in the original state) and print
     a side-by-side comparison.

Pre-conditions
  - rmdb server is running on 127.0.0.1:8765.
  - TPC-C data has been loaded (e.g. `python TPCC-Tester/runner.py --prepare --w 1`).
  - The number of warehouses passed via --w must match what was loaded.

Usage
  python3 index_test/index_bench.py --w 1 --queries 200
  python3 index_test/index_bench.py --w 1 --queries 500 --csv out/index_bench.csv

Note: phase B can be slow when --queries is large (each query is a full scan).
Start with --queries 100~500 the first time.
"""

import argparse
import csv
import os
import random
import sys
import time

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, THIS_DIR)

from bench_common import (
    DBClient,
    TPCC_INDEXES,
    create_index_sql,
    drop_index_sql,
    is_error_response,
    latency_stats,
    timed,
)


CUST_PER_DIST = 3000
DIST_PER_WARE = 10
CNT_ITEM = 100000


def build_query_batch(n, cnt_w, seed=42):
    """Generate `n` queries split evenly across 4 patterns that all hit an index."""
    rng = random.Random(seed)
    queries = []
    patterns = ["customer_pk", "stock_pk", "item_pk", "order_line_range"]
    for i in range(n):
        kind = patterns[i % len(patterns)]
        if kind == "customer_pk":
            w = rng.randint(1, cnt_w)
            d = rng.randint(1, DIST_PER_WARE)
            c = rng.randint(1, CUST_PER_DIST)
            sql = (
                f"select c_id, c_first, c_last, c_balance from customer "
                f"where c_w_id={w} and c_d_id={d} and c_id={c};"
            )
        elif kind == "stock_pk":
            w = rng.randint(1, cnt_w)
            i_id = rng.randint(1, CNT_ITEM)
            sql = (
                f"select s_quantity, s_ytd, s_order_cnt from stock "
                f"where s_w_id={w} and s_i_id={i_id};"
            )
        elif kind == "item_pk":
            i_id = rng.randint(1, CNT_ITEM)
            sql = f"select i_price, i_name from item where i_id={i_id};"
        else:  # order_line_range
            w = rng.randint(1, cnt_w)
            d = rng.randint(1, DIST_PER_WARE)
            lo = rng.randint(1, 2900)
            hi = lo + 10
            sql = (
                f"select ol_i_id, ol_amount from order_line "
                f"where ol_w_id={w} and ol_d_id={d} "
                f"and ol_o_id >= {lo} and ol_o_id <= {hi};"
            )
        queries.append((kind, sql))
    return queries


def run_phase(label, client, queries, verbose):
    """Execute the query batch once and return per-pattern stats."""
    print(f"\n=== Phase: {label} ===  (queries={len(queries)})")
    per_kind = {}
    overall = []
    t_start = time.perf_counter()
    errors = 0
    for idx, (kind, sql) in enumerate(queries):
        lat, resp = timed(client, sql)
        if is_error_response(resp):
            errors += 1
            if verbose:
                print(f"  err [{kind}] {sql.strip()} -> {resp.strip()[:120]}")
        per_kind.setdefault(kind, []).append(lat)
        overall.append(lat)
        if verbose and (idx + 1) % 50 == 0:
            print(f"  {idx + 1}/{len(queries)} done")
    wall = time.perf_counter() - t_start

    print(f"  wall={wall:.3f}s  errors={errors}  ops/s={len(queries)/wall:.1f}")
    rows = []
    for kind, lats in per_kind.items():
        s = latency_stats(lats)
        print(
            f"  [{kind:>16}] n={s['count']:>4}  "
            f"avg={s['avg_ms']:.3f}ms  p50={s['p50_ms']:.3f}  "
            f"p95={s['p95_ms']:.3f}  p99={s['p99_ms']:.3f}  max={s['max_ms']:.3f}"
        )
        rows.append((label, kind, s))
    overall_stats = latency_stats(overall)
    print(
        f"  [{'OVERALL':>16}] n={overall_stats['count']:>4}  "
        f"avg={overall_stats['avg_ms']:.3f}ms  p50={overall_stats['p50_ms']:.3f}  "
        f"p95={overall_stats['p95_ms']:.3f}  p99={overall_stats['p99_ms']:.3f}  max={overall_stats['max_ms']:.3f}"
    )
    rows.append((label, "OVERALL", overall_stats))
    return rows, wall, errors


def drop_all_indexes(client):
    print("\n-- dropping TPC-C indexes --")
    for table, cols in TPCC_INDEXES:
        sql = drop_index_sql(table, cols)
        resp = client.exec(sql)
        marker = "ok" if not is_error_response(resp) else "skip/err"
        print(f"  {sql:<70} [{marker}]")


def create_all_indexes(client):
    print("\n-- recreating TPC-C indexes --")
    for table, cols in TPCC_INDEXES:
        sql = create_index_sql(table, cols)
        resp = client.exec(sql)
        marker = "ok" if not is_error_response(resp) else "skip/err"
        print(f"  {sql:<70} [{marker}]")


def print_comparison(rows_with, rows_without):
    print("\n=== Comparison: with index  vs  without index ===")
    by_kind_with = {r[1]: r[2] for r in rows_with}
    by_kind_without = {r[1]: r[2] for r in rows_without}
    header = f"{'kind':>16} {'with avg(ms)':>14} {'no-idx avg(ms)':>16} {'speedup':>10} {'with p95':>10} {'no-idx p95':>12}"
    print(header)
    print("-" * len(header))
    for kind in by_kind_with:
        a = by_kind_with[kind]["avg_ms"]
        b = by_kind_without.get(kind, {}).get("avg_ms", 0.0)
        speedup = (b / a) if a > 0 else 0.0
        p95a = by_kind_with[kind]["p95_ms"]
        p95b = by_kind_without.get(kind, {}).get("p95_ms", 0.0)
        print(
            f"{kind:>16} {a:>14.3f} {b:>16.3f} {speedup:>9.2f}x {p95a:>10.3f} {p95b:>12.3f}"
        )


def write_csv(path, rows_with, rows_without):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            ["phase", "kind", "count", "avg_ms", "min_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms"]
        )
        for phase_rows in (rows_with, rows_without):
            for label, kind, s in phase_rows:
                w.writerow(
                    [
                        label, kind, s["count"], f"{s['avg_ms']:.4f}", f"{s['min_ms']:.4f}",
                        f"{s['p50_ms']:.4f}", f"{s['p95_ms']:.4f}", f"{s['p99_ms']:.4f}",
                        f"{s['max_ms']:.4f}",
                    ]
                )
    print(f"\nCSV written: {path}")


def main():
    p = argparse.ArgumentParser(description="TPC-C index performance benchmark")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--w", type=int, required=True, help="warehouse count loaded into the server")
    p.add_argument("--queries", type=int, default=200, help="number of queries per phase")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--skip-recreate",
        action="store_true",
        help="do not recreate indexes after the no-index phase (leaves server in a degraded state)",
    )
    p.add_argument("--csv", default="", help="optional CSV output path")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    queries = build_query_batch(args.queries, args.w, args.seed)
    print(f"Generated {len(queries)} queries (warehouses={args.w}, seed={args.seed})")

    client = DBClient(args.host, args.port)
    try:
        rows_with, _, _ = run_phase("with_index", client, queries, args.verbose)
        drop_all_indexes(client)
        rows_without, _, _ = run_phase("no_index", client, queries, args.verbose)
        if not args.skip_recreate:
            create_all_indexes(client)
        print_comparison(rows_with, rows_without)
        if args.csv:
            write_csv(args.csv, rows_with, rows_without)
    finally:
        client.close()


if __name__ == "__main__":
    main()
