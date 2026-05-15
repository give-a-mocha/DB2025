"""
Diff two concurrency_bench.py CSVs (typically crab vs root_lock) and print a
side-by-side scalability table.

Usage:
  python3 index_test/compare_concurrency.py \
      --crab index_test/result/conc_crab.csv \
      --root index_test/result/conc_root_lock.csv
"""

import argparse
import csv


def load(path):
    rows = {}
    with open(path) as f:
        for r in csv.DictReader(f):
            rows[int(r["threads"])] = r
    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--crab", required=True)
    p.add_argument("--root", required=True)
    args = p.parse_args()

    crab = load(args.crab)
    root = load(args.root)
    levels = sorted(set(crab) & set(root))
    if not levels:
        print("No matching thread levels in the two CSVs.")
        return

    hdr = (
        f"{'threads':>8} | "
        f"{'crab thr':>11} {'root thr':>11} {'thr ratio':>10} | "
        f"{'crab p95':>10} {'root p95':>10} {'p95 ratio':>10}"
    )
    print(hdr)
    print("-" * len(hdr))
    for n in levels:
        ct = float(crab[n]["throughput_ops_s"])
        rt = float(root[n]["throughput_ops_s"])
        cp = float(crab[n]["p95_ms"])
        rp = float(root[n]["p95_ms"])
        thr_ratio = ct / rt if rt > 0 else 0.0
        p95_ratio = cp / rp if rp > 0 else 0.0
        print(
            f"{n:>8} | {ct:>11.1f} {rt:>11.1f} {thr_ratio:>9.2f}x | "
            f"{cp:>10.2f} {rp:>10.2f} {p95_ratio:>9.2f}x"
        )

    print("\nthr ratio > 1  => crab is faster at that concurrency.")
    print("p95 ratio < 1  => crab has lower tail latency.")


if __name__ == "__main__":
    main()
