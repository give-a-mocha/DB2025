#!/usr/bin/env python3
"""
RMDB SQL 执行速度基准测试
支持并发测试，统计 P50 / P90 / P99 延迟

用法：
    python benchmark.py [--host 127.0.0.1] [--port 8765]
                        [--concurrency 8] [--iterations 200]
                        [--skip-setup] [--skip-teardown]
"""

import socket
import time
import threading
import statistics
import argparse
import sys
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

# ─────────────────────────── TCP 连接工具 ────────────────────────────

_THREAD_LOCAL = threading.local()


def _create_connection(host: str, port: int, timeout: float) -> socket.socket:
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.settimeout(timeout)
    conn.connect((host, port))
    return conn


def _get_thread_connection(host: str, port: int, timeout: float) -> socket.socket:
    conn = getattr(_THREAD_LOCAL, "conn", None)
    addr = getattr(_THREAD_LOCAL, "addr", None)

    if conn is None or addr != (host, port):
        if conn is not None:
            conn.close()
        conn = _create_connection(host, port, timeout)
        _THREAD_LOCAL.conn = conn
        _THREAD_LOCAL.addr = (host, port)

    conn.settimeout(timeout)
    return conn


def _reset_thread_connection():
    conn = getattr(_THREAD_LOCAL, "conn", None)
    if conn is not None:
        conn.close()
    _THREAD_LOCAL.conn = None
    _THREAD_LOCAL.addr = None


def _receive_response(conn: socket.socket) -> str:
    """
    与官方 rmdb_client 协议一致：服务端每条 SQL 回复一个数据包，
    客户端一次 recv 即视为一条完整响应。
    响应中若含 '\0'，则截断到 '\0' 之前；否则按原样返回。
    """
    data = conn.recv(8192)
    if not data:
        raise ConnectionError("服务器在响应结束前关闭了连接")
    if b"\0" in data:
        data = data.split(b"\0", 1)[0]
    return data.decode("utf-8", errors="replace")


def send_query(host: str, port: int, sql: str, timeout: float = 10.0) -> tuple[str, float]:
    """
    每个线程固定复用一个长连接，返回 (响应文本, 耗时秒)。
    协议：发送 null-terminated 字符串；接收 null-terminated 字符串。
    """
    if not sql.rstrip().endswith(";"):
        sql = sql.rstrip() + ";"

    conn = _get_thread_connection(host, port, timeout)
    try:
        t0 = time.perf_counter()
        conn.sendall((sql + "\0").encode("utf-8"))
        response = _receive_response(conn)
        elapsed = time.perf_counter() - t0
        return response, elapsed
    except Exception:
        _reset_thread_connection()
        raise


# ─────────────────────────── 数据结构 ────────────────────────────────

@dataclass
class BenchResult:
    name: str
    latencies: list[float] = field(default_factory=list)
    errors: int = 0

    def add(self, latency: float):
        self.latencies.append(latency)

    def percentile(self, p: float) -> float:
        if not self.latencies:
            return float("nan")
        sorted_lats = sorted(self.latencies)
        idx = int(len(sorted_lats) * p / 100)
        idx = min(idx, len(sorted_lats) - 1)
        return sorted_lats[idx]

    def report(self) -> str:
        if not self.latencies:
            return f"  {self.name}: 无数据 (errors={self.errors})"
        n = len(self.latencies)
        avg = statistics.mean(self.latencies)
        p50 = self.percentile(50)
        p90 = self.percentile(90)
        p99 = self.percentile(99)
        mn = min(self.latencies)
        mx = max(self.latencies)
        total = sum(self.latencies)
        tps = n / total if total > 0 else 0
        return (
            f"  [{self.name}]\n"
            f"    请求数={n}  错误数={self.errors}\n"
            f"    avg={avg*1000:.2f}ms  min={mn*1000:.2f}ms  max={mx*1000:.2f}ms\n"
            f"    P50={p50*1000:.2f}ms  P90={p90*1000:.2f}ms  P99={p99*1000:.2f}ms\n"
            f"    吞吐量 ≈ {tps:.1f} QPS"
        )


# ─────────────────────────── 测试场景定义 ────────────────────────────

def make_scenarios(data_scale: int):
    ids = list(range(1, data_scale + 1))
    user_ids = list(range(1, data_scale + 1))
    order_ids = list(range(1, data_scale * 3 + 1))

    def rand_id(pool):
        return random.choice(pool)

    scenarios = [
        (
            "SELECT 点查询(PK)",
            lambda: f"SELECT id, name, age, score FROM bench_users WHERE id = {rand_id(ids)}",
        ),
                (
            "JOIN 两表(orders+users)",
            lambda: (
                lambda uid=rand_id(user_ids):
                f"SELECT bench_orders.id, bench_users.name, bench_orders.amount, bench_orders.status "
                f"FROM bench_orders, bench_users "
                f"WHERE bench_orders.user_id = bench_users.id AND bench_users.id = {uid}"
            )(),
        ),
        (
            "JOIN 三表(items+orders+products)",
            lambda: (
                lambda oid=rand_id(order_ids):
                f"SELECT bench_order_items.quantity, bench_order_items.price, "
                f"bench_orders.status, bench_products.name "
                f"FROM bench_order_items, bench_orders, bench_products "
                f"WHERE bench_order_items.order_id = bench_orders.id "
                f"AND bench_order_items.product_id = bench_products.id "
                f"AND bench_orders.id = {oid}"
            )(),
        ),
        # (
        #     "SELECT 范围扫描(age)",
        #     lambda: (
        #         lambda a=random.randint(18, 50):
        #         f"SELECT id, name, age FROM bench_users WHERE age >= {a} AND age <= {a+5}"
        #     )(),
        # ),
        # (
        #     "SELECT ORDER BY+LIMIT",
        #     lambda: "SELECT id, name, score FROM bench_users ORDER BY score LIMIT 10",
        # ),
        # (
        #     "SELECT COUNT(*)",
        #     lambda: (
        #         lambda uid=rand_id(user_ids):
        #         f"SELECT COUNT(*) FROM bench_orders WHERE user_id = {uid}"
        #     )(),
        # ),
        # (
        #     "SELECT SUM(amount)",
        #     lambda: (
        #         lambda uid=rand_id(user_ids):
        #         f"SELECT SUM(amount) FROM bench_orders WHERE user_id = {uid}"
        #     )(),
        # ),
        # (
        #     "SELECT AVG(amount)",
        #     lambda: (
        #         lambda uid=rand_id(user_ids):
        #         f"SELECT AVG(amount) FROM bench_orders WHERE user_id = {uid}"
        #     )(),
        # ),
        (
            "SELECT GROUP BY",
            lambda: "SELECT status, COUNT(*) FROM bench_orders GROUP BY status",
        ),
        (
            "JOIN+GROUP BY+HAVING",
            lambda: (
                "SELECT bench_users.name, COUNT(*) "
                "FROM bench_orders, bench_users "
                "WHERE bench_orders.user_id = bench_users.id "
                "GROUP BY bench_users.name "
                "HAVING COUNT(*) >= 1"
            ),
        ),
        # (
        #     "INSERT(users_insert)",
        #     lambda: (
        #         lambda nm="".join(random.choices(string.ascii_lowercase, k=6)),
        #             ag=random.randint(18, 60),
        #             sc=round(random.uniform(0, 100), 2):
        #         f"INSERT INTO bench_users_insert VALUES "
        #         f"({random.randint(1000000, 9999999)}, '{nm}', {ag}, {sc})"
        #     )(),
        # ),
        # (
        #     "UPDATE(score by id)",
        #     lambda: (
        #         lambda uid=rand_id(ids), sc=round(random.uniform(0, 100), 2):
        #         f"UPDATE bench_users SET score = {sc} WHERE id = {uid}"
        #     )(),
        # ),
        # (
        #     "读写混合(join+range_update)",
        #     lambda: (
        #         lambda uid=rand_id(user_ids),
        #                 iid=random.randint(1, max(1, data_scale * 5 - data_scale)),
        #                 price=round(random.uniform(1, 200), 2): [
        #             f"UPDATE bench_order_items SET price = {price} WHERE id >= {iid} AND id <= {iid + data_scale}",
        #             f"SELECT * "
        #             f"FROM bench_users "
        #             f"WHERE id = {uid}"
        #         ]
        #     )(),
        # ),
    ]
    return scenarios


# ─────────────────────────── DDL & 数据初始化 ────────────────────────

SETUP_SQLS = """
DROP TABLE bench_order_items;
DROP TABLE bench_orders;
DROP TABLE bench_products;
DROP TABLE bench_users;
DROP TABLE bench_users_insert;
CREATE TABLE bench_users (id INT, name CHAR(32), age INT, score FLOAT);
CREATE TABLE bench_orders (id INT, user_id INT, amount FLOAT, status CHAR(16));
CREATE TABLE bench_products (id INT, name CHAR(32), price FLOAT, stock INT);
CREATE TABLE bench_order_items (id INT, order_id INT, product_id INT, quantity INT, price FLOAT);
CREATE TABLE bench_users_insert (id INT, name CHAR(32), age INT, score FLOAT);
CREATE INDEX bench_users(id);
CREATE INDEX bench_orders(id);
CREATE INDEX bench_products(id);
CREATE INDEX bench_order_items(id);
CREATE INDEX bench_users_insert(id);
""".strip()

TEARDOWN_SQLS = """
DROP TABLE bench_order_items;
DROP TABLE bench_orders;
DROP TABLE bench_products;
DROP TABLE bench_users;
DROP TABLE bench_users_insert;
""".strip()


def setup(host, port, data_scale):
    statuses = ["pending", "shipped", "done"]
    product_count = max(data_scale // 2, 1)
    order_count = data_scale * 3
    item_count = data_scale * 5

    print(f"\n[Setup] 建表中……")
    for sql in SETUP_SQLS.splitlines():
        sql = sql.strip()
        if not sql:
            continue
        resp, _ = send_query(host, port, sql)
        if "error" in resp.lower() and not sql.startswith("DROP"):
            print(f"  警告: {sql!r} => {resp.strip()!r}")

    print(f"[Setup] 插入 {data_scale} 条用户数据……")
    for i in range(1, data_scale + 1):
        name = f"user_{i:05d}"
        age = 18 + (i % 42)
        score = round((i * 7.3) % 100, 2)
        send_query(host, port, f"INSERT INTO bench_users VALUES ({i}, '{name}', {age}, {score})")

    print(f"[Setup] 插入 {product_count} 条产品数据……")
    for i in range(1, product_count + 1):
        name = f"prod_{i:04d}"
        price = round((i * 3.7) % 500 + 1, 2)
        stock = (i * 13) % 1000
        send_query(host, port, f"INSERT INTO bench_products VALUES ({i}, '{name}', {price}, {stock})")

    print(f"[Setup] 插入 {order_count} 条订单数据……")
    for i in range(1, order_count + 1):
        uid = (i % data_scale) + 1
        amount = round((i * 11.1) % 999 + 1, 2)
        status = statuses[i % 3]
        send_query(host, port, f"INSERT INTO bench_orders VALUES ({i}, {uid}, {amount}, '{status}')")

    print(f"[Setup] 插入 {item_count} 条订单明细数据……")
    for i in range(1, item_count + 1):
        oid = (i % order_count) + 1
        pid = (i % product_count) + 1
        qty = (i % 10) + 1
        price = round((i * 2.5) % 200 + 1, 2)
        send_query(host, port, f"INSERT INTO bench_order_items VALUES ({i}, {oid}, {pid}, {qty}, {price})")

    print("[Setup] 数据初始化完成。\n")


def teardown(host, port):
    print("\n[Teardown] 清理测试表……")
    for sql in TEARDOWN_SQLS.splitlines():
        sql = sql.strip()
        if sql:
            send_query(host, port, sql)
    print("[Teardown] 完成。")


# ─────────────────────────── 并发执行引擎 ────────────────────────────

def run_scenario(
    name: str,
    sql_gen,
    host: str,
    port: int,
    concurrency: int,
    iterations: int,
) -> BenchResult:
    result = BenchResult(name=name)
    lock = threading.Lock()

    def worker(_):
        sql = sql_gen()
        try:
            if isinstance(sql, list):
                total_elapsed = 0.0
                for s in sql:
                    _, elapsed = send_query(host, port, s)
                    total_elapsed += elapsed
                with lock:
                    result.add(total_elapsed)
            else:
                _, elapsed = send_query(host, port, sql)
                with lock:
                    result.add(elapsed)
        except Exception:
            with lock:
                result.errors += 1

    actual_concurrency = min(concurrency, 14)

    with ThreadPoolExecutor(max_workers=actual_concurrency) as pool:
        futures = [pool.submit(worker, i) for i in range(iterations)]
        for f in as_completed(futures):
            pass

    return result


# ─────────────────────────── 主流程 ──────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="RMDB SQL 基准测试")
    parser.add_argument("--host", default="127.0.0.1", help="服务器地址")
    parser.add_argument("--port", type=int, default=8765, help="服务器端口")
    parser.add_argument("--concurrency", type=int, default=8, help="并发线程数 (≤14)")
    parser.add_argument("--iterations", type=int, default=200, help="每个场景执行次数")
    parser.add_argument("--data-scale", type=int, default=None,
                        help="基础数据量（用户数，订单数=3x，明细=5x）；不填则交互式询问")
    parser.add_argument("--skip-setup", action="store_true", help="跳过建表和插入数据")
    parser.add_argument("--skip-teardown", action="store_true", help="跳过测试后清理")
    parser.add_argument("-o", "--output", default=None, help="将结果表格输出到指定文件")
    parser.add_argument("--scenarios", nargs="*", help="只运行指定序号的场景 (0-based, 默认全部)")
    args = parser.parse_args()

    host, port = args.host, args.port
    concurrency = min(args.concurrency, 14)
    iterations = args.iterations

    # 交互式询问数据量
    if args.data_scale is not None:
        data_scale = args.data_scale
    else:
        while True:
            try:
                raw = input("请输入基础数据量（用户数，订单数=3x，明细=5x，默认500）: ").strip()
                data_scale = int(raw) if raw else 500
                if data_scale <= 0:
                    print("  数据量必须为正整数，请重新输入。")
                    continue
                break
            except ValueError:
                print("  请输入有效的整数。")

    print("=" * 60)
    print("  RMDB SQL 基准测试")
    print(f"  目标: {host}:{port}")
    print(f"  并发数: {concurrency}  每场景迭代: {iterations}")
    print(f"  数据规模: {data_scale} 用户 / {data_scale*3} 订单 / {data_scale*5} 明细")
    print("=" * 60)

    # 先确认服务器可达
    try:
        send_query(host, port, "SHOW TABLES")
    except Exception as e:
        print(f"\n[错误] 无法连接到 {host}:{port} — {e}")
        print("请先启动 RMDB 服务器。")
        sys.exit(1)

    if not args.skip_setup:
        setup(host, port, data_scale)

    scenarios = make_scenarios(data_scale)
    if args.scenarios:
        selected_indices = [int(x) for x in args.scenarios]
        scenarios = [scenarios[i] for i in selected_indices if i < len(scenarios)]

    results: list[BenchResult] = []

    print(f"\n开始执行 {len(scenarios)} 个测试场景……\n")
    total_start = time.perf_counter()

    for idx, (name, sql_gen) in enumerate(scenarios):
        print(f"[{idx+1}/{len(scenarios)}] {name} … ", end="", flush=True)
        t0 = time.perf_counter()
        res = run_scenario(name, sql_gen, host, port, concurrency, iterations)
        elapsed = time.perf_counter() - t0
        results.append(res)
        print(f"完成 ({elapsed:.1f}s, errors={res.errors})")

    total_elapsed = time.perf_counter() - total_start

    # ── 输出汇总报告 ─────────────────────────────────────────────────
    lines: list[str] = []
    lines.append("=" * 60)
    lines.append("  基准测试结果汇总")
    lines.append("=" * 60)
    for res in results:
        lines.append(res.report())
        lines.append("")

    # ── 横向对比表格 ─────────────────────────────────────────────────
    lines.append("=" * 60)
    lines.append(f"  {'场景':<30} {'P50(ms)':>9} {'P90(ms)':>9} {'P99(ms)':>9} {'QPS':>8}")
    lines.append("-" * 60)
    for res in results:
        if res.latencies:
            p50 = res.percentile(50) * 1000
            p90 = res.percentile(90) * 1000
            p99 = res.percentile(99) * 1000
            tps = len(res.latencies) / sum(res.latencies) if res.latencies else 0
            name_short = res.name[:29]
            lines.append(f"  {name_short:<30} {p50:>9.2f} {p90:>9.2f} {p99:>9.2f} {tps:>8.1f}")
        else:
            lines.append(f"  {res.name:<30} {'N/A':>9} {'N/A':>9} {'N/A':>9} {'N/A':>8}")

    lines.append("=" * 60)
    lines.append(f"\n总用时: {total_elapsed:.1f}s\n")

    report = "\n".join(lines)
    print("\n" + report)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report + "\n")
        print(f"结果已写入: {args.output}")

    if not args.skip_teardown:
        teardown(host, port)


if __name__ == "__main__":
    main()
