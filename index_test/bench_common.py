"""
Shared utilities for index_test/index_bench.py and index_test/concurrency_bench.py.

Talks to a running rmdb server (default 127.0.0.1:8765) over the same wire
protocol used by TPCC-Tester/db/rmdb_client.py: one SQL per send, one chunk per
recv. The bench scripts assume TPC-C data has already been loaded by
`python TPCC-Tester/runner.py --prepare --w <N>` (or the equivalent
load_data.sql flow) before they are invoked.
"""

import socket
import time


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
RECV_BUF = 8192


class DBClient:
    """Minimal blocking SQL client. One client == one TCP connection == one session."""

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, recv_buf=RECV_BUF):
        self.recv_buf = recv_buf
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

    def exec(self, sql: str) -> str:
        if not sql.endswith(";"):
            sql = sql + ";"
        self.sock.sendall(sql.encode())
        buf = self.sock.recv(self.recv_buf)
        return buf.decode() if buf else ""

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def is_error_response(resp: str) -> bool:
    if not resp:
        return True
    head = resp.lstrip().lower()
    return head.startswith("error") or head.startswith("abort") or head.startswith("failure")


def percentile(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def latency_stats(latencies_ms):
    """Return summary stats over a list of per-op latencies in milliseconds."""
    if not latencies_ms:
        return {
            "count": 0,
            "avg_ms": 0.0,
            "min_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "max_ms": 0.0,
        }
    return {
        "count": len(latencies_ms),
        "avg_ms": sum(latencies_ms) / len(latencies_ms),
        "min_ms": min(latencies_ms),
        "p50_ms": percentile(latencies_ms, 50),
        "p95_ms": percentile(latencies_ms, 95),
        "p99_ms": percentile(latencies_ms, 99),
        "max_ms": max(latencies_ms),
    }


def timed(client: DBClient, sql: str):
    """Run one SQL and return (latency_ms, response_string)."""
    t0 = time.perf_counter()
    resp = client.exec(sql)
    return (time.perf_counter() - t0) * 1000.0, resp


# Index definitions used by load_data.sql. Kept in one place so index_bench can
# drop and recreate the exact same set.
TPCC_INDEXES = [
    ("warehouse",  ["w_id"]),
    ("district",   ["d_w_id", "d_id"]),
    ("customer",   ["c_w_id", "c_d_id", "c_id"]),
    ("new_orders", ["no_w_id", "no_d_id", "no_o_id"]),
    ("orders",     ["o_w_id", "o_d_id", "o_id"]),
    ("order_line", ["ol_w_id", "ol_d_id", "ol_o_id", "ol_number"]),
    ("item",       ["i_id"]),
    ("stock",      ["s_w_id", "s_i_id"]),
]


def drop_index_sql(table, cols):
    return f"drop index {table}({', '.join(cols)});"


def create_index_sql(table, cols):
    return f"create index {table}({', '.join(cols)});"
