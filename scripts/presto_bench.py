"""G3: chạy truy vấn qua Presto 0.286 THẬT (REST API) với connector Presto-OCS của nhóm.

  python scripts/presto_bench.py explain       # EXPLAIN trên Presto gốc (Hive) và Presto-OCS -> results/presto/
  python scripts/presto_bench.py bench --reps 3
  python scripts/presto_bench.py features      # R1/R3/residual: so Presto-OCS với Presto+Hive (oracle độc lập)
  python scripts/presto_bench.py bench --set q3 --reps 5   # Q3 trên Presto: 4 kiểu nén x {filter, all}
  python scripts/presto_bench.py bench --set q4 --reps 10  # Q4 trên Presto: 1 file, phân rã từ EventListener

Ba catalog cùng một connector, khác mức pushdown của Local Optimizer (ConnectorPlanOptimizer):
  ocsnopd  : none   – TableScan chỉ chọn cột, mọi toán tử chạy ở Presto
  ocsfilter: filter – chỉ đẩy Filter (tương đương S3 Select)
  ocs      : all    – Filter / Project / Aggregation / TopN theo quy tắc của connector
Mỗi lần chạy được so khớp với oracle DuckDB; data movement đo bằng tx_bytes của node OCS.
"""
import argparse
import json
import os
import random
import sys
import time
import urllib.request

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, "/app")
from ocsdemo.engine import Engine, same_result  # noqa: E402

PRESTO = "http://presto:8080"
OUT = "/app/results/presto"

QUERIES = {
    "DW": ("deepwater",
           "SELECT timestep, max((rowid % 250000) / 500) AS max_height FROM deepwater WHERE v02 > 0.1 GROUP BY timestep"),
    "L": ("laghos_64",
          "SELECT timestep, vertex_id, min(vertex_id) AS vid, min(x) AS min_x, min(y) AS min_y, min(z) AS min_z, "
          "avg(e) AS avg_e FROM laghos WHERE x BETWEEN 0.8 AND 3.2 AND y BETWEEN 0.8 AND 3.2 AND z BETWEEN 0.8 AND 3.2 "
          "GROUP BY timestep, vertex_id ORDER BY avg_e, timestep, vertex_id LIMIT 100"),
    "TQ": ("tpch_q1_double",
           "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, "
           "sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, "
           "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, "
           "avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order "
           "FROM lineitem_double WHERE l_shipdate <= DATE '1998-09-02' "
           "GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"),
}
# Q3 trên Presto: cùng truy vấn Deep Water trên 4 bảng khác kiểu nén (cùng oracle)
Q3 = {f"DW_{c}": ("deepwater", QUERIES["DW"][1].replace("FROM deepwater", f"FROM deepwater_{c}" if c != "none" else "FROM deepwater"))
      for c in ("none", "snappy", "gzip", "zstd")}
# Q4 trên Presto: truy vấn Laghos trên đúng 1 file như bài báo
Q4 = {"L1": ("laghos_1", QUERIES["L"][1].replace("FROM laghos", "FROM laghos_one"))}
# R4 trên Presto thật: Laghos đủ 256 file (bảng laghos_full)
L256 = {"L256": ("laghos_256", QUERIES["L"][1].replace("FROM laghos", "FROM laghos_full"))}
SETS = {"main": (QUERIES, ("ocsnopd", "ocsfilter", "ocs"), "presto"),
        "q3": (Q3, ("ocsfilter", "ocs"), "presto_q3"),
        "q4": (Q4, ("ocsnopd", "ocsfilter", "ocs"), "presto_q4"),
        "l256": (L256, ("ocsfilter", "ocs"), "presto_l256")}

# Kiểm tra chức năng: kết quả Presto-OCS phải trùng Presto + Hive connector (đường đọc Parquet độc lập của Presto)
FEATURES = {
    # R1: gom nhóm hai pha (nhóm trải nhiều object) trên DOUBLE
    "TQ_two_phase": ("lineitem_double", QUERIES["TQ"][1], "Aggregation(PARTIAL)"),
    # R3: decimal + hai pha với sum/min/max/count
    "TQ_decimal_sum": ("lineitem",
                       "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, "
                       "sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, min(l_discount) AS min_disc, "
                       "max(l_tax) AS max_tax, count(*) AS n FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' "
                       "GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus", "Aggregation(PARTIAL)"),
    # R3: avg(decimal) không đẩy (làm tròn khác nhau) -> chỉ Filter, kết quả vẫn đúng
    "TQ_decimal_avg": ("lineitem", QUERIES["TQ"][1].replace("lineitem_double", "lineitem"), "Filter"),
    # Window function: gom nhóm hoàn chỉnh đẩy xuống, rank() giữ ở Presto (residual)
    "DW_window": ("deepwater",
                  "SELECT timestep, max_height, rank() OVER (ORDER BY max_height DESC, timestep) AS rk FROM ("
                  + QUERIES["DW"][1] + ") t", "Aggregation(COMPLETE)"),
    # Hàm không dịch được (LIKE): Filter tách đôi, phần LIKE + gom nhóm ở Presto
    "TQ_like_residual": ("lineitem_double",
                         "SELECT l_shipmode, count(*) AS n, sum(l_extendedprice) AS rev FROM lineitem_double "
                         "WHERE l_shipdate >= DATE '1998-06-01' AND l_comment LIKE '%special%' "
                         "GROUP BY l_shipmode ORDER BY l_shipmode", "Filter(partial)"),
}
HIVE_DIRS = {"lineitem": "tpch", "lineitem_double": "tpch_double", "deepwater": "deepwater_none"}

HIVE_DDL = [
    "CREATE SCHEMA IF NOT EXISTS hive.demo",
    "CREATE TABLE IF NOT EXISTS hive.demo.lineitem_double (l_orderkey bigint, l_partkey bigint, l_suppkey bigint, "
    "l_linenumber bigint, l_quantity double, l_extendedprice double, l_discount double, l_tax double, "
    "l_returnflag varchar, l_linestatus varchar, l_shipdate date, l_commitdate date, l_receiptdate date, "
    "l_shipinstruct varchar, l_shipmode varchar, l_comment varchar) "
    "WITH (format = 'PARQUET', external_location = 'file:///data/tpch_double')",
    "CREATE TABLE IF NOT EXISTS hive.demo.deepwater (rowid bigint, v02 double, v03 double, timestep bigint) "
    "WITH (format = 'PARQUET', external_location = 'file:///data/deepwater_none')",
]


def presto(sql, catalog="ocs", schema="default"):
    """Gửi một câu lệnh qua Presto REST API, trả (cột, dòng)."""
    req = urllib.request.Request(f"{PRESTO}/v1/statement", data=sql.encode(), method="POST",
                                 headers={"X-Presto-User": "demo", "X-Presto-Catalog": catalog,
                                          "X-Presto-Schema": schema})
    r = json.load(urllib.request.urlopen(req))
    cols, rows = None, []
    while True:
        if "error" in r:
            raise RuntimeError(r["error"].get("message"))
        cols = cols or [c["name"] for c in r.get("columns", [])] or None
        rows += r.get("data", [])
        if "nextUri" not in r:
            return cols, rows
        r = json.load(urllib.request.urlopen(r["nextUri"]))


def to_table(cols, rows):
    return pa.table({c: [row[i] for row in rows] for i, c in enumerate(cols)})


def explain():
    os.makedirs(OUT, exist_ok=True)
    for ddl in HIVE_DDL:
        presto(ddl, catalog="hive", schema="default")
    hive_q = {"TQ": QUERIES["TQ"][1].replace("FROM lineitem_double", "FROM hive.demo.lineitem_double"),
              "DW": QUERIES["DW"][1].replace("FROM deepwater", "FROM hive.demo.deepwater")}
    for k, sql in hive_q.items():
        _, rows = presto("EXPLAIN " + sql, catalog="hive", schema="demo")
        open(f"{OUT}/explain_hive_{k}.txt", "w").write(rows[0][0])
    for cat in ("ocsnopd", "ocsfilter", "ocs"):
        for k, (_, sql) in QUERIES.items():
            _, rows = presto("EXPLAIN " + sql, catalog=cat)
            open(f"{OUT}/explain_{cat}_{k}.txt", "w").write(rows[0][0])
    print("đã lưu EXPLAIN vào", OUT)


def hive_tables():
    """Tạo bảng Hive (file metastore) trỏ vào cùng thư mục Parquet, schema lấy từ connector OCS."""
    presto("CREATE SCHEMA IF NOT EXISTS hive.demo", catalog="hive")
    for t, d in HIVE_DIRS.items():
        _, rows = presto(f"DESCRIBE ocs.default.{t}")
        cols = ", ".join(f"{r[0]} {r[1]}" for r in rows)
        presto(f"CREATE TABLE IF NOT EXISTS hive.demo.{t} ({cols}) "
               f"WITH (format = 'PARQUET', external_location = 'file:///data/{d}')", catalog="hive")


def features():
    os.makedirs(OUT, exist_ok=True)
    hive_tables()
    out = []
    for k, (t, sql, expect) in FEATURES.items():
        hcols, hrows = presto(sql.replace(f"FROM {t}", f"FROM hive.demo.{t}"), catalog="hive", schema="demo")
        _, plan = presto("EXPLAIN " + sql, catalog="ocs")
        open(f"{OUT}/explain_ocs_{k}.txt", "w").write(plan[0][0])
        t0 = time.perf_counter()
        cols, rows = presto(sql, catalog="ocs")
        el = time.perf_counter() - t0
        ok = same_result(to_table(hcols, hrows), to_table(cols, rows))
        pushed = expect in plan[0][0]
        out.append({"feature": k, "correct_vs_hive": ok, "expected_pushdown": expect, "pushdown_seen": pushed,
                    "rows": len(rows), "time_s": el})
        print(f"{k:18s} đúng={ok}  pushdown '{expect}' có trong plan={pushed}  {len(rows)} dòng  {el:.2f}s", flush=True)
    json.dump(out, open(f"{OUT}/features.json", "w"), indent=1)


def bench(reps, which="main"):
    queries, cats, stage = SETS[which]
    # Presto trả lời SELECT 1 trước khi node worker đăng ký xong -> chờ node active rồi mới đo
    for _ in range(60):
        try:
            if presto("SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'", catalog="system")[1][0][0] >= 1:
                break
        except RuntimeError:
            pass
        time.sleep(3)
    time.sleep(10)
    eng = Engine()
    eng.conn.set_net(1000)
    eng.conn.set_media(500)
    oracles = {k: pq.read_table(f"/app/results/oracle/{o}.parquet") for k, (o, _) in queries.items()}
    cfgs = [(k, cat) for k in queries for cat in cats]

    def once(k, cat):
        tx0, t0, wall = eng.conn.tx_bytes(), time.perf_counter(), time.time()
        cols, rows = presto(queries[k][1], catalog=cat)
        t = time.perf_counter() - t0
        return {"stage": stage, "label": None, "cfg": f"{k}:{cat}", "time_s": t,
                "wire_bytes": eng.conn.tx_bytes() - tx0, "payload_bytes": 0, "started": wall, "ended": time.time(),
                "storage": {"media_bytes": 0}, "correct": same_result(oracles[k], to_table(cols, rows))}

    for k, cat in cfgs:
        s = once(k, cat)
        print(f"warm-up {s['cfg']:14s} {s['time_s']:7.2f}s wire={s['wire_bytes'] / 1e6:9.2f}MB đúng={s['correct']}", flush=True)
    rng, samples = random.Random(51), []
    for rep in range(reps):
        order = cfgs[:]
        rng.shuffle(order)
        for i, (k, cat) in enumerate(order):
            s = once(k, cat)
            s.update(rep=rep, order=i)
            samples.append(s)
        print(f"vòng {rep + 1}/{reps} xong", flush=True)
        json.dump(samples, open(f"/app/results/reliability/{stage}.json", "w"), indent=1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["explain", "bench", "check", "features"])
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--set", choices=list(SETS), default="main")
    a = ap.parse_args()
    if a.cmd == "explain":
        explain()
    elif a.cmd == "check":
        e = Engine()
        for k, (o, sql) in QUERIES.items():
            cols, rows = presto(sql, catalog="ocs")
            print(k, "đúng =", same_result(pq.read_table(f"/app/results/oracle/{o}.parquet"), to_table(cols, rows)))
    elif a.cmd == "features":
        features()
    else:
        bench(a.reps, a.set)
