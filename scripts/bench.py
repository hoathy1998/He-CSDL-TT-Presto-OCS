"""Chạy toàn bộ thí nghiệm, ghi results/bench.json.

  E1 (Q1) Laghos: pushdown tăng dần -> thời gian + data movement
  E2 (Q2) Deep Water, TPC-H Q1: pushdown Project có thể làm CHẬM; thêm Aggregation thì nhanh lại
  E3 (Q3) Deep Water với 4 kiểu nén x {filter, all}
  E4 (Q4) Phân rã thời gian một truy vấn trên 1 file Laghos (overhead của connector)
  E5      Quét băng thông mạng (100 Mbps / 1 Gbps / 10 Gbps)
  E6      Selectivity Analyzer: ước lượng vs thực tế, quyết định 'auto' vs chế độ tốt nhất đo được
"""
import argparse
import json
import os
import statistics
import time

from ocsdemo.engine import Engine, same_result
from ocsdemo.optimizer import estimate
from ocsdemo.queries import DEEPWATER, LAGHOS, QUERIES, TPCH_PROJ, TPCH_Q1, with_table

OUT = "/app/results/bench.json"


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


class Bench:
    def __init__(self, reps, max_splits):
        self.eng = Engine()
        self.reps = reps
        self.max_splits = max_splits
        self.rows = []
        self.refs = {}

    def run(self, exp, q, mode, csv=False, reps=None, max_splits=None, extra=None):
        ms = max_splits or self.max_splits
        reps = reps or self.reps
        self.eng.run(q, mode, kind_filter_csv=csv, max_splits=ms, record=False)  # warm-up
        runs = [self.eng.run(q, mode, kind_filter_csv=csv, max_splits=ms, use_history=False) for _ in range(reps)]
        key = (q.name, q.table, ms)
        if key not in self.refs:  # kết quả chuẩn = không pushdown
            self.refs[key] = (runs[0].result if mode == "none" else
                              self.eng.run(q, "none", max_splits=ms, record=False).result)
        correct = same_result(self.refs[key], runs[0].result)
        med = sorted(runs, key=lambda r: r.total_s)[len(runs) // 2]
        row = {
            "exp": exp, "query": q.name, "table": q.table, "mode": med.mode, "k": med.k,
            "net_mbit": med.net_mbit, "splits": ms or len(self.eng.catalog[q.table]["splits"]),
            "time_s": statistics.median(r.total_s for r in runs),
            "time_all": [r.total_s for r in runs],
            "wire_bytes": statistics.median(r.wire_bytes for r in runs),
            "payload_bytes": med.payload_bytes,
            "timings": med.timings, "storage": med.storage, "substrait_bytes": med.substrait_bytes,
            "result_rows": med.result.num_rows, "correct": correct,
            "storage_sql": med.storage_sql, "residual_sql": med.residual_sql,
        }
        row.update(extra or {})
        self.rows.append(row)
        log(f"{exp} {q.name:10s} {q.table:17s} {row['mode']:24s} {row['time_s']:7.2f}s "
            f"wire={row['wire_bytes'] / 1e6:10.3f}MB correct={correct}")
        self.save()
        return row

    def save(self):
        os.makedirs(os.path.dirname(OUT), exist_ok=True)
        cl = self.eng.cluster()
        with open(OUT, "w") as fh:
            json.dump({"cluster": cl, "reps": self.reps, "rows": self.rows}, fh, indent=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--max-splits", type=int, default=None)
    ap.add_argument("--only", default="E1,E2,E3,E4,E5,E6")
    a = ap.parse_args()
    only = a.only.split(",")
    b = Bench(a.reps, a.max_splits)
    b.eng.conn.set_net(1000)

    if "E1" in only:
        for m in ["none", "filter", "filter+aggr", "filter+aggr+topN"]:
            b.run("E1", LAGHOS, m)
        b.run("E1", LAGHOS, "filter", csv=True)          # S3 Select: kết quả CSV theo dòng

    if "E2" in only:
        for q in (DEEPWATER, TPCH_Q1):
            for m in ["none", "filter", "filter+proj", "filter+proj+aggr"]:
                b.run("E2", q, m)

    if "E3" in only:
        for codec in ["deepwater", "deepwater_snappy", "deepwater_gzip", "deepwater_zstd"]:
            q = with_table(DEEPWATER, codec)
            for m in ["filter", "all"]:
                b.run("E3", q, m, extra={"codec": codec.replace("deepwater_", "") if "_" in codec else "raw"})

    if "E4" in only:
        b.run("E4", LAGHOS, "all", reps=7, max_splits=1)

    if "E5" in only:
        for mbit in [100, 1000, 10000]:
            b.eng.conn.set_net(mbit)
            for q, modes in ((LAGHOS, ["none", "filter", "all"]), (TPCH_Q1, ["none", "filter", "filter+proj", "all"])):
                for m in modes:
                    b.run("E5", q, m, reps=max(1, a.reps - 1))
        b.eng.conn.set_net(1000)

    if "E6" in only:
        cl = b.eng.cluster()
        for q in (LAGHOS, DEEPWATER, TPCH_Q1, TPCH_PROJ):
            entry = b.eng.catalog[q.table]
            ests, complete = estimate(q, entry, cl)
            measured = {}
            for e in ests:
                if not e.allowed:
                    continue
                r = b.run("E6", q, e.mode, reps=max(1, a.reps - 1),
                          extra={"est_rows": e.rows, "est_bytes": e.bytes, "est_s": e.total_s,
                                 "est_selectivity": e.selectivity})
                measured[e.k] = r
            auto = b.eng.run(q, "auto", max_splits=a.max_splits, use_history=False, record=False)
            best_k = min(measured, key=lambda k: measured[k]["time_s"])
            b.rows.append({"exp": "E6-decision", "query": q.name, "auto_mode": auto.mode, "auto_k": auto.k,
                           "best_mode": measured[best_k]["mode"], "best_k": best_k,
                           "auto_time_s": measured[auto.k]["time_s"], "best_time_s": measured[best_k]["time_s"],
                           "notes": auto.notes})
            log(f"E6 {q.name}: auto={auto.mode}  best={measured[best_k]['mode']}")
            b.save()
    log("xong.")


if __name__ == "__main__":
    main()
