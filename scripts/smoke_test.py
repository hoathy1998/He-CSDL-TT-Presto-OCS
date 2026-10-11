"""Kiểm tra nhanh: mọi truy vấn x mọi chế độ pushdown cho cùng kết quả với 'none'."""
import argparse
import sys

from ocsdemo.engine import Engine, same_result
from ocsdemo.plan import k_to_mode
from ocsdemo.queries import QUERIES

ap = argparse.ArgumentParser()
ap.add_argument("--splits", type=int, default=2)
a = ap.parse_args()

eng = Engine()
ok = True
for q in QUERIES.values():
    ref = None
    modes = [k_to_mode(q, k) for k in range(len(q.ops) + 1)] + ["auto"]
    for m in modes:
        try:
            r = eng.run(q, m, max_splits=a.splits, record=False)
        except ValueError as e:
            print(f"  {q.name:10s} {m:22s} BỎ QUA: {e}")
            continue
        ref = ref if ref is not None else r.result
        good = same_result(ref, r.result)
        ok &= good
        print(f"  {q.name:10s} {r.mode:22s} {r.total_s:7.2f}s  wire={r.wire_bytes / 1e6:9.2f}MB "
              f"rows={r.result.num_rows:5d} {'OK' if good else 'SAI KẾT QUẢ'}")
    if q.name == "tpch_proj":
        csv = eng.run(q, "filter", kind_filter_csv=True, max_splits=a.splits, record=False)
        good = same_result(ref, csv.result)
        ok &= good
        print(f"  {q.name:10s} {csv.mode:22s} {csv.total_s:7.2f}s  wire={csv.wire_bytes / 1e6:9.2f}MB "
              f"{'OK' if good else 'SAI KẾT QUẢ'}")
sys.exit(0 if ok else 1)
