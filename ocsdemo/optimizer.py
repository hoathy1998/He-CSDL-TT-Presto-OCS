"""Local Optimizer của OCS connector (tương ứng ConnectorPlanOptimizer trong Presto):
Selectivity Analyzer + Operator Extractor + chọn điểm cắt pushdown.

Ước lượng theo đúng mô tả ở Mục 4 của bài báo:
  - Filter      : giả định giá trị phân phối CHUẨN giữa min/max của cột (mu=(min+max)/2, sigma=(max-min)/6),
                  độc lập giữa các cột -> tỉ lệ dòng thỏa điều kiện range.
  - Aggregation : số nhóm ước lượng từ NDV của các GROUP BY key (NDV thấp -> output nhỏ -> nên đẩy).
  - Top-N       : LIMIT cho biết trực tiếp số dòng output.
Quyết định: so sánh lượng dữ liệu giảm được và chi phí tính toán (storage CPU yếu hơn compute) bằng một
mô hình chi phí đơn giản, chọn tiền tố toán tử có thời gian ước lượng nhỏ nhất. Ngoài ra đọc
pushdown history (các lần chạy trước) để điều chỉnh quyết định.
"""
import datetime
import json
import math
import os
from dataclasses import dataclass, field

from .plan import Query, k_to_mode, mode_to_k

HISTORY = os.environ.get("OCS_HISTORY", "/app/results/pushdown_history.jsonl")

# Trọng số chi phí CPU (đơn vị / dòng) cho từng loại toán tử
DECODE_W = 0.5      # giải mã Parquet, mỗi giá trị
CPU_RATE = 1.0e8    # đơn vị / giây / lõi (hiệu chỉnh thô)


def _num(v):
    if isinstance(v, str):
        try:
            return datetime.date.fromisoformat(v[:10]).toordinal()
        except ValueError:
            return None
    return v


def range_selectivity(stats, lo, hi):
    mn, mx = _num(stats.get("min")), _num(stats.get("max"))
    if mn is None or mx is None or mx <= mn:
        return 1.0 / 3  # mặc định khi thiếu thống kê
    mu, sd = (mn + mx) / 2, (mx - mn) / 6
    cdf = lambda v: 0.5 * (1 + math.erf((v - mu) / (sd * math.sqrt(2))))
    a = cdf(_num(lo)) if lo is not None else 0.0
    b = cdf(_num(hi)) if hi is not None else 1.0
    return max(b - a, 1e-9)


def op_weight(op):
    if op.kind == "filter":
        return len(op.ranges)
    if op.kind == "proj":
        return op.complexity
    if op.kind == "aggr":
        return 1 + 0.5 * len(op.aggs)
    if op.kind == "topN":
        return 1
    if op.kind == "sort":
        return 2
    return 1


@dataclass
class Estimate:
    k: int
    mode: str
    rows: float          # số dòng truyền qua mạng
    bytes: float         # số byte truyền qua mạng
    selectivity: float   # output / input của toán tử thứ k
    transfer_s: float = 0
    storage_cpu_s: float = 0
    compute_cpu_s: float = 0
    allowed: bool = True
    why_not: str = ""

    @property
    def total_s(self):
        return self.transfer_s + self.storage_cpu_s + self.compute_cpu_s


@dataclass
class Decision:
    k: int
    mode: str
    agg_complete: bool
    chosen_by: str
    estimates: list = field(default_factory=list)
    notes: list = field(default_factory=list)


def agg_is_complete(q: Query, entry) -> bool:
    """Aggregation ở từng object cho kết quả hoàn chỉnh nếu một GROUP BY key là hằng số trong mỗi split
    (cột partition, hoặc min==max theo thống kê footer) -> nhóm không trải qua nhiều object."""
    agg = next((op for op in q.ops if op.kind == "aggr"), None)
    if agg is None:
        return True
    if len(entry["splits"]) == 1:
        return True
    cols = entry["columns"]
    return any(k == entry.get("partition_column") or cols.get(k, {}).get("constant_per_split") for k in agg.keys)


def estimate(q: Query, entry, cluster):
    cols = entry["columns"]
    nsplit = len(entry["splits"])
    rows_total = entry["rows"]
    net_Bps = max(cluster["net_mbit"], 1e-3) * 1e6 / 8
    s_rate = cluster["storage_threads"] * CPU_RATE
    c_rate = cluster["compute_threads"] * CPU_RATE
    complete = agg_is_complete(q, entry)

    # luồng dữ liệu qua từng toán tử: rows_in[i], rows_out[i], width_out[i]
    rows, width = rows_total, sum(cols[c]["width"] for c in q.columns)
    flow, seen_aggr = [], False
    for op in q.ops:
        r_in = rows
        if op.kind == "filter":
            sel = 1.0
            for c, lo, hi in op.ranges:
                sel *= range_selectivity(cols[c], lo, hi)
            rows, width = rows * sel, sum(cols[c]["width"] for c in op.output)
        elif op.kind == "proj":
            width = sum(w for _, _, w in op.exprs)
        elif op.kind == "aggr":
            ndv = 1.0
            for kcol in op.keys:
                st = cols.get(kcol, {})
                if not (kcol == entry.get("partition_column") or st.get("constant_per_split")):
                    ndv *= st.get("ndv", rows)
            groups_per_split = min(rows / nsplit, ndv)
            rows = groups_per_split * nsplit
            width = sum(cols.get(kc, {}).get("width", 8) for kc in op.keys) + 8 * len(op.aggs)
            seen_aggr = True
        elif op.kind == "topN":
            rows = min(rows, op.limit * nsplit)
        flow.append({"op": op, "rows_in": r_in, "rows_out": rows, "width": width})

    decode = rows_total * len(q.columns) * DECODE_W
    ests = []
    for k in range(len(q.ops) + 1):
        if k == 0:
            e = Estimate(0, "none", rows_total, entry["bytes"], 1.0)
            e.compute_cpu_s = (decode + sum(f["rows_in"] * op_weight(f["op"]) for f in flow)) / c_rate
        else:
            f = flow[k - 1]
            e = Estimate(k, k_to_mode(q, k), f["rows_out"], f["rows_out"] * f["width"],
                         f["rows_out"] / max(f["rows_in"], 1))
            e.storage_cpu_s = (decode + sum(x["rows_in"] * op_weight(x["op"]) for x in flow[:k])) / s_rate
            e.compute_cpu_s = sum(x["rows_in"] * op_weight(x["op"]) for x in flow[k:]) / c_rate
            pushed = [x["op"].kind for x in flow[:k]]
            if "topN" in pushed and "aggr" in pushed and not complete:
                e.allowed, e.why_not = False, "TopN trên aggregation từng phần không chính xác"
        e.transfer_s = e.bytes / net_Bps
        ests.append(e)
    return ests, complete


def load_history(q: Query, net_mbit):
    best = {}
    if not os.path.exists(HISTORY):
        return best
    with open(HISTORY) as fh:
        for line in fh.readlines()[-500:]:  # cửa sổ trượt các lần chạy gần nhất
            h = json.loads(line)
            if h["query"] == q.name and h["table"] == q.table and abs(h["net_mbit"] - net_mbit) < 1:
                best.setdefault(h["k"], []).append(h["total_s"])
    return {k: sorted(v)[len(v) // 2] for k, v in best.items()}


def choose(q: Query, entry, cluster, mode="auto", use_history=True) -> Decision:
    ests, complete = estimate(q, entry, cluster)
    if mode not in ("auto",):
        k = mode_to_k(q, mode)
        if not ests[k].allowed:
            raise ValueError(f"không thể đẩy '{mode}': {ests[k].why_not}")
        return Decision(k, ests[k].mode, complete, "người dùng chỉ định", ests)
    allowed = [e for e in ests if e.allowed]
    best = min(allowed, key=lambda e: e.total_s)
    d = Decision(best.k, best.mode, complete, "mô hình chi phí (Selectivity Analyzer)", ests)
    for e in ests:
        if 0 < e.k and e.selectivity > 0.9:
            d.notes.append(f"{e.mode}: toán tử cuối không giảm dữ liệu (selectivity≈{e.selectivity:.2f})"
                           f" -> chỉ có lợi nếu toán tử phía sau giảm mạnh")
    if use_history:
        hist = load_history(q, cluster["net_mbit"])
        if len(hist) >= 2:
            k_meas = min(hist, key=hist.get)
            if k_meas != best.k and hist.get(best.k, math.inf) > 1.1 * hist[k_meas]:
                d.k, d.mode = k_meas, ests[k_meas].mode
                d.chosen_by = "pushdown history (đo thực tế các lần trước)"
            d.notes.append("history (trung vị, s): " + ", ".join(f"{ests[k].mode}={v:.2f}" for k, v in sorted(hist.items())))
    return d
