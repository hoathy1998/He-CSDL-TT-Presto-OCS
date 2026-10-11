"""Phân tích thống kê kết quả results/reliability/*.json -> summary.md, summary.json, các hình fig_r*.png

Phương pháp:
  - Trung vị + khoảng tin cậy 95% bằng bootstrap phân vị (B = 10.000), không giả định phân phối chuẩn
  - Tỉ số tăng tốc = trung vị(B) / trung vị(A), CI 95% bằng bootstrap lấy mẫu lại độc lập hai nhóm
  - Kiểm định Mann–Whitney U một phía (exact), hiệu chỉnh Holm–Bonferroni cho họ các khẳng định
  - Kích thước hiệu ứng Cliff's delta (|d| < 0.147 không đáng kể, < 0.33 nhỏ, < 0.474 vừa, còn lại lớn)
  - Hệ số biến thiên CV; đánh dấu mẫu trùng thời điểm máy chạy pin hoặc ngủ (log nguồn của host)
"""
import csv
import glob
import json
import os
import sys

import numpy as np
from scipy.stats import mannwhitneyu

sys.path.insert(0, "/app/scripts")
import plots as PL  # noqa: E402  (dùng chung bảng màu / style)
import matplotlib.pyplot as plt  # noqa: E402

D = "/app/results/reliability"
FIG = "/app/results/figures"
RNG = np.random.default_rng(2026)
B = 10000


# ------------------------------------------------------------------ dữ liệu
def load_samples():
    out = []
    for f in sorted(glob.glob(f"{D}/*.json")):
        name = os.path.basename(f)[:-5]
        if name in ("calib", "oracle", "summary", "standby"):
            continue
        data = json.load(open(f))
        # chỉ nhận file là danh sách số đo (mỗi mẫu có mốc thời gian để đối chiếu nguồn điện/nhiệt)
        if isinstance(data, list) and data and isinstance(data[0], dict) and "started" in data[0]:
            out += data
    return out


def load_power():
    """power_log.csv (epoch, on_ac, charge) do host ghi mỗi 15s; standby.json = sự kiện Modern Standby."""
    log, stand = [], []
    p = f"{D}/power_log.csv"
    if os.path.exists(p):
        with open(p) as fh:
            for row in csv.DictReader(fh):
                log.append((float(row["epoch"]), int(row["on_ac"]), int(row["charge"])))
    p = f"{D}/standby.json"
    if os.path.exists(p):
        stand = json.load(open(p))
    return log, stand


def load_cpu_perf():
    """cpu_perf_log.csv (epoch, % Processor Performance) do thermal_monitor.ps1 ghi mỗi 15s."""
    p, out = f"{D}/cpu_perf_log.csv", []
    if os.path.exists(p):
        with open(p) as fh:
            for row in csv.DictReader(fh):
                out.append((float(row["epoch"]), float(row["perf_pct"]), float(row["utility_pct"])))
    return out


CPU_PERF = []
CPU_PERF_MEDIAN = None


def flag(sample, power, standby):
    a, b = sample["started"], sample["ended"]
    reasons = []
    # Hạ xung do nhiệt = hiệu năng thấp KHI CPU ĐANG BẬN (util >= 20%); khi rảnh Windows tự hạ xung nên bỏ qua.
    # Cửa sổ ±7,5 s = nửa chu kỳ lấy mẫu 15 s.
    if CPU_PERF_MEDIAN and any(a - 7.5 <= t <= b + 7.5 and v < 0.75 * CPU_PERF_MEDIAN and u >= 20
                               for t, v, u in CPU_PERF):
        reasons.append("CPU hạ xung (nghi do nhiệt)")
    if any(a - 20 <= t <= b + 20 and not ac for t, ac, _ in power):
        reasons.append("chạy pin")
    ts = [t for t, _, _ in power if a - 60 <= t <= b + 60]
    if power and any(y - x > 60 for x, y in zip(ts, ts[1:])):
        reasons.append("log nguồn bị gián đoạn (nghi máy ngủ)")
    if any(e["start"] <= b and e["end"] >= a for e in standby):
        reasons.append("Modern Standby")
    return reasons


# ------------------------------------------------------------------ thống kê
def boot_median_ci(x):
    x = np.asarray(x)
    meds = np.median(RNG.choice(x, (B, len(x)), replace=True), axis=1)
    return float(np.median(x)), float(np.percentile(meds, 2.5)), float(np.percentile(meds, 97.5))


def boot_ratio_ci(slow, fast):
    """tỉ số tăng tốc = median(slow)/median(fast)."""
    s, f = np.asarray(slow), np.asarray(fast)
    rs = np.median(RNG.choice(s, (B, len(s))), axis=1) / np.median(RNG.choice(f, (B, len(f))), axis=1)
    return float(np.median(s) / np.median(f)), float(np.percentile(rs, 2.5)), float(np.percentile(rs, 97.5))


def cliffs_delta(a, b):
    a, b = np.asarray(a)[:, None], np.asarray(b)[None, :]
    return float(((a > b).sum() - (a < b).sum()) / (a.size * b.size))


def magnitude(d):
    d = abs(d)
    return "không đáng kể" if d < 0.147 else "nhỏ" if d < 0.33 else "vừa" if d < 0.474 else "lớn"


def holm(pvals):
    order = np.argsort(pvals)
    m, adj, running = len(pvals), [0.0] * len(pvals), 0.0
    for rank, i in enumerate(order):
        running = max(running, min(1.0, (m - rank) * pvals[i]))
        adj[i] = running
    return adj


# ------------------------------------------------------------------ khẳng định cần kiểm định
# (nhóm, nhãn, cấu hình CHẬM hơn theo giả thuyết, cấu hình NHANH hơn theo giả thuyết, mô tả)
CLAIMS = [
    ("e7", None, "L:none", "L:filter", "Laghos: filter nhanh hơn không pushdown"),
    ("e7", None, "L:filter", "L:filter+aggr+topN", "Laghos: đẩy toàn bộ nhanh hơn filter"),
    ("e7", None, "L:filter+aggr", "L:filter+aggr+topN", "Laghos @1Gbps: thêm Top-N nhanh hơn filter+aggr"),
    ("e7", None, "L:filter(csv)", "L:filter", "Laghos: kết quả CSV chậm hơn Arrow"),
    ("e7", None, "DW:none", "DW:filter", "Deep Water: filter nhanh hơn không pushdown"),
    ("e7", None, "DW:filter+proj", "DW:filter", "Deep Water: thêm projection làm chậm"),
    ("e7", None, "DW:filter", "DW:filter+proj+aggr", "Deep Water: đẩy toàn bộ nhanh hơn filter"),
    ("e7", None, "TQ:filter", "TQ:none", "TPC-H: filter CHẬM hơn không pushdown"),
    ("e7", None, "TQ:filter+proj", "TQ:filter", "TPC-H: thêm projection làm chậm"),
    ("e7", None, "TQ:filter", "TQ:filter+proj+aggr", "TPC-H: đẩy toàn bộ nhanh hơn filter"),
    ("e7", None, "DW:filter", "DW:filter+proj+aggr", "Q3 raw: đẩy toàn bộ nhanh hơn filter"),
    ("e7", None, "DW-snappy:filter", "DW-snappy:all", "Q3 snappy: đẩy toàn bộ nhanh hơn filter"),
    ("e7", None, "DW-gzip:filter", "DW-gzip:all", "Q3 gzip: đẩy toàn bộ nhanh hơn filter"),
    ("e7", None, "DW-zstd:filter", "DW-zstd:all", "Q3 zstd: đẩy toàn bộ nhanh hơn filter"),
    ("e7", None, "DW:filter+proj+aggr", "DW-zstd:filter", "Q3 (bài): zstd chỉ filter nhanh hơn raw đẩy toàn bộ"),
    ("media", "150", "DW-none:all", "DW-zstd:filter", "Đĩa 150MB/s: zstd chỉ filter nhanh hơn raw đẩy toàn bộ"),
    ("media", "500", "DW-none:all", "DW-zstd:filter", "Đĩa 500MB/s: zstd chỉ filter nhanh hơn raw đẩy toàn bộ"),
    ("media", "2000", "DW-none:all", "DW-zstd:filter", "Đĩa 2000MB/s: zstd chỉ filter nhanh hơn raw đẩy toàn bộ"),
    ("bandwidth", "100", "L:filter+aggr", "L:filter+aggr+topN", "Laghos @100Mbps: thêm Top-N nhanh hơn filter+aggr"),
    ("bandwidth", "10000", "L:filter+aggr+topN", "L:filter", "Laghos @10Gbps: đẩy toàn bộ CHẬM hơn filter"),
    ("cpus", "1", "DW:filter+proj", "DW:filter", "Storage 1 CPU: projection Deep Water làm chậm"),
    ("cpus", "1", "DWH:filter+proj", "DW:filter", "Storage 1 CPU: projection NẶNG làm chậm"),
    ("cpus", "2", "DWH:filter+proj", "DW:filter", "Storage 2 CPU: projection NẶNG làm chậm"),
    ("cpus", "4", "DWH:filter+proj", "DW:filter", "Storage 4 CPU: projection NẶNG làm chậm"),
    ("ablation", None, "TQ-dec:filter", "TQ-dbl:filter", "Ablation: decimal->double làm filter nhanh hơn"),
    ("ablation", None, "TQ-dec:filter", "TQ-ipc:filter", "Ablation: nén Arrow IPC làm filter nhanh hơn"),
    ("presto", None, "L:ocsnopd", "L:ocsfilter", "Presto thật – Laghos: filter pushdown nhanh hơn không pushdown"),
    ("presto", None, "L:ocsfilter", "L:ocs", "Presto thật – Laghos: đẩy toàn bộ nhanh hơn filter"),
    ("presto", None, "DW:ocsnopd", "DW:ocsfilter", "Presto thật – Deep Water: filter pushdown nhanh hơn không pushdown"),
    ("presto", None, "DW:ocsfilter", "DW:ocs", "Presto thật – Deep Water: đẩy toàn bộ nhanh hơn filter"),
    ("presto", None, "TQ:ocsnopd", "TQ:ocsfilter", "Presto thật – TPC-H (double): filter pushdown nhanh hơn không pushdown"),
    ("presto", None, "TQ:ocsfilter", "TQ:ocs", "Presto thật – TPC-H (double): gom nhóm hai pha (R1) nhanh hơn filter"),
] + [("presto_q3", None, f"DW_{c}:ocsfilter", f"DW_{c}:ocs", f"Presto thật – Q3 Deep Water {c}: đẩy toàn bộ nhanh hơn filter")
     for c in ("none", "snappy", "gzip", "zstd")] + [
    ("scale256", None, "L64:filter", "L64:filter+aggr+topN", "Laghos 64 file (vượt page cache): đẩy toàn bộ nhanh hơn filter"),
    ("scale256", None, "L128:filter", "L128:filter+aggr+topN", "Laghos 128 file (vượt page cache): đẩy toàn bộ nhanh hơn filter"),
] + [("scale", None, f"L{n}:filter", f"L{n}:filter+aggr+topN", f"Laghos {n} file (trong page cache): đẩy toàn bộ nhanh hơn filter")
     for n in (4, 8, 16, 32)] + [
    ("scale", None, f"L{n}:none", f"L{n}:filter", f"Laghos {n} file (trong page cache): filter nhanh hơn không pushdown")
    for n in (4, 8, 16, 32)] + [
    ("presto_l256", None, "L256:ocsfilter", "L256:ocs", "Presto thật – Laghos 256 file: đẩy toàn bộ nhanh hơn filter"),
    ("presto_q4", None, "L1:ocsnopd", "L1:ocs", "Presto thật – Q4 Laghos 1 file: đẩy toàn bộ nhanh hơn không pushdown"),
    ("minio", None, "L:minio-select", "L:ocs-all", "MinIO Select thật vs OCS đẩy toàn bộ (Laghos)"),
    ("minio", None, "DW:minio-select", "DW:ocs-all", "MinIO Select thật vs OCS đẩy toàn bộ (Deep Water)"),
    ("minio", None, "TQ:minio-select", "TQ:ocs-all", "MinIO Select thật vs OCS đẩy toàn bộ (TPC-H, double)"),
    ("join", None, "JOIN:none", "JOIN:filter", "Join: filter pushdown nhanh hơn không pushdown"),
    ("join", None, "JOIN:filter", "JOIN:all", "Join: đẩy thêm aggregation từng phần nhanh hơn filter"),
] + [("nodes", str(n), "L:filter", "L:filter+aggr+topN", f"{n} storage node: Laghos đẩy toàn bộ nhanh hơn filter")
      for n in (1, 2, 3)] + [
    ("nodes", str(n), "TQ:filter", "TQ:filter+proj+aggr", f"{n} storage node: TPC-H đẩy toàn bộ nhanh hơn filter")
    for n in (1, 2, 3)] + [
    ("concurrency", f"filter:k={k}", "filter:k=" + str(k), "all:k=" + str(k),
     f"{k} truy vấn đồng thời (3 node): đẩy toàn bộ nhanh hơn filter") for k in (1, 4, 8)] + [
    ("engine", str(ns), "DW:filter+proj", "DW:filter",
      f"Engine chậm {ns} ns/đv: projection Deep Water làm chậm") for ns in (0, 1, 2, 5, 10, 20, 50, 100)] + [
    ("engine", str(ns), "TQ:filter+proj", "TQ:filter",
     f"Engine chậm {ns} ns/đv: projection TPC-H làm chậm") for ns in (0, 20)]

# so với bài báo: (mô tả, nhóm, nhãn, cấu hình mẫu số chậm, cấu hình nhanh, giá trị bài báo)
VS_PAPER = [
    ("Laghos: filter / không pushdown", "e7", None, "L:none", "L:filter", 2710 / 1015),
    ("Laghos: đẩy toàn bộ / filter", "e7", None, "L:filter", "L:filter+aggr+topN", 2.25),
    ("Deep Water: filter / không pushdown", "e7", None, "DW:none", "DW:filter", 2.33),
    ("Deep Water: projection (tỉ số <1 = chậm)", "e7", None, "DW:filter", "DW:filter+proj", 1 / 1.07),
    ("Deep Water: đẩy toàn bộ / filter", "e7", None, "DW:filter", "DW:filter+proj+aggr", 1.32),
    ("TPC-H: filter / không pushdown", "e7", None, "TQ:none", "TQ:filter", 1.22),
    ("TPC-H: projection (tỉ số <1 = chậm)", "e7", None, "TQ:filter", "TQ:filter+proj", 1 / 1.55),
    ("TPC-H: đẩy toàn bộ / filter", "e7", None, "TQ:filter", "TQ:filter+proj+aggr", 4.07),
    ("Q3 raw: đẩy toàn bộ / filter", "e7", None, "DW:filter", "DW:filter+proj+aggr", 1.22),
    ("Q3 snappy: đẩy toàn bộ / filter", "e7", None, "DW-snappy:filter", "DW-snappy:all", 1.37),
    ("Q3 gzip: đẩy toàn bộ / filter", "e7", None, "DW-gzip:filter", "DW-gzip:all", 1.39),
    ("Q3 zstd: đẩy toàn bộ / filter", "e7", None, "DW-zstd:filter", "DW-zstd:all", 1.36),
]


def main():
    samples = load_samples()
    power, standby = load_power()
    global CPU_PERF, CPU_PERF_MEDIAN
    CPU_PERF = load_cpu_perf()
    if CPU_PERF:
        CPU_PERF_MEDIAN = float(np.median([v for _, v, u in CPU_PERF if u >= 20] or [v for _, v, _ in CPU_PERF]))
    for s in samples:
        s["flags"] = flag(s, power, standby)
    good = [s for s in samples if not s["flags"]]
    groups = {}
    for s in good:
        groups.setdefault((s["stage"], None if s["label"] is None else str(s["label"]), s["cfg"]), []).append(s)
    def T(st, lb, c):
        # concurrency: nhãn mỗi nhóm là "<mode>:k=<k>"; cho phép tra theo tên cấu hình ở cả hai phía
        if st == "concurrency":
            return [s["time_s"] for s in groups.get((st, c, c), [])]
        return [s["time_s"] for s in groups.get((st, lb, c), [])]

    L = []  # các dòng markdown
    L.append("# Tổng hợp đánh giá độ tin cậy\n")
    L.append(f"Tổng số lần đo: {len(samples)}; bị đánh dấu (pin/ngủ) và loại: {len(samples) - len(good)}; "
             f"sai kết quả so với oracle: {sum(not s['correct'] for s in samples)}.\n")

    # hiệu chuẩn
    if os.path.exists(f"{D}/calib.json"):
        c = json.load(open(f"{D}/calib.json"))
        L.append("## Hiệu chuẩn phần cứng giả lập\n\n| Thành phần | Khai báo | Đo được (hồi quy) | Sai lệch |\n|---|---|---|---|")
        for n in c["network"]:
            L.append(f"| Mạng (tc) | {n['declared_mbit']:.0f} Mbps | {n['measured_mbit']:.0f} Mbps | "
                     f"{n['measured_mbit'] / n['declared_mbit'] - 1:+.1%} |")
        for m in c["media"]:
            L.append(f"| Đọc thiết bị | {m['declared_mbps']:.0f} MB/s | {m['measured_mbps']:.0f} MB/s | "
                     f"{m['measured_mbps'] / m['declared_mbps'] - 1:+.1%} |")
        L.append("")
    if os.path.exists(f"{D}/oracle.json"):
        o = json.load(open(f"{D}/oracle.json"))
        L.append(f"## Oracle DuckDB độc lập\n\n{sum(r['correct'] for r in o)}/{len(o)} tổ hợp (truy vấn × chế độ) "
                 f"cho kết quả trùng với DuckDB.\n")

    # bảng mô tả
    L.append("## Thống kê mô tả (trung vị, CI 95% bootstrap, CV)\n\n| Nhóm | Nhãn | Cấu hình | n | Trung vị (s) | CI 95% | CV | Dữ liệu truyền (MB) | Đúng |\n|---|---|---|---|---|---|---|---|---|")
    desc = []
    for (st, lb, c), ss in sorted(groups.items(), key=lambda kv: (kv[0][0], str(kv[0][1]), kv[0][2])):
        t = [s["time_s"] for s in ss]
        med, lo, hi = boot_median_ci(t)
        cv = float(np.std(t, ddof=1) / np.mean(t)) if len(t) > 1 else 0.0
        wire = float(np.median([s["wire_bytes"] for s in ss])) / 1e6
        ok = all(s["correct"] for s in ss)
        desc.append({"stage": st, "label": lb, "cfg": c, "n": len(t), "median": med, "ci": [lo, hi], "cv": cv,
                     "wire_mb": wire, "correct": ok, "media_mb": float(np.median([s["storage"]["media_bytes"] for s in ss])) / 1e6})
        L.append(f"| {st} | {lb or ''} | {c} | {len(t)} | {med:.3f} | [{lo:.3f}, {hi:.3f}] | {cv:.1%} | {wire:,.3f} | {'✓' if ok else '✗'} |")
    cvs = [d["cv"] for d in desc if d["n"] > 1]
    L.append(f"\nCV trung vị = {np.median(cvs):.2%}; CV lớn nhất = {max(cvs):.2%}; "
             f"{sum(c < 0.02 for c in cvs)}/{len(cvs)} cấu hình có CV < 2%.\n")

    # so với bài báo
    L.append("## Tỉ số tăng tốc so với bài báo (CI 95% bootstrap)\n\n| Phép so sánh | Bài báo | Tái hiện | CI 95% | Bài báo nằm trong CI? | Cùng chiều? |\n|---|---|---|---|---|---|")
    vs = []
    for name, st, lb, slow, fast, paper in VS_PAPER:
        a, b = T(st, lb, slow), T(st, lb, fast)
        if not a or not b:
            continue
        r, lo, hi = boot_ratio_ci(a, b)
        same = (r > 1) == (paper > 1)
        vs.append({"name": name, "paper": paper, "ratio": r, "ci": [lo, hi], "same_direction": same})
        L.append(f"| {name} | {paper:.2f}× | {r:.2f}× | [{lo:.2f}, {hi:.2f}] | {'có' if lo <= paper <= hi else 'không'} | {'có' if same else 'KHÔNG'} |")
    L.append("")

    # kiểm định
    rows = []
    for st, lb, slow, fast, text in CLAIMS:
        a, b = T(st, lb, slow), T(st, lb, fast)
        if len(a) < 2 or len(b) < 2:
            continue
        p = mannwhitneyu(a, b, alternative="greater", method="exact").pvalue
        d = cliffs_delta(a, b)
        r, lo, hi = boot_ratio_ci(a, b)
        rows.append({"claim": text, "stage": st, "label": lb, "slow": slow, "fast": fast, "n": (len(a), len(b)),
                     "p": float(p), "cliffs_delta": d, "magnitude": magnitude(d), "ratio": r, "ci": [lo, hi]})
    # Holm–Bonferroni áp dụng theo HỌ giả thuyết (mỗi thí nghiệm = một họ), không gộp mọi thí nghiệm:
    # với n=5 mỗi nhóm, p nhỏ nhất của Mann–Whitney một phía là 1/252, gộp 26 giả thuyết sẽ làm mất
    # hoàn toàn khả năng phát hiện dù hai nhóm tách biệt hoàn toàn.
    for fam in {r["stage"] for r in rows}:
        idx = [i for i, r in enumerate(rows) if r["stage"] == fam]
        for i, pa in zip(idx, holm([rows[i]["p"] for i in idx])):
            rows[i]["p_holm"] = pa
            rows[i]["family_size"] = len(idx)
            rows[i]["supported"] = pa < 0.05
    L.append("## Kiểm định Mann–Whitney U một phía (exact; hiệu chỉnh Holm theo từng thí nghiệm), Cliff's δ\n\n"
             "| Khẳng định | n | Tỉ số (CI 95%) | p (Holm) | Cliff's δ | Kết luận |\n|---|---|---|---|---|---|")
    for r in rows:
        L.append(f"| {r['claim']} | {r['n'][0]}/{r['n'][1]} | {r['ratio']:.3f} [{r['ci'][0]:.3f}, {r['ci'][1]:.3f}] | "
                 f"{r['p_holm']:.4f} | {r['cliffs_delta']:+.2f} ({r['magnitude']}) | "
                 f"{'ĐƯỢC ỦNG HỘ' if r['supported'] else 'không đủ bằng chứng'} |")
    L.append("")

    # quy mô: tỉ số tăng tốc có ổn định khi dữ liệu tăng không
    scale_rows = []
    for n in (4, 8, 16, 32, 64, 128):
        st = "scale" if n <= 32 else "scale256"   # khối trong page cache / vượt page cache
        a, b, c = T(st, None, f"L{n}:none"), T(st, None, f"L{n}:filter"), T(st, None, f"L{n}:filter+aggr+topN")
        if b and c:
            r1 = boot_ratio_ci(a, b) if a else (float("nan"),) * 3
            scale_rows.append({"files": n, "filter_vs_none": r1, "all_vs_filter": boot_ratio_ci(b, c),
                               "block": "trong page cache" if n <= 32 else "vượt page cache"})
    if scale_rows:
        L.append("## Quy mô dữ liệu (Laghos): tỉ số tăng tốc theo số file\n\n| Số file | Khối đo | filter / không pushdown (CI 95%) | đẩy toàn bộ / filter (CI 95%) |\n|---|---|---|---|")
        for s in scale_rows:
            f, g = s["filter_vs_none"], s["all_vs_filter"]
            fs = "–" if f[0] != f[0] else f"{f[0]:.2f} [{f[1]:.2f}, {f[2]:.2f}]"
            L.append(f"| {s['files']} | {s['block']} | {fs} | {g[0]:.2f} [{g[1]:.2f}, {g[2]:.2f}] |")
        L.append("")

    # nhiều storage node + truy vấn đồng thời (G2.2)
    node_rows = []
    for n in (1, 2, 3):
        a, b, c = T("nodes", str(n), "L:none"), T("nodes", str(n), "L:filter"), T("nodes", str(n), "L:filter+aggr+topN")
        if a and b and c:
            node_rows.append({"nodes": n, "none": boot_median_ci(a), "filter": boot_median_ci(b),
                              "all": boot_median_ci(c), "all_vs_filter": boot_ratio_ci(b, c)})
    if node_rows:
        L.append("## Nhiều storage node (Laghos 16 file, mỗi node 2 CPU + 1 Gbps + 500 MB/s)\n\n"
                 "| Số node | Không pushdown (s) | Filter (s) | Đẩy toàn bộ (s) | Đẩy toàn bộ / filter (CI 95%) |\n|---|---|---|---|---|")
        for r in node_rows:
            g = r["all_vs_filter"]
            L.append(f"| {r['nodes']} | {r['none'][0]:.2f} | {r['filter'][0]:.2f} | {r['all'][0]:.2f} | "
                     f"{g[0]:.2f} [{g[1]:.2f}, {g[2]:.2f}] |")
        L.append("")
    conc = {}
    for (st, lb, c), ss in groups.items():
        if st == "concurrency":
            conc[c] = ss
    conc_rows = []
    if conc:
        L.append("## Truy vấn đồng thời trên cụm 3 node (Laghos 16 file)\n\n"
                 "| Chế độ | k truy vấn | Độ trễ trung vị mỗi truy vấn (s, CI 95%) | Thông lượng trung vị (truy vấn/s) |\n|---|---|---|---|")
        for m in ("filter", "all"):
            for k in (1, 4, 8):
                ss = conc.get(f"{m}:k={k}")
                if not ss:
                    continue
                lat = boot_median_ci([s["time_s"] for s in ss])
                per_rep = {s["rep"]: s["throughput_qps"] for s in ss}   # mỗi vòng có một giá trị thông lượng
                thr = float(np.median(list(per_rep.values())))
                conc_rows.append({"mode": m, "k": k, "latency": lat, "throughput": thr})
                L.append(f"| {m} | {k} | {lat[0]:.2f} [{lat[1]:.2f}, {lat[2]:.2f}] | {thr:.2f} |")
        L.append("")

    # thống kê theo nhóm phụ (scale, bandwidth, media, cpus) cho phần viết
    extra = {}
    for st in ("scale", "bandwidth", "media", "cpus", "ablation"):
        extra[st] = [d for d in desc if d["stage"] == st]

    with open(f"{D}/summary.md", "w") as fh:
        fh.write("\n".join(L))
    json.dump({"desc": desc, "vs_paper": vs, "claims": rows, "scale": scale_rows, "nodes": node_rows,
               "concurrency": conc_rows, "flagged": [
        {k: s[k] for k in ("stage", "label", "cfg", "rep", "started", "flags")} for s in samples if s["flags"]]},
        open(f"{D}/summary.json", "w"), indent=1)

    figures(vs, groups, T)
    regen_main_figures(groups)
    print("\n".join(L))


def regen_main_figures(groups):
    """Vẽ lại các hình chính (Q1, Q2, Q3, Q4, E5) từ dữ liệu n=10 / n=5 thay cho lượt đo n=3 cũ,
    để số liệu trong hình và trong văn bản là cùng một bộ."""
    def med(st, lb, c, key="time_s"):
        ss = groups.get((st, lb, c), [])
        return float(np.median([s[key] for s in ss])) if ss else None

    rows = []
    for exp, prefix, query, modes in (
            ("E1", "L", "laghos", ["none", "filter", "filter+aggr", "filter+aggr+topN", "filter(csv)"]),
            ("E2", "DW", "deepwater", ["none", "filter", "filter+proj", "filter+proj+aggr"]),
            ("E2", "TQ", "tpch_q1", ["none", "filter", "filter+proj", "filter+proj+aggr"])):
        for m in modes:
            t = med("e7", None, f"{prefix}:{m}")
            if t is not None:
                rows.append({"exp": exp, "query": query, "mode": m, "time_s": t,
                             "wire_bytes": med("e7", None, f"{prefix}:{m}", "wire_bytes")})
    for codec, name in (("raw", "DW"), ("snappy", "DW-snappy"), ("gzip", "DW-gzip"), ("zstd", "DW-zstd")):
        for k, m in ((1, "filter"), (3, "filter+proj+aggr" if codec == "raw" else "all")):
            ss = groups.get(("e7", None, f"{name}:{m}"), [])
            if ss:
                rows.append({"exp": "E3", "codec": codec, "k": k, "time_s": float(np.median([s["time_s"] for s in ss])),
                             "storage": {"media_bytes": float(np.median([s["storage"]["media_bytes"] for s in ss]))}})
    l1 = groups.get(("e7", None, "L1:all"), [])
    if l1:
        rows.append({"exp": "E4", "timings": {k: float(np.median([s["timings"][k] for s in l1]))
                                              for k in l1[0]["timings"]}})
    for st, lb, c in groups:
        if st == "bandwidth":
            q, m = c.split(":")
            rows.append({"exp": "E5", "query": "laghos" if q == "L" else "tpch_q1", "mode": m,
                         "net_mbit": float(lb), "time_s": med(st, lb, c)})
    PL.fig_progressive(rows, "E1", "laghos", "Q1. Laghos – đẩy thêm toán tử xuống OCS (trung vị n=10)", "fig_q1_laghos.png")
    PL.fig_progressive(rows, "E2", "deepwater", "Q2. Deep Water Impact – Project pushdown (trung vị n=10)",
                       "fig_q2_deepwater.png")
    PL.fig_progressive(rows, "E2", "tpch_q1", "Q2. TPC-H Q1 – Project pushdown có thể làm chậm (trung vị n=10)",
                       "fig_q2_tpch.png")
    PL.fig_compression(rows)
    PL.fig_breakdown(rows)
    PL.fig_bandwidth(rows)

    # G3: Presto 0.286 thật + connector Presto-OCS, đẩy dần toán tử (như Hình 5 của bài)
    for q, name, title, fname in (("L", "laghos", "G3. Presto 0.286 thật – Laghos 64 file (trung vị n=10)", "fig_g3_presto_laghos.png"),
                                  ("DW", "deepwater", "G3. Presto 0.286 thật – Deep Water (trung vị n=10)", "fig_g3_presto_deepwater.png")):
        prow = []
        for cat, mode in (("ocsnopd", "none"), ("ocsfilter", "filter"), ("ocs", "đẩy toàn bộ")):
            t = med("presto", None, f"{q}:{cat}")
            if t is not None:
                prow.append({"exp": "G3", "query": name, "mode": mode, "time_s": t,
                             "wire_bytes": med("presto", None, f"{q}:{cat}", "wire_bytes")})
        PL.fig_progressive(prow, "G3", name, title, fname)

    # G3/Q3: Presto thật, cùng truy vấn Deep Water trên 4 kiểu nén, filter vs đẩy toàn bộ
    codecs = [c for c in ("none", "snappy", "gzip", "zstd") if med("presto_q3", None, f"DW_{c}:ocs") is not None]
    if codecs:
        fig, ax = plt.subplots(figsize=(8.5, 4.2))
        w = 0.38
        for k, (cat, lab, col) in enumerate((("ocsfilter", "chỉ filter (như S3 Select)", PL.CONV),
                                             ("ocs", "đẩy toàn bộ", PL.OCS))):
            ts = [med("presto_q3", None, f"DW_{c}:{cat}") for c in codecs]
            xs = [x + (k - 0.5) * w for x in range(len(codecs))]
            ax.bar(xs, ts, w * 0.94, color=col, label=lab)
            for x, t in zip(xs, ts):
                ax.text(x, t, f"{t:.2f}s", ha="center", va="bottom", fontsize=8.5, color=PL.INK2)
        sps = [med("presto_q3", None, f"DW_{c}:ocsfilter") / med("presto_q3", None, f"DW_{c}:ocs") for c in codecs]
        ax.set_xticks(range(len(codecs)), [("raw (không nén)" if c == "none" else c) + f"\nđẩy toàn bộ nhanh {sp:.2f}×"
                                           for c, sp in zip(codecs, sps)])
        ax.set_ylabel("Thời gian thực thi (giây)")
        ax.grid(True, axis="y")
        ax.legend(frameon=False, fontsize=9)
        ax.set_title("Q3 trên Presto 0.286 thật – Deep Water theo kiểu nén (trung vị n=10)", loc="left")
        fig.tight_layout()
        fig.savefig(f"{FIG}/fig_g3_presto_q3.png")
        plt.close(fig)

    # G4b: MinIO Select thật so với OCS giả lập (cùng 2 CPU, 1 Gbps, không giới hạn đĩa)
    for q, name in (("L", "Laghos"), ("DW", "Deep Water"), ("TQ", "TPC-H Q1")):
        mrow = []
        for cfgname, mode in (("minio-select", "MinIO Select"), ("ocs-filter-csv", "filter(csv)"),
                              ("ocs-filter", "filter"), ("ocs-all", "đẩy toàn bộ")):
            t = med("minio", None, f"{q}:{cfgname}")
            if t is not None:
                mrow.append({"exp": "G4b", "query": name, "mode": mode, "time_s": t,
                             "wire_bytes": med("minio", None, f"{q}:{cfgname}", "payload_bytes")})
        PL.fig_progressive(mrow, "G4b", name, f"G4b. MinIO Select thật vs OCS – {name} (trung vị n=4)",
                           f"fig_g4b_minio_{q.lower()}.png",
                           note="Mạng 1 Gbps (tc) và 2 CPU cho cả MinIO lẫn OCS; KHÔNG giới hạn đọc đĩa ở cả hai bên. "
                                "Data movement = byte kết quả nhận được.")


# ------------------------------------------------------------------ hình
def figures(vs, groups, T):
    os.makedirs(FIG, exist_ok=True)
    # R1: forest plot tỉ số tăng tốc vs bài báo
    if vs:
        fig, ax = plt.subplots(figsize=(9, 0.42 * len(vs) + 1.4))
        ys = range(len(vs))[::-1]
        for y, v in zip(ys, vs):
            ax.plot(v["ci"], [y, y], color=PL.OCS, lw=2.5, solid_capstyle="round")
            ax.plot(v["ratio"], y, "o", color=PL.OCS, ms=8, mec=PL.SURF, mew=2, zorder=3)
            ax.plot(v["paper"], y, "D", color=PL.CONV, ms=7, mec=PL.SURF, mew=1.5, zorder=4)
        ax.axvline(1, color=PL.AXIS, lw=1, ls="--")
        ax.set_yticks(list(ys), [v["name"] for v in vs], fontsize=9)
        ax.set_xscale("log")
        ax.grid(True, axis="x", which="both")
        ax.grid(False, axis="y")
        ax.set_xlabel("Tỉ số tăng tốc (log; >1 = nhanh hơn, <1 = chậm hơn)")
        from matplotlib.lines import Line2D
        ax.legend(handles=[Line2D([], [], color=PL.OCS, marker="o", lw=2.5, label="Tái hiện: trung vị + CI 95%"),
                           Line2D([], [], color=PL.CONV, marker="D", lw=0, label="Bài báo [4]")],
                  frameon=False, loc="upper right", fontsize=9)
        ax.set_title("Tỉ số tăng tốc: tái hiện (n=10, CI 95% bootstrap) so với bài báo", loc="left")
        fig.tight_layout()
        fig.savefig(f"{FIG}/fig_r1_speedup_ci.png")
        plt.close(fig)

    # R2: quét băng thông đĩa – Q3 crossover
    lv = sorted({int(k[1]) for k in groups if k[0] == "media"})
    if lv:
        fig, ax = plt.subplots(figsize=(8.5, 4.2))
        series = [("DW-none:all", "raw, đẩy toàn bộ", PL.SERIES[0]), ("DW-zstd:filter", "zstd, chỉ filter", PL.SERIES[1]),
                  ("DW-zstd:all", "zstd, đẩy toàn bộ", PL.SERIES[2]), ("DW-none:filter", "raw, chỉ filter", PL.SERIES[3])]
        for c, lab, col in series:
            meds = [boot_median_ci(T("media", str(m), c)) for m in lv]
            ax.errorbar(lv, [m[0] for m in meds], yerr=[[m[0] - m[1] for m in meds], [m[2] - m[0] for m in meds]],
                        color=col, lw=2, marker="o", ms=7, mec=PL.SURF, mew=2, capsize=4, label=lab)
        ax.set_xscale("log")
        ax.set_xticks(lv, [f"{m} MB/s" for m in lv])
        ax.set_ylabel("Thời gian thực thi (giây)")
        ax.grid(True, axis="both")
        ax.legend(frameon=False, fontsize=9)
        ax.set_title("Q3 theo băng thông đọc thiết bị lưu trữ (Deep Water, mạng 1 Gbps)", loc="left")
        fig.tight_layout()
        fig.savefig(f"{FIG}/fig_r2_media_sweep.png")
        plt.close(fig)

    # R3: quy mô dữ liệu
    def scale_T(n, c):
        return T("scale" if n <= 32 else "scale256", None, f"L{n}:{c}")

    ns = sorted({int(k[2].split(":")[0][1:]) for k in groups if k[0] in ("scale", "scale256")})
    if ns:
        fig, ax = plt.subplots(figsize=(9, 4.4))
        ax.axvspan(45, max(ns) * 1.4, color=PL.AXIS, alpha=0.12, lw=0)
        ax.text(48, 0.98, "vượt page cache của máy ảo: đọc từ ổ đĩa thật", transform=ax.get_xaxis_transform(),
                fontsize=8, color=PL.INK2, va="top")
        for c, lab, col in (("none", "không pushdown", PL.SERIES[3]), ("filter", "chỉ filter", PL.SERIES[1]),
                            ("filter+aggr+topN", "đẩy toàn bộ", PL.SERIES[0])):
            xs = [n for n in ns if scale_T(n, c)]
            meds = [boot_median_ci(scale_T(n, c)) for n in xs]
            ax.errorbar(xs, [m[0] for m in meds], yerr=[[m[0] - m[1] for m in meds], [m[2] - m[0] for m in meds]],
                        color=col, lw=2, marker="o", ms=7, mec=PL.SURF, mew=2, capsize=4, label=lab)
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.set_xticks(ns, [f"{n}" for n in ns])
        ax.set_xlim(min(ns) / 1.4, max(ns) * 1.4)
        ax.set_xlabel("Số file Laghos (mỗi file 4,2 triệu dòng, ~100 MB)")
        ax.set_ylabel("Thời gian thực thi (giây, log)")
        ax.grid(True, axis="both")
        ax.legend(frameon=False, fontsize=9, loc="upper left")
        ax.set_title("Quy mô dữ liệu: Laghos 4–256 file trên engine giả lập (trung vị n=10, mạng 1 Gbps)", loc="left")
        fig.tight_layout()
        fig.savefig(f"{FIG}/fig_r3_scale.png")
        plt.close(fig)

    # R5: mô phỏng engine nhúng chậm – mức chậm do projection theo chi phí mỗi đơn vị
    ns_levels = sorted({int(k[1]) for k in groups if k[0] == "engine"})
    if ns_levels:
        fig, ax = plt.subplots(figsize=(8.5, 4.2))
        for prefix, lab, col, paper in (("DW", "Deep Water", PL.SERIES[0], 1.07), ("TQ", "TPC-H Q1", PL.SERIES[1], 1.55)):
            pts = [boot_ratio_ci(T("engine", str(n), f"{prefix}:filter+proj"), T("engine", str(n), f"{prefix}:filter"))
                   for n in ns_levels]
            xs = [max(n, 0.5) for n in ns_levels]  # trục log: đặt mức 0 tại 0.5
            ax.errorbar(xs, [p[0] for p in pts], yerr=[[p[0] - p[1] for p in pts], [p[2] - p[0] for p in pts]],
                        color=col, lw=2, marker="o", ms=7, mec=PL.SURF, mew=2, capsize=4,
                        label=f"{lab}: thời gian(filter+proj) / thời gian(filter)")
            ax.axhline(paper, color=col, lw=1, ls="--")
            ax.text(xs[-1], paper, f"  bài báo: {lab} chậm {paper - 1:.0%}", color=PL.INK2, fontsize=8, va="bottom", ha="right")
        # đẩy toàn bộ so với filter (Deep Water): >1 nghĩa là đẩy toàn bộ CHẬM hơn filter
        pts = [boot_ratio_ci(T("engine", str(n), "DW:filter+proj+aggr"), T("engine", str(n), "DW:filter"))
               for n in ns_levels]
        ax.errorbar([max(n, 0.5) for n in ns_levels], [p[0] for p in pts],
                    yerr=[[p[0] - p[1] for p in pts], [p[2] - p[0] for p in pts]], color=PL.SERIES[2], lw=2,
                    marker="o", ms=7, mec=PL.SURF, mew=2, capsize=4,
                    label="Deep Water: thời gian(đẩy toàn bộ) / thời gian(filter)")
        ax.axhline(1, color=PL.AXIS, lw=1)
        ax.set_yscale("log")
        ticks = [0.7, 0.85, 1, 1.25, 1.5, 2, 3]
        ax.set_yticks(ticks, [f"{t:g}×" for t in ticks])
        ax.minorticks_off()
        ax.set_xscale("log")
        ax.set_xticks([max(n, 0.5) for n in ns_levels], ["0 (DataFusion)" if n == 0 else f"{n}" for n in ns_levels])
        ax.set_xlabel("Chi phí mô phỏng thêm của engine nhúng (ns / dòng / đơn vị chi phí toán tử)")
        ax.set_ylabel("Tỉ số thời gian so với chỉ filter (log)")
        ax.grid(True, axis="both")
        ax.legend(frameon=False, fontsize=8.5, loc="upper left")
        ax.set_title("Mô phỏng engine storage chậm: khi nào pushdown làm chậm truy vấn", loc="left")
        fig.tight_layout()
        fig.savefig(f"{FIG}/fig_r5_engine_cost.png")
        plt.close(fig)

    # R4: độ nhạy theo CPU storage
    cl = sorted({int(k[1]) for k in groups if k[0] == "cpus"})
    if cl:
        fig, ax = plt.subplots(figsize=(8.5, 4.2))
        for c, lab, col in (("DW:filter", "chỉ filter", PL.SERIES[1]), ("DW:filter+proj", "filter + projection thường", PL.SERIES[2]),
                            ("DWH:filter+proj", "filter + projection nặng", PL.SERIES[3]), ("DW:filter+proj+aggr", "đẩy toàn bộ", PL.SERIES[0])):
            meds = [boot_median_ci(T("cpus", str(n), c)) for n in cl]
            ax.errorbar(cl, [m[0] for m in meds], yerr=[[m[0] - m[1] for m in meds], [m[2] - m[0] for m in meds]],
                        color=col, lw=2, marker="o", ms=7, mec=PL.SURF, mew=2, capsize=4, label=lab)
        ax.set_xticks(cl, [f"{n} CPU" for n in cl])
        ax.set_ylabel("Thời gian thực thi (giây)")
        ax.grid(True, axis="both")
        ax.legend(frameon=False, fontsize=9)
        ax.set_title("Độ nhạy theo số CPU của storage node (Deep Water, mạng 1 Gbps)", loc="left")
        fig.tight_layout()
        fig.savefig(f"{FIG}/fig_r4_cpus.png")
        plt.close(fig)


if __name__ == "__main__":
    main()
