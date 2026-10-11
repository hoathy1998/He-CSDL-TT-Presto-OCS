"""Vẽ biểu đồ từ results/bench.json -> results/figures/*.png (dùng cho slide / báo cáo)."""
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

IN = "/app/results/bench.json"
OUT = "/app/results/figures"

# Bảng màu đã kiểm tra (dataviz validator): cam = object storage truyền thống, xanh = OCS mở rộng
CONV, OCS = "#eb6834", "#2a78d6"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
INK, INK2, MUTED, GRID, AXIS, SURF = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#fcfcfb"

LABEL = {
    "none": "không\npushdown", "filter": "filter", "filter(csv)": "filter\n(CSV)",
    "filter+aggr": "filter\n+aggr", "filter+aggr+topN": "filter\n+aggr\n+topN",
    "filter+proj": "filter\n+proj", "filter+proj+aggr": "filter\n+proj\n+aggr",
    "filter+proj+aggr+sort": "filter+proj\n+aggr+sort",
    "MinIO Select": "MinIO\nSelect thật", "đẩy toàn bộ": "đẩy\ntoàn bộ",
}

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 10, "axes.edgecolor": AXIS, "axes.labelcolor": INK2,
    "xtick.color": INK2, "ytick.color": INK2, "axes.titlecolor": INK, "axes.titlesize": 11,
    "axes.titleweight": "bold", "figure.facecolor": SURF, "axes.facecolor": SURF,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "axes.grid.axis": "y",
    "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True, "savefig.dpi": 200,
})


def fmt_bytes(b):
    for u, s in ((1e9, "GB"), (1e6, "MB"), (1e3, "KB")):
        if b >= u:
            return f"{b / u:.3g} {s}"
    return f"{b:.0f} B"


def is_conv(mode):
    return mode in ("none", "filter", "filter(csv)", "MinIO Select")


def legend(fig):
    from matplotlib.patches import Patch
    fig.legend(handles=[Patch(color=CONV, label="Object storage thường / S3 Select"),
                        Patch(color=OCS, label="Pushdown mở rộng của OCS")],
               frameon=False, loc="upper right", ncol=2, fontsize=9, bbox_to_anchor=(0.995, 0.925))


def bars(ax, labels, values, colors, texts, ylabel, log=False):
    x = range(len(values))
    ax.bar(x, values, width=0.62, color=colors, edgecolor=SURF, linewidth=2)
    if log:
        ax.set_yscale("log")
        ax.grid(True, axis="y", which="major")
    top = max(values)
    for i, (v, t) in enumerate(zip(values, texts)):
        ax.text(i, v * (1.15 if log else 1) + (0 if log else top * 0.015), t, ha="center", va="bottom",
                fontsize=8.5, color=INK)
    ax.set_xticks(list(x), labels, fontsize=8.5)
    ax.set_ylabel(ylabel)
    if not log:
        ax.set_ylim(0, top * 1.22)
    else:
        ax.set_ylim(min(values) / 3, top * 8)


DEFAULT_NOTE = "Mạng 1 Gbps (đo được ~850 Mbps), storage 2 CPU, SSD 500 MB/s (đo được ~420 MB/s)."


def fig_progressive(rows, exp, query, title, fname, baseline="filter", note=DEFAULT_NOTE):
    rs = [r for r in rows if r["exp"] == exp and r["query"] == query]
    if not rs:
        return
    base = next(r for r in rs if r["mode"] == baseline)
    labels = [LABEL.get(r["mode"], r["mode"]) for r in rs]
    colors = [CONV if is_conv(r["mode"]) else OCS for r in rs]
    times = [r["time_s"] for r in rs]
    tlab = []
    for r in rs:
        s = f"{r['time_s']:.2f}s"
        if r["mode"] not in ("none", baseline):
            sp = base["time_s"] / r["time_s"]
            slow = 1 / sp - 1
            s += f"\n{sp:.2f}×" if sp >= 1 else f"\nchậm {slow:.1%}" if slow < 0.01 else f"\nchậm {slow:.0%}"
        tlab.append(s)
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.1))
    bars(a1, labels, times, colors, tlab, "Thời gian thực thi (giây)")
    a1.set_title("Thời gian thực thi (trung vị)", loc="left")
    legend(fig)
    wb = [max(r["wire_bytes"], 1) for r in rs]
    bars(a2, labels, wb, colors, [fmt_bytes(b) for b in wb], "Dữ liệu truyền OCS → compute (log)", log=True)
    a2.set_title("Data movement qua mạng", loc="left")
    fig.suptitle(title, x=0.01, ha="left", fontsize=12.5, fontweight="bold", color=INK)
    fig.text(0.01, 0.005, f"×: speedup so với '{baseline}' (pushdown kiểu S3 Select).\n{note}",
             fontsize=8, color=MUTED, linespacing=1.4)
    fig.tight_layout(rect=(0, 0.07, 1, 0.95))
    fig.savefig(os.path.join(OUT, fname))
    plt.close(fig)


def fig_compression(rows):
    rs = [r for r in rows if r["exp"] == "E3"]
    if not rs:
        return
    codecs = ["raw", "snappy", "gzip", "zstd"]
    get = lambda c, k: next(r for r in rs if r["codec"] == c and (r["k"] == 1 if k == "f" else r["k"] > 1))
    f = [get(c, "f")["time_s"] for c in codecs]
    a = [get(c, "a")["time_s"] for c in codecs]
    fig, ax = plt.subplots(figsize=(8.5, 4.2))
    w = 0.36
    xs = range(len(codecs))
    ax.bar([x - w / 2 for x in xs], f, w, color=CONV, edgecolor=SURF, linewidth=2, label="Filter pushdown")
    ax.bar([x + w / 2 for x in xs], a, w, color=OCS, edgecolor=SURF, linewidth=2, label="All pushdown (OCS)")
    top = max(f)
    for x, fv, av in zip(xs, f, a):
        ax.text(x - w / 2, fv + top * 0.015, f"{fv:.2f}s", ha="center", va="bottom", fontsize=8.5, color=INK)
        ax.text(x + w / 2, av + top * 0.015, f"{av:.2f}s\n{fv / av:.2f}×", ha="center", va="bottom",
                fontsize=8.5, color=INK)
    sizes = {c: get(c, "f")["storage"]["media_bytes"] for c in codecs}
    ax.set_xticks(list(xs), [f"{c}\n(đọc {fmt_bytes(sizes[c])})" for c in codecs])
    ax.set_ylim(0, top * 1.25)
    ax.set_ylabel("Thời gian thực thi (giây)")
    ax.legend(frameon=False, loc="upper right")
    ax.set_title("Q3. Pushdown kết hợp nén dữ liệu – Deep Water Impact", loc="left")
    fig.text(0.01, 0.005, "×: speedup của All pushdown so với Filter pushdown cùng kiểu nén. "
             "'đọc': byte đọc từ thiết bị lưu trữ.", fontsize=8, color=MUTED)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(os.path.join(OUT, "fig_q3_compression.png"))
    plt.close(fig)


def fig_breakdown(rows):
    r = next((r for r in rows if r["exp"] == "E4"), None)
    if not r:
        return
    names = {"plan_analysis": "Phân tích logical plan", "substrait_gen": "Sinh Substrait IR",
             "pushdown_transfer": "Pushdown & truyền kết quả", "post_scan": "Thực thi sau scan (residual)",
             "others": "Khác"}
    t = r["timings"]
    total = sum(t.values())
    keys = list(names)
    fig, ax = plt.subplots(figsize=(8.5, 3.4))
    vals = [t[k] * 1000 for k in keys]
    ax.barh(range(len(keys)), vals, color=OCS, height=0.55, edgecolor=SURF, linewidth=2)
    for i, v in enumerate(vals):
        ax.text(v + total * 1000 * 0.01, i, f"{v:.1f} ms ({v / (total * 1000):.2%})", va="center", fontsize=9)
    ax.set_yticks(range(len(keys)), [names[k] for k in keys])
    ax.invert_yaxis()
    ax.grid(True, axis="x")
    ax.grid(False, axis="y")
    ax.set_xlim(0, max(vals) * 1.35)
    ax.set_xlabel("ms")
    ax.set_title(f"Q4. Phân rã thời gian 1 truy vấn (1 file Laghos) – tổng {total * 1000:.0f} ms", loc="left")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fig_q4_breakdown.png"))
    plt.close(fig)


def fig_bandwidth(rows):
    rs = [r for r in rows if r["exp"] == "E5"]
    if not rs:
        return
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.1))
    for ax, (q, title) in zip(axes, (("laghos", "Laghos"), ("tpch_q1", "TPC-H Q1"))):
        qr = [r for r in rs if r["query"] == q]
        modes = list(dict.fromkeys(r["mode"] for r in qr))
        # màu theo vai trò chế độ, thống nhất giữa các panel: đẩy toàn bộ luôn là slot 1 (xanh)
        role = {"none": 3, "filter": 1, "filter+proj": 2, "filter+aggr": 2}
        for m in modes:
            pts = sorted((r["net_mbit"], r["time_s"]) for r in qr if r["mode"] == m)
            xs, ys = zip(*pts)
            label = m if m in role else f"{m} (đẩy toàn bộ)"
            ax.plot(xs, ys, color=SERIES[role.get(m, 0)], lw=2, marker="o", ms=7, mec=SURF, mew=2, label=label)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xticks([100, 1000, 10000], ["100 Mbps", "1 Gbps", "10 Gbps"])
        ax.set_xlim(70, 14000)
        ax.grid(True, which="major", axis="both")
        ax.set_ylabel("Thời gian thực thi (giây, log)")
        ax.set_title(title, loc="left")
        ax.legend(frameon=False, fontsize=8.5, loc="upper right")
    fig.suptitle("E5. Lợi ích pushdown phụ thuộc băng thông mạng", x=0.01, ha="left", fontsize=12.5,
                 fontweight="bold", color=INK)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(os.path.join(OUT, "fig_e5_bandwidth.png"))
    plt.close(fig)


def fig_estimates(rows):
    rs = [r for r in rows if r["exp"] == "E6" and r["k"] > 0]
    if not rs:
        return
    fig, ax = plt.subplots(figsize=(6.2, 5))
    qs = list(dict.fromkeys(r["query"] for r in rs))
    for i, q in enumerate(qs):
        qr = [r for r in rs if r["query"] == q]
        ax.scatter([max(r["est_bytes"], 1) for r in qr], [max(r["payload_bytes"], 1) for r in qr],
                   s=60, color=SERIES[i], edgecolor=SURF, linewidth=2, label=q, zorder=3)
        for r in qr:
            # chỉ ghi nhãn các điểm không nằm trong cụm dày ở góc trên (cụm đó đọc qua bảng trong báo cáo)
            if r["est_bytes"] < 1e8 or r["mode"] == "filter+aggr":
                ax.annotate(r["mode"], (max(r["est_bytes"], 1), max(r["payload_bytes"], 1)), fontsize=7.5,
                            color=INK2, xytext=(6, -3), textcoords="offset points")
    lo, hi = 1e2, 3e9
    ax.plot([lo, hi], [lo, hi], color=AXIS, lw=1, ls="--", zorder=1)
    ax.text(hi / 4, hi / 1.4, "ước lượng = thực tế", color=MUTED, fontsize=8, ha="right")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlim(lo, hi)
    ax.set_ylim(lo, hi)
    ax.grid(True, which="major", axis="both")
    ax.set_xlabel("Byte truyền ƯỚC LƯỢNG (Selectivity Analyzer)")
    ax.set_ylabel("Byte truyền THỰC TẾ")
    ax.set_title("E6. Độ chính xác của Selectivity Analyzer", loc="left")
    ax.legend(frameon=False, fontsize=8.5, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fig_e6_estimates.png"))
    plt.close(fig)


def main():
    os.makedirs(OUT, exist_ok=True)
    with open(IN) as fh:
        rows = json.load(fh)["rows"]
    fig_progressive(rows, "E1", "laghos", "Q1. Laghos – đẩy thêm toán tử xuống OCS", "fig_q1_laghos.png")
    fig_progressive(rows, "E2", "deepwater", "Q2. Deep Water Impact – Project pushdown có thể làm chậm",
                    "fig_q2_deepwater.png")
    fig_progressive(rows, "E2", "tpch_q1", "Q2. TPC-H Q1 – Project pushdown có thể làm chậm",
                    "fig_q2_tpch.png")
    fig_compression(rows)
    fig_breakdown(rows)
    fig_bandwidth(rows)
    fig_estimates(rows)
    print("đã vẽ:", sorted(os.listdir(OUT)))


if __name__ == "__main__":
    main()
