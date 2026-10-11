"""Q4 trên Presto thật: phân rã chi phí một truy vấn Laghos trên 1 file, lấy từ EventListener của connector.

  python scripts/presto_q4.py      (chạy trong container compute, sau `presto_bench.py bench --set q4`)

Mỗi mẫu trong results/reliability/presto_q4.json được ghép với đúng một dòng results/presto/history.jsonl
theo khoảng thời gian chạy. Các thành phần:
  planning      : Presto lập kế hoạch (đã gồm Local Optimizer của connector)
  optimizer     : riêng Local Optimizer của connector (nhận diện + dựng SQL)
  substrait     : sinh kế hoạch Substrait bằng isthmus (R2), một lần mỗi truy vấn
  scan          : toán tử đọc của Presto = chờ OCS đọc + thực thi + gửi kết quả qua mạng
  residual      : các toán tử còn lại chạy ở Presto (Top-N cuối, gom nhóm final, exchange, ...)
Ghi results/presto/q4_breakdown.json và .md.
"""
import datetime
import json
import statistics as stt

HIST = "/app/results/presto/history.jsonl"
SAMPLES = "/app/results/reliability/presto_q4.json"
OUT = "/app/results/presto/q4_breakdown"
SCAN_OPS = ("TableScanOperator", "ScanFilterAndProjectOperator")


def ts(s):
    return datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def main():
    hist = [json.loads(l) for l in open(HIST, encoding="utf-8") if l.strip()]
    hist = [h for h in hist if "laghos_one" in h.get("query", "") and not h["query"].lstrip().upper().startswith("EXPLAIN")]
    rows = {}
    for s in json.load(open(SAMPLES)):
        # truy vấn chỉ ~1 s và chạy liền nhau -> chọn bản ghi tạo gần thời điểm bắt đầu nhất, trong khoảng chạy của mẫu
        cand = [h for h in hist if s["started"] - 0.5 <= ts(h["create_time"]) <= s["ended"]]
        if not cand:
            continue
        h = min(cand, key=lambda h: abs(ts(h["create_time"]) - s["started"]))
        ops = h.get("operators", {})
        scan = sum(v["wall_ms"] for k, v in ops.items() if k in SCAN_OPS)
        other = sum(v["wall_ms"] for k, v in ops.items() if k not in SCAN_OPS)
        rows.setdefault(s["cfg"].split(":")[1], []).append({
            "elapsed_ms": h["elapsed_ms"], "client_ms": s["time_s"] * 1000, "planning_ms": h["planning_ms"],
            "optimizer_ms": h.get("ocs_optimizer_ms", 0.0), "substrait_ms": h.get("ocs_substrait_ms", 0.0),
            "scan_wall_ms": scan, "residual_wall_ms": other, "cpu_ms": h["cpu_ms"],
            "substrait_failures": h.get("ocs_substrait_failures", 0), "wire_bytes": s["wire_bytes"]})
    summary = {}
    for cat, rs in rows.items():
        m = {k: stt.median(r[k] for r in rs) for k in rs[0]}
        m["n"] = len(rs)
        m["overhead_pct"] = 100 * (m["optimizer_ms"] + m["substrait_ms"]) / m["elapsed_ms"]
        summary[cat] = m
    json.dump(summary, open(OUT + ".json", "w"), indent=1)
    lines = ["| catalog | n | tổng (ms) | planning | optimizer | substrait | scan (OCS + mạng) | residual (wall, cộng dồn) "
             "| overhead connector | dữ liệu qua mạng |", "|---|---|---|---|---|---|---|---|---|---|"]
    for cat in ("ocsnopd", "ocsfilter", "ocs"):
        if cat in summary:
            m = summary[cat]
            lines.append(f"| {cat} | {m['n']} | {m['elapsed_ms']:.0f} | {m['planning_ms']:.0f} | {m['optimizer_ms']:.2f} | "
                         f"{m['substrait_ms']:.1f} | {m['scan_wall_ms']:.0f} | {m['residual_wall_ms']:.0f} | "
                         f"{m['overhead_pct']:.2f}% | {m['wire_bytes'] / 1e6:.2f} MB |")
    open(OUT + ".md", "w", encoding="utf-8").write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
