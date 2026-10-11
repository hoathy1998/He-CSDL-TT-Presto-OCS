"""In bảng tóm tắt results/bench.json (dùng khi viết báo cáo)."""
import json

d = json.load(open("/app/results/bench.json"))
print("cluster:", d["cluster"], "reps:", d["reps"])
for r in d["rows"]:
    if r["exp"] == "E6-decision":
        print(f"DECISION {r['query']:10s} auto={r['auto_mode']:22s} best={r['best_mode']:22s} "
              f"auto_t={r['auto_time_s']:.2f} best_t={r['best_time_s']:.2f} notes={r['notes']}")
        continue
    s = r["storage"]
    line = (f"{r['exp']} {r['query']:24s} {r['mode']:24s} net={r['net_mbit']:6.0f} t={r['time_s']:7.2f} "
            f"all={[round(x, 2) for x in r['time_all']]} wire={r['wire_bytes'] / 1e6:10.3f}MB "
            f"payload={r['payload_bytes'] / 1e6:9.3f}MB rows={r['result_rows']}")
    if r["exp"] == "E3":
        line += f" codec={r['codec']} media={s['media_bytes'] / 1e6:.0f}MB read_s={s['read_s']:.1f} exec_s={s['exec_s']:.1f}"
    if r["exp"] == "E4":
        line += " timings_ms=" + json.dumps({k: round(v * 1000, 1) for k, v in r["timings"].items()})
        line += f" substrait_bytes={r['substrait_bytes']}"
    if r["exp"] == "E6":
        line += (f" est_rows={r['est_rows']:.0f} act_rows={s['out_rows']} est_MB={r['est_bytes'] / 1e6:.2f}"
                 f" est_sel={r['est_selectivity']:.4f} est_s={r['est_s']:.2f}")
    print(line)
