"""Thí nghiệm đánh giá độ tin cậy (chạy trong container compute).

Nguyên tắc (Hoefler & Belli, SC'15): đo lặp nhiều lần, thứ tự cấu hình XEN KẼ NGẪU NHIÊN trong từng vòng
(randomized block design) để trôi hiệu năng theo thời gian ảnh hưởng đều mọi cấu hình, có vòng khởi động
không tính, giữ lại TOÀN BỘ số đo thô kèm mốc thời gian, mỗi lần chạy đều so khớp với oracle DuckDB độc lập.

  python scripts/reliability.py calib           # hiệu chuẩn mạng (tc) và thiết bị lưu trữ giả lập
  python scripts/reliability.py oracle          # so khớp mọi truy vấn x mọi chế độ với oracle DuckDB
  python scripts/reliability.py e7              # các thí nghiệm chính, n=10, xen kẽ ngẫu nhiên
  python scripts/reliability.py media           # B: quét băng thông đọc thiết bị 150/500/2000 MB/s
  python scripts/reliability.py ablation        # C: TPC-H decimal / double / nén Arrow IPC
  python scripts/reliability.py bandwidth       # D: quét băng thông mạng, có thêm filter+aggr
  python scripts/reliability.py scale           # E: Laghos 4/8/16/32 object
  python scripts/reliability.py cpus --label 1  # A: độ nhạy theo số CPU storage (host đổi OCS_CPUS)
"""
import argparse
import json
import os
import random
import time

import pyarrow.parquet as pq

from ocsdemo.engine import Engine, same_result
from ocsdemo.queries import DEEPWATER, DEEPWATER_HEAVY, LAGHOS, TPCH_PROJ, TPCH_Q1, with_table

OUT = "/app/results/reliability"
ORACLE = "/app/results/oracle"


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def cfg(name, q, mode, max_splits=None, csv=False, ipc=False):
    return {"name": name, "q": q, "mode": mode, "max_splits": max_splits, "csv": csv, "ipc": ipc}


def oracle_key(c):
    q = c["q"]
    if q.name.startswith("laghos"):
        default = 256 if q.table == "laghos_full" else 64
        return f"laghos_{c['max_splits'] or default}"
    if q.name.startswith("deepwater"):
        return "deepwater"
    if q.name.startswith("tpch_q1"):
        return "tpch_q1_double" if q.table == "lineitem_double" else "tpch_q1"
    if q.name.startswith("tpch_proj"):
        return "tpch_proj"
    if q.name == "tpch_join":
        return "tpch_join"
    raise KeyError(q.name)


def node_addrs(n):
    return [f"grpc://{h}:8815" for h in ("ocs", "ocs2", "ocs3")[:n]]


class Runner:
    def __init__(self, addrs=None):
        self.eng = Engine(addrs)
        self._oracle = {}

    def oracle(self, key):
        if key not in self._oracle:
            self._oracle[key] = pq.read_table(f"{ORACLE}/{key}.parquet")
        return self._oracle[key]

    def once(self, c):
        if c.get("join"):
            j = self.eng.run_join(c["q"], c["mode"])
            cl = self.eng.cluster()
            return {"cfg": c["name"], "query": c["q"].name, "table": "lineitem+orders", "mode": c["mode"],
                    "max_splits": None, "ipc": False, "time_s": j["total_s"], "wire_bytes": j["wire_bytes"],
                    "payload_bytes": j["payload_bytes"], "timings": j["timings"], "storage": j["storage"],
                    "started": j["started"], "ended": j["ended"], "branches": j["branches"],
                    "correct": same_result(self.oracle(oracle_key(c)), j["result"]),
                    "net_mbit": cl["net_mbit"], "media_mbps": cl["media_mbps"],
                    "storage_threads": cl["storage_threads"]}
        r = self.eng.run(c["q"], c["mode"], kind_filter_csv=c["csv"], max_splits=c["max_splits"],
                         use_history=False, record=False, ipc_compression=c["ipc"])
        cl = self.eng.cluster()
        return {
            "cfg": c["name"], "query": c["q"].name, "table": c["q"].table, "mode": r.mode,
            "max_splits": c["max_splits"], "ipc": c["ipc"], "time_s": r.total_s,
            "wire_bytes": r.wire_bytes, "payload_bytes": r.payload_bytes, "timings": r.timings,
            "storage": r.storage, "started": r.started, "ended": r.ended,
            "correct": same_result(self.oracle(oracle_key(c)), r.result),
            "net_mbit": cl["net_mbit"], "media_mbps": cl["media_mbps"], "storage_threads": cl["storage_threads"],
        }

    def interleaved(self, stage, cfgs, reps, seed, label=None, extra=None):
        rng = random.Random(seed)
        log(f"{stage}{'/' + str(label) if label else ''}: khởi động {len(cfgs)} cấu hình")
        for c in cfgs:
            self.once(c)
        samples = []
        for rep in range(reps):
            order = cfgs[:]
            rng.shuffle(order)
            for i, c in enumerate(order):
                s = self.once(c)
                s.update(stage=stage, label=label, rep=rep, order=i, **(extra or {}))
                samples.append(s)
                if not s["correct"]:
                    log(f"  !! SAI KẾT QUẢ: {c['name']}")
            done = [s for s in samples if s["rep"] == rep]
            log(f"  vòng {rep + 1}/{reps} xong ({sum(s['time_s'] for s in done):.0f}s)")
            save(f"{stage}{'_' + str(label) if label is not None else ''}", samples)
        return samples


def save(name, obj):
    os.makedirs(OUT, exist_ok=True)
    with open(f"{OUT}/{name}.json", "w") as fh:
        json.dump(obj, fh, indent=1)


# ------------------------------------------------------------------ các giai đoạn
def stage_calib(R):
    """Hiệu chuẩn: đo băng thông mạng và thiết bị THỰC ĐẠT ĐƯỢC so với giá trị khai báo,
    bằng hồi quy tuyến tính thời gian theo số byte (t = a + bytes / throughput)."""
    import numpy as np
    conn, cat = R.eng.conn, R.eng.catalog
    splits = cat["laghos"]["splits"]
    out = {"network": [], "media": []}
    conn.set_media(0)                                   # bỏ giới hạn đĩa khi đo mạng
    for mbit, ks, reps in ((100, (1, 2, 3), 2), (1000, (2, 4, 8), 3)):
        conn.set_net(mbit)
        pts = []
        for _ in range(reps):
            for k in ks:
                tx0, t0 = conn.tx_bytes(), time.perf_counter()
                conn.scan(splits[:k], "raw", LAGHOS.columns)
                pts.append((conn.tx_bytes() - tx0, time.perf_counter() - t0))
        b, a = np.polyfit([p[0] for p in pts], [p[1] for p in pts], 1)
        fit = {"declared_mbit": mbit, "measured_mbit": 8 / b / 1e6, "intercept_s": a, "points": pts}
        out["network"].append(fit)
        log(f"calib mạng: khai báo {mbit} Mbps -> đo được {fit['measured_mbit']:.0f} Mbps")
    conn.set_net(0)                                     # bỏ giới hạn mạng khi đo đĩa
    old = conn.parallelism
    conn.parallelism = 1
    for mbps in (150, 500, 2000):
        conn.set_media(mbps)
        pts = []
        for k in (1, 2, 3):
            _, _, st = conn.scan(splits[:k], "raw", LAGHOS.columns)
            pts.append((sum(s["media_bytes"] for s in st), sum(s["read_s"] for s in st)))
        b, a = np.polyfit([p[0] for p in pts], [p[1] for p in pts], 1)
        fit = {"declared_mbps": mbps, "measured_mbps": 1 / b / 1e6, "intercept_s": a, "points": pts}
        out["media"].append(fit)
        log(f"calib đĩa: khai báo {mbps} MB/s -> đo được {fit['measured_mbps']:.0f} MB/s")
    conn.parallelism = old
    conn.set_media(500)
    conn.set_net(1000)
    save("calib", out)


def stage_oracle(R):
    rows = []
    for q, ms, modes in ((LAGHOS, 16, ["none", "filter", "filter+aggr", "filter+aggr+topN"]),
                         (DEEPWATER, None, ["none", "filter", "filter+proj", "filter+proj+aggr"]),
                         (DEEPWATER_HEAVY, None, ["filter+proj", "filter+proj+aggr"]),
                         (TPCH_Q1, None, ["none", "filter", "filter+proj", "filter+proj+aggr",
                                          "filter+proj+aggr+sort", "auto"]),
                         (with_table(TPCH_Q1, "lineitem_double"), None, ["none", "filter+proj+aggr"]),
                         (TPCH_PROJ, None, ["none", "filter", "filter+proj"])):
        for m in modes:
            s = R.once(cfg(f"{q.name}:{m}", q, m, ms))
            rows.append({k: s[k] for k in ("cfg", "query", "table", "mode", "correct", "time_s")})
            log(f"oracle {q.name:28s} {s['mode']:24s} {'ĐÚNG' if s['correct'] else 'SAI'}")
        s = R.once(cfg(f"{q.name}:filter(csv)", q, "filter", ms, csv=True)) if q is LAGHOS else None
        if s:
            rows.append({k: s[k] for k in ("cfg", "query", "table", "mode", "correct", "time_s")})
            log(f"oracle {q.name:28s} {s['mode']:24s} {'ĐÚNG' if s['correct'] else 'SAI'}")
    for codec in ("deepwater_snappy", "deepwater_gzip", "deepwater_zstd"):
        s = R.once(cfg(f"{codec}:all", with_table(DEEPWATER, codec), "all"))
        rows.append({k: s[k] for k in ("cfg", "query", "table", "mode", "correct", "time_s")})
        log(f"oracle {codec:28s} all {'ĐÚNG' if s['correct'] else 'SAI'}")
    save("oracle", rows)


def e7_cfgs():
    L = lambda m, **k: cfg(f"L:{m}", LAGHOS, m, 16, **k)
    c = [L("none"), L("filter"), L("filter+aggr"), L("filter+aggr+topN"),
         cfg("L:filter(csv)", LAGHOS, "filter", 16, csv=True),
         cfg("L1:all", LAGHOS, "filter+aggr+topN", 1)]
    c += [cfg(f"DW:{m}", DEEPWATER, m) for m in ("none", "filter", "filter+proj", "filter+proj+aggr")]
    c += [cfg(f"TQ:{m}", TPCH_Q1, m) for m in ("none", "filter", "filter+proj", "filter+proj+aggr")]
    for codec in ("snappy", "gzip", "zstd"):
        q = with_table(DEEPWATER, f"deepwater_{codec}")
        c += [cfg(f"DW-{codec}:filter", q, "filter"), cfg(f"DW-{codec}:all", q, "filter+proj+aggr")]
    return c


def q3_cfgs():
    c = []
    for codec in ("none", "snappy", "gzip", "zstd"):
        q = DEEPWATER if codec == "none" else with_table(DEEPWATER, f"deepwater_{codec}")
        c += [cfg(f"DW-{codec}:filter", q, "filter"), cfg(f"DW-{codec}:all", q, "filter+proj+aggr")]
    return c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("stage")
    ap.add_argument("--label", default=None)
    ap.add_argument("--reps", type=int, default=None)
    a = ap.parse_args()
    R = Runner()
    R.eng.conn.set_net(1000)
    R.eng.conn.set_media(500)
    if a.stage == "calib":
        stage_calib(R)
    elif a.stage == "oracle":
        stage_oracle(R)
    elif a.stage == "e7":
        R.interleaved("e7", e7_cfgs(), a.reps or 10, seed=7)
    elif a.stage == "media":
        for mbps in (150, 500, 2000):
            R.eng.conn.set_media(mbps)
            R.interleaved("media", q3_cfgs(), a.reps or 5, seed=mbps, label=mbps)
        R.eng.conn.set_media(500)
    elif a.stage == "ablation":
        qd = with_table(TPCH_Q1, "lineitem_double")
        c = [cfg(f"TQ-dec:{m}", TPCH_Q1, m) for m in ("none", "filter", "filter+proj", "filter+proj+aggr")]
        c += [cfg(f"TQ-dbl:{m}", qd, m) for m in ("none", "filter", "filter+proj", "filter+proj+aggr")]
        c += [cfg(f"TQ-ipc:{m}", TPCH_Q1, m, ipc=True) for m in ("filter", "filter+proj", "filter+proj+aggr")]
        R.interleaved("ablation", c, a.reps or 5, seed=3)
    elif a.stage == "bandwidth":
        c = [cfg(f"L:{m}", LAGHOS, m, 16) for m in ("filter", "filter+aggr", "filter+aggr+topN")]
        c += [cfg(f"TQ:{m}", TPCH_Q1, m) for m in ("filter", "filter+proj", "filter+proj+aggr")]
        for mbit in (100, 1000, 10000):
            R.eng.conn.set_net(mbit)
            R.interleaved("bandwidth", c, a.reps or 5, seed=mbit, label=mbit)
        R.eng.conn.set_net(1000)
    elif a.stage == "scale":
        # Khối 1 – dữ liệu nằm gọn trong page cache của máy ảo (4–32 file, tối đa 3,3 GB). Chỉ xen kẽ các mức
        # TRONG khối: nếu trộn với 64 file trở lên, mỗi lần chạy lớn sẽ đẩy dữ liệu nhỏ ra khỏi cache và làm
        # chế độ đẩy toàn bộ (giới hạn bởi tốc độ đọc) chậm đi một cách giả tạo (đã gặp ở lượt đo trước).
        c = [cfg(f"L{n}:{m}", LAGHOS, m, n) for n in (4, 8, 16, 32) for m in ("none", "filter", "filter+aggr+topN")]
        R.interleaved("scale", c, a.reps or 10, seed=11)
    elif a.stage == "scale256":
        # Khối 2 – dữ liệu vượt page cache (64, 128, 256 file; bảng laghos_full): mọi mức đều đọc đĩa thật nên
        # xen kẽ với nhau không gây lệch. "none" không chạy được (engine giữ toàn bộ dữ liệu thô trong RAM);
        # filter chỉ tới 128 file (256 file ~10 GB kết quả lọc). Presto thật đo đủ 256 file (presto_bench.py --set l256).
        LF = with_table(LAGHOS, "laghos_full")
        n_full = len(R.eng.catalog["laghos_full"]["splits"])
        c = [cfg(f"L{n}:{m}", LF, m, n) for n in (64, 128) for m in ("filter", "filter+aggr+topN") if n_full >= n]
        if n_full >= 256:
            c.append(cfg("L256:filter+aggr+topN", LF, "filter+aggr+topN", 256))
        R.interleaved("scale256", c, a.reps or 10, seed=256)
    elif a.stage == "nodes":
        # G2.2: 1/2/3 storage node, mỗi node 2 CPU + NIC 1 Gbps + SSD 500 MB/s riêng
        for n in (1, 2, 3):
            Rn = Runner(node_addrs(n))
            Rn.eng.conn.set_net(1000)
            Rn.eng.conn.set_media(500)
            c = [cfg(f"L:{m}", LAGHOS, m, 16) for m in ("none", "filter", "filter+aggr+topN")]
            c += [cfg(f"DW:{m}", DEEPWATER, m) for m in ("filter", "filter+proj+aggr")]
            c += [cfg(f"TQ:{m}", TPCH_Q1, m) for m in ("filter", "filter+proj+aggr")]
            Rn.interleaved("nodes", c, a.reps or 5, seed=200 + n, label=n)
    elif a.stage == "concurrency":
        # G2.2: k truy vấn đồng thời trên cụm 3 node -> độ trễ từng truy vấn và thông lượng
        from concurrent.futures import ThreadPoolExecutor
        R3 = Runner(node_addrs(3))
        R3.eng.conn.set_net(1000)
        R3.eng.conn.set_media(500)
        for key in ("laghos_16",):
            R3.oracle(key)
        modes = {"filter": cfg("L:filter", LAGHOS, "filter", 16),
                 "all": cfg("L:filter+aggr+topN", LAGHOS, "filter+aggr+topN", 16)}
        for c in modes.values():           # khởi động
            R3.once(c)
        rng, samples = random.Random(31), []
        for rep in range(a.reps or 3):
            combos = [(k, m) for k in (1, 4, 8) for m in modes]
            rng.shuffle(combos)
            for k, m in combos:
                t0 = time.perf_counter()
                with ThreadPoolExecutor(k) as ex:
                    res = list(ex.map(lambda _: R3.once(modes[m]), range(k)))
                makespan = time.perf_counter() - t0
                for i, s in enumerate(res):
                    s.update(stage="concurrency", label=f"{m}:k={k}", cfg=f"{m}:k={k}", rep=rep, order=i,
                             k=k, makespan_s=makespan, throughput_qps=k / makespan)
                    samples.append(s)
                log(f"  {m:6s} k={k}: makespan {makespan:6.2f}s, trễ trung vị "
                    f"{sorted(s['time_s'] for s in res)[k // 2]:.2f}s, đúng={all(s['correct'] for s in res)}")
            save("concurrency", samples)
    elif a.stage == "join":
        from ocsdemo.queries import TPCH_JOIN
        c = [dict(cfg(f"JOIN:{m}", TPCH_JOIN, m), join=True) for m in ("none", "filter", "all")]
        R.interleaved("join", c, a.reps or 10, seed=21)
    elif a.stage == "engine":
        # G2.3: engine nhúng chậm hơn DataFusion c ns / (dòng × đơn vị chi phí)
        c = [cfg(f"DW:{m}", DEEPWATER, m) for m in ("filter", "filter+proj", "filter+proj+aggr")]
        c += [cfg(f"TQ:{m}", TPCH_Q1, m) for m in ("filter", "filter+proj", "filter+proj+aggr")]
        levels = [int(x) for x in a.label.split(",")] if a.label else [0, 1, 2, 5, 10, 20]
        try:
            for ns in levels:
                R.eng.conn.set_engine_cost(ns)
                R.interleaved("engine", c, a.reps or 5, seed=100 + ns, label=ns)
        finally:
            R.eng.conn.set_engine_cost(0)
    elif a.stage == "cpus":
        c = [cfg(f"DW:{m}", DEEPWATER, m) for m in ("filter", "filter+proj", "filter+proj+aggr")]
        c += [cfg(f"DWH:{m}", DEEPWATER_HEAVY, m) for m in ("filter+proj", "filter+proj+aggr")]
        c += [cfg(f"TQ:{m}", TPCH_Q1, m) for m in ("filter", "filter+proj", "filter+proj+aggr")]
        R.interleaved("cpus", c, a.reps or 5, seed=int(a.label or 0), label=a.label)
    else:
        raise SystemExit(f"stage không hợp lệ: {a.stage}")
    log(f"{a.stage}: xong")


if __name__ == "__main__":
    main()
