"""Chuẩn bị dữ liệu cho demo (chạy trong container `prep`, ghi vào volume /data).

  laghos/      : dữ liệu Laghos THẬT của LANL (tải từ GitHub lanl-ocs/laghos-sample-dataset)
  deepwater_*/ : dữ liệu tổng hợp mô phỏng Deep Water Asteroid Impact (cùng schema/ý nghĩa),
                 ghi với 4 kiểu nén: none, snappy, gzip, zstd (thí nghiệm Q3)
  tpch/        : bảng lineitem TPC-H (DuckDB dbgen), chia thành nhiều file (split)

Sau đó xây catalog.json (vai trò Hive Metastore: schema + thống kê min/max/NDV/row count).
"""
import argparse
import os
import subprocess
import time
import urllib.error
import urllib.request

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ocsdemo import catalog

DATA = os.environ.get("OCS_DATA", "/data")
LAGHOS_URL = "https://raw.githubusercontent.com/lanl-ocs/laghos-sample-dataset/main/data/xx_{ts}.parquet.br"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def prepare_laghos(n_files):
    out = os.path.join(DATA, "laghos")
    os.makedirs(out, exist_ok=True)
    for i in range(n_files):
        ts = 36000 + 5 * i
        dst = os.path.join(out, f"xx_{ts}.parquet")
        if os.path.exists(dst):
            continue
        br = dst + ".br"
        log(f"laghos: tải {os.path.basename(br)}")
        for attempt in range(5):   # GitHub raw đôi khi cắt kết nối giữa chừng -> tải lại
            try:
                urllib.request.urlretrieve(LAGHOS_URL.format(ts=ts), br)
                break
            except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
                log(f"laghos: lỗi tải ({e}), thử lại {attempt + 1}/5")
                time.sleep(10 * (attempt + 1))
        else:
            raise RuntimeError(f"không tải được {br}")
        subprocess.run(["brotli", "-d", "-f", "-o", dst, br], check=True)
        os.remove(br)
    log(f"laghos: {n_files} file sẵn sàng")


def deepwater_timestep(t, nx=500, ny=500, nz=16, seed=0):
    """Một snapshot: lưới 500 x 500 x nz ô. rowid = k*250000 + j*500 + i.
    v02 = tỉ lệ thể tích nước, v03 = tỉ lệ vật chất thiên thạch.
    (rowid % 250000)/500 = j = chỉ số độ cao -> MAX(j | v02 > 0.1) = độ cao cột nước bắn lên."""
    rng = np.random.default_rng(seed + t)
    i = np.arange(nx, dtype=np.float64)
    k = np.arange(nz, dtype=np.float64)
    ii, kk = np.meshgrid(i, k)                          # (nz, nx)
    r = np.hypot(ii - nx / 2, (kk - nz / 2) * 20)       # khoảng cách tới tâm va chạm
    phase = t / 4.0
    surface = 170 + 6 * np.sin(ii / 23 + phase) + 4 * np.cos(kk / 3 + phase)
    splash = (25 + 18 * t) * np.exp(-((r / (40 + 6 * t)) ** 2))   # cột nước dâng theo thời gian
    height = surface + splash                           # (nz, nx)
    j = np.arange(ny, dtype=np.float64)
    d = j[None, :, None] - height[:, None, :]           # (nz, ny, nx), >0: phía trên mặt nước
    v02 = np.clip(0.5 - d / 6.0, 0.0, 1.0)
    drops = rng.random(v02.shape) < 0.004 * np.exp(-np.maximum(d, 0) / 12)
    v02 = np.where(drops & (d > 0), rng.random(v02.shape) * 0.6, v02)
    v02 = np.round(v02, 3)
    v03 = np.round(np.clip(0.3 * np.exp(-((r[:, None, :] / 30) ** 2)) * np.exp(-np.abs(d) / 40), 0, 1), 3)
    n = nx * ny * nz
    return pa.table({
        "rowid": pa.array(np.arange(n, dtype=np.int64)),
        "v02": pa.array(v02.reshape(-1)),
        "v03": pa.array(v03.reshape(-1)),
        "timestep": pa.array(np.full(n, t, dtype=np.int64)),
    })


def prepare_deepwater(n_files):
    codecs = ["none", "snappy", "gzip", "zstd"]
    for c in codecs:
        os.makedirs(os.path.join(DATA, f"deepwater_{c}"), exist_ok=True)
    for t in range(n_files):
        paths = {c: os.path.join(DATA, f"deepwater_{c}", f"ts_{t:03d}.parquet") for c in codecs}
        if all(os.path.exists(p) for p in paths.values()):
            continue
        tbl = deepwater_timestep(t)
        for c, p in paths.items():
            # "none" = dữ liệu thô: không nén, không dictionary encoding (như "raw" ở Hình 6)
            pq.write_table(tbl, p, compression=c, use_dictionary=(c != "none"), row_group_size=1 << 20)
        log(f"deepwater: timestep {t} ({tbl.num_rows:,} dòng)")


def prepare_tpch(sf, n_files):
    import duckdb
    out = os.path.join(DATA, "tpch")
    os.makedirs(out, exist_ok=True)
    if len([f for f in os.listdir(out) if f.endswith(".parquet")]) == n_files:
        return
    log(f"tpch: dbgen sf={sf}")
    con = duckdb.connect()
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"CALL dbgen(sf={sf})")
    maxkey = con.execute("SELECT max(l_orderkey) FROM lineitem").fetchone()[0]
    step = maxkey // n_files + 1
    for p in range(n_files):
        dst = os.path.join(out, f"lineitem_{p:02d}.parquet")
        con.execute(f"""COPY (SELECT * FROM lineitem
                        WHERE l_orderkey >= {p * step} AND l_orderkey < {(p + 1) * step}
                        ORDER BY l_orderkey)
                        TO '{dst}' (FORMAT PARQUET, COMPRESSION 'uncompressed', ROW_GROUP_SIZE 1048576)""")
    log(f"tpch: {n_files} file sẵn sàng")


def prepare_tpch_orders(sf, n_files=4):
    """G2.1: bảng orders (cùng hệ số quy mô) cho truy vấn có join; chia theo khoảng o_orderkey."""
    import duckdb
    out = os.path.join(DATA, "tpch_orders")
    os.makedirs(out, exist_ok=True)
    if len([f for f in os.listdir(out) if f.endswith(".parquet")]) == n_files:
        return
    con = duckdb.connect()
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"CALL dbgen(sf={sf})")
    maxkey = con.execute("SELECT max(o_orderkey) FROM orders").fetchone()[0]
    step = maxkey // n_files + 1
    for p in range(n_files):
        dst = os.path.join(out, f"orders_{p:02d}.parquet")
        con.execute(f"""COPY (SELECT * FROM orders WHERE o_orderkey >= {p * step} AND o_orderkey < {(p + 1) * step}
                        ORDER BY o_orderkey)
                        TO '{dst}' (FORMAT PARQUET, COMPRESSION 'uncompressed', ROW_GROUP_SIZE 1048576)""")
    log(f"tpch_orders: {n_files} file sẵn sàng")


def prepare_tpch_double():
    """Ablation C: cùng dữ liệu lineitem nhưng các cột decimal đổi sang DOUBLE (8 byte trong Arrow)."""
    import duckdb
    src, out = os.path.join(DATA, "tpch"), os.path.join(DATA, "tpch_double")
    os.makedirs(out, exist_ok=True)
    con = duckdb.connect()
    for f in sorted(os.listdir(src)):
        dst = os.path.join(out, f)
        if f.endswith(".parquet") and not os.path.exists(dst):
            con.execute(f"""COPY (SELECT * REPLACE (l_quantity::DOUBLE AS l_quantity,
                                   l_extendedprice::DOUBLE AS l_extendedprice,
                                   l_discount::DOUBLE AS l_discount, l_tax::DOUBLE AS l_tax)
                            FROM read_parquet('{os.path.join(src, f)}'))
                            TO '{dst}' (FORMAT PARQUET, COMPRESSION 'uncompressed', ROW_GROUP_SIZE 1048576)""")
    log("tpch_double: sẵn sàng")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--laghos-files", type=int, default=16)
    ap.add_argument("--deepwater-files", type=int, default=16)
    ap.add_argument("--tpch-sf", type=float, default=1)
    ap.add_argument("--tpch-files", type=int, default=8)
    a = ap.parse_args()
    prepare_laghos(a.laghos_files)
    prepare_deepwater(a.deepwater_files)
    prepare_tpch(a.tpch_sf, a.tpch_files)
    prepare_tpch_double()
    prepare_tpch_orders(a.tpch_sf)
    one = os.path.join(DATA, "laghos_one")      # Q4 trên Presto: truy vấn trên đúng 1 file như bài báo
    os.makedirs(one, exist_ok=True)
    if not os.listdir(one):
        import shutil
        shutil.copy(os.path.join(DATA, "laghos", "xx_36000.parquet"), one)
    log("xây catalog (thống kê kiểu Hive Metastore ANALYZE)...")
    catalog.build(DATA)
    log("xong.")


if __name__ == "__main__":
    main()
