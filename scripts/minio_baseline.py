"""G4b: baseline MinIO Select THẬT so với OCS giả lập.

MinIO Select (S3 SelectObjectContent) chỉ đẩy được WHERE + chọn cột, trả CSV theo dòng; phần còn lại
(aggregation, top-N, sort) chạy ở compute như khi Presto đọc qua Hive connector. So sánh với OCS giả lập
cùng điều kiện: 2 CPU storage, mạng 1 Gbps (tc), KHÔNG giới hạn đọc đĩa ở cả hai bên.

  docker compose --profile minio up -d minio minio-tc
  docker compose --profile prep run --rm prep python scripts/minio_baseline.py upload
  docker compose exec compute python scripts/minio_baseline.py bench [--reps 5]
"""
import argparse
import io
import json
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import datafusion as df
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from botocore.config import Config

ENDPOINT, BUCKET = "http://minio:9000", "ocs"
DATA = os.environ.get("OCS_DATA", "/data")
OUT = "/app/results/reliability/minio.json"
UPLOAD = {"laghos": 16, "deepwater_none": 16, "tpch_double": 8}


def s3():
    return boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id="ocsdemo",
                        aws_secret_access_key="ocsdemo-secret", region_name="us-east-1",
                        config=Config(max_pool_connections=32, retries={"max_attempts": 2}))


def upload():
    c = s3()
    if BUCKET not in [b["Name"] for b in c.list_buckets().get("Buckets", [])]:
        c.create_bucket(Bucket=BUCKET)
    for d, n in UPLOAD.items():
        files = sorted(f for f in os.listdir(f"{DATA}/{d}") if f.endswith(".parquet"))[:n]
        for f in files:
            c.upload_file(f"{DATA}/{d}/{f}", BUCKET, f"{d}/{f}")
        print(f"upload {d}: {len(files)} object", flush=True)


# ------------------------------------------------------------------ bench
def specs():
    from ocsdemo.queries import DEEPWATER, LAGHOS, TPCH_Q1, with_table
    return [
        ("L", LAGHOS, 16, "laghos_16",
         "SELECT s.vertex_id, s.x, s.y, s.z, s.e FROM S3Object s WHERE s.x BETWEEN 0.8 AND 3.2 "
         "AND s.y BETWEEN 0.8 AND 3.2 AND s.z BETWEEN 0.8 AND 3.2"),
        ("DW", DEEPWATER, None, "deepwater", "SELECT s.rowid, s.timestep FROM S3Object s WHERE s.v02 > 0.1"),
        ("TQ", with_table(TPCH_Q1, "lineitem_double"), None, "tpch_q1_double",
         "SELECT s.l_returnflag, s.l_linestatus, s.l_quantity, s.l_extendedprice, s.l_discount, s.l_tax "
         # MinIO Select: cột date của Parquet không so sánh trực tiếp được với TIMESTAMP, và khi đổi sang chuỗi thì
         # ngày mùng 1 bị in thành "1998-09T" (mất phần ngày) nên so sánh chuỗi sai. Phải ép CẢ HAI vế về TIMESTAMP.
         "FROM S3Object s WHERE CAST(s.l_shipdate AS TIMESTAMP) < CAST('1998-09-03T' AS TIMESTAMP)"),
    ]


def minio_select(client, split, sql):
    resp = client.select_object_content(
        Bucket=BUCKET, Key=split["file"].replace("deepwater/", "deepwater_none/"), ExpressionType="SQL",
        Expression=sql, InputSerialization={"Parquet": {}}, OutputSerialization={"CSV": {}})
    buf, returned = io.BytesIO(), None
    for ev in resp["Payload"]:
        if "Records" in ev:
            buf.write(ev["Records"]["Payload"])
        elif "Stats" in ev:
            returned = ev["Stats"]["Details"].get("BytesReturned")
    return buf.getvalue(), returned


def run_minio(eng, q, max_splits, sql, client):
    """Hive connector + MinIO Select: đẩy WHERE + chọn cột, phần còn lại chạy ở compute."""
    from ocsdemo import plan as P
    from ocsdemo.catalog import schema_of
    from ocsdemo.optimizer import agg_is_complete
    t0, wall = time.perf_counter(), time.time()
    entry = eng.catalog[q.table]
    splits = entry["splits"][:max_splits] if max_splits else entry["splits"]
    full = schema_of(entry)
    filt = q.ops[0]
    phys = [c for c in filt.output if c != entry.get("partition_column")]
    types = {c: full.field(c).type for c in phys}
    with ThreadPoolExecutor(4) as ex:
        res = list(ex.map(lambda s: minio_select(client, s, sql), splits))
    tables, payload = [], 0
    for (data, _), sp in zip(res, splits):
        payload += len(data)
        t = pacsv.read_csv(pa.BufferReader(data), read_options=pacsv.ReadOptions(column_names=phys),
                           convert_options=pacsv.ConvertOptions(column_types=types)) if data else \
            pa.Table.from_pylist([], schema=pa.schema([(c, types[c]) for c in phys]))
        for k, v in sp.get("partition_values", {}).items():
            t = t.append_column(k, pa.array([v] * t.num_rows, pa.int64()))
        tables.append(t.select(filt.output))
    residual = P.compile(q, 1, agg_is_complete(q, dict(entry, splits=splits)))[1]
    ctx = df.SessionContext()
    batches = [b for t in tables for b in t.to_batches()] or [pa.RecordBatch.from_pylist([], schema=tables[0].schema)]
    ctx.register_record_batches("t", [batches])
    result = ctx.sql(residual).to_arrow_table()
    return result, time.perf_counter() - t0, payload, wall


def bench(reps):
    sys.path.insert(0, "/app")
    from ocsdemo.engine import Engine, same_result
    eng = Engine()
    eng.conn.set_net(1000)
    eng.conn.set_media(0)          # công bằng với MinIO (không có bộ giới hạn đọc đĩa)
    client = s3()
    oracles = {k: pq.read_table(f"/app/results/oracle/{k}.parquet") for _, _, _, k, _ in specs()}
    cfgs = []
    for p, q, ms, ok, sql in specs():
        cfgs += [(f"{p}:minio-select", q, ms, ok, sql, None), (f"{p}:ocs-filter-csv", q, ms, ok, None, "csv"),
                 (f"{p}:ocs-filter", q, ms, ok, None, "filter"), (f"{p}:ocs-all", q, ms, ok, None, "all")]

    def once(c):
        name, q, ms, ok, sql, mode = c
        if sql:
            result, t, payload, wall = run_minio(eng, q, ms, sql, client)
            wire = payload
        else:
            r = eng.run(q, "filter" if mode == "csv" else mode, kind_filter_csv=(mode == "csv"),
                        max_splits=ms, use_history=False, record=False)
            result, t, payload, wall, wire = r.result, r.total_s, r.payload_bytes, r.started, r.wire_bytes
        return {"stage": "minio", "label": None, "cfg": name, "query": q.name, "time_s": t,
                "payload_bytes": payload, "wire_bytes": wire, "started": wall, "ended": time.time(),
                "storage": {"media_bytes": 0}, "correct": same_result(oracles[ok], result)}

    try:
        for c in cfgs:                                   # khởi động + kiểm tra đúng
            s = once(c)
            print(f"warm-up {s['cfg']:20s} {s['time_s']:6.2f}s payload={s['payload_bytes'] / 1e6:8.1f}MB "
                  f"đúng={s['correct']}", flush=True)
        rng, samples = random.Random(41), []
        for rep in range(reps):
            order = cfgs[:]
            rng.shuffle(order)
            for i, c in enumerate(order):
                s = once(c)
                s.update(rep=rep, order=i)
                samples.append(s)
            print(f"vòng {rep + 1}/{reps} xong", flush=True)
            json.dump(samples, open(OUT, "w"), indent=1)
    finally:
        eng.conn.set_media(500)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["upload", "bench"])
    ap.add_argument("--reps", type=int, default=5)
    a = ap.parse_args()
    upload() if a.cmd == "upload" else bench(a.reps)
