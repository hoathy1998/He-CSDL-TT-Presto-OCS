"""'Presto thu nhỏ' ở compute node: coordinator (planning + local optimizer) + worker (residual).

Luồng thực thi giống Mục 3.4 của bài báo:
  (1) Pushdown Target Selection  : optimizer.choose()  (Selectivity Analyzer, Operator Extractor)
  (2) Task Scheduling            : mỗi split (object) là một task đọc dữ liệu, chạy song song
  (3) Translate & Submit         : dựng lại SQL cho các toán tử đẩy xuống -> Substrait IR -> gRPC
  (4) In-Storage Execution       : OCS thực thi, trả Arrow
  (5) Result Reception & Post-processing : gộp Arrow từ các split, chạy toán tử residual
"""
import json
import os
import time
from dataclasses import dataclass, field

import datafusion as df
import pyarrow as pa
import pyarrow.flight as fl
from datafusion import substrait as ss

from . import plan as P
from .catalog import schema_of
from .connector import OcsConnector
from .optimizer import HISTORY, choose, op_weight


@dataclass
class RunResult:
    query: str
    table: str
    mode: str
    k: int
    agg_complete: bool
    result: pa.Table
    total_s: float
    timings: dict
    wire_bytes: int           # byte thực sự rời khỏi node OCS (eth0 tx_bytes)
    payload_bytes: int        # byte dữ liệu ứng dụng (Arrow / CSV / object thô)
    storage: dict
    storage_sql: str
    residual_sql: str
    substrait_bytes: int
    decision: object
    net_mbit: float
    notes: list = field(default_factory=list)
    started: float = 0.0      # epoch (giây) - để đối chiếu với log nguồn điện / sleep
    ended: float = 0.0
    pushdown_ok: bool = True  # False nếu OCS lỗi và connector phải fallback về đọc nguyên object
    fallback_reason: str = None


class Engine:
    def __init__(self, addr=None, parallelism=4):
        self.conn = OcsConnector(addr, parallelism)
        self.catalog = self.conn.catalog()

    def cluster(self):
        cfg = self.conn.config()
        nodes = cfg.get("nodes", 1)
        # nhiều storage node: mỗi node có NIC riêng -> băng thông tổng = N × băng thông mỗi node
        return {"net_mbit": cfg["net_mbit"] * nodes, "media_mbps": cfg["media_mbps"],
                "storage_threads": cfg["threads"], "compute_threads": os.cpu_count(), "nodes": nodes}

    def run(self, q: P.Query, mode="auto", kind_filter_csv=False, max_splits=None,
            use_history=True, record=True, ipc_compression=False, inject_failure=False) -> RunResult:
        wall_start = time.time()
        entry = self.catalog[q.table]
        splits = entry["splits"][:max_splits] if max_splits else entry["splits"]
        if max_splits:
            entry = dict(entry, splits=splits, rows=sum(s["rows"] for s in splits),
                         bytes=sum(s["bytes"] for s in splits))
        cluster = self.cluster()
        t_start = time.perf_counter()

        # (1) Local optimizer: chọn toán tử đẩy xuống
        t = time.perf_counter()
        dec = choose(q, entry, cluster, mode, use_history=use_history)
        t_plan = time.perf_counter() - t

        # (3a) Dựng SQL cho phần đẩy xuống và phần residual, dịch sang Substrait IR
        t = time.perf_counter()
        storage_sql, residual_sql = P.compile(q, dec.k, dec.agg_complete)
        full = schema_of(self.catalog[q.table])
        scan_schema = pa.schema([full.field(c) for c in q.columns])
        plan_bytes, out_schema = None, scan_schema
        if storage_sql:
            pctx = df.SessionContext()
            pctx.register_record_batches("t", [[pa.RecordBatch.from_pylist([], schema=scan_schema)]])
            plan_bytes = ss.Serde.serialize_bytes(storage_sql, pctx)
            out_schema = pctx.sql(storage_sql).schema()
        t_substrait = time.perf_counter() - t

        # (2)(3b)(4) Gửi plan tới OCS cho từng split, nhận Arrow
        kind = "raw" if dec.k == 0 else ("csv" if kind_filter_csv else "plan")
        if kind == "csv" and any(op.kind not in ("filter", "proj") for op in q.ops[:dec.k]):
            raise ValueError("S3 Select chỉ hỗ trợ filter + projection")
        tx0 = self.conn.tx_bytes()
        t = time.perf_counter()
        sim_units = sum(op_weight(op) for op in q.ops[:dec.k])   # cho mô phỏng engine chậm (G2.3)
        pushdown_ok, fallback_reason = True, None
        try:
            tables, payload, sstats = self.conn.scan(splits, kind, q.columns, plan_bytes, out_schema,
                                                     ipc_compression=ipc_compression and kind == "plan",
                                                     sim_units=sim_units, inject_failure=inject_failure)
        except fl.FlightError as ex:
            if dec.k == 0:
                raise
            # Fallback: OCS không thực thi được pushdown -> đọc nguyên object, chạy toàn bộ ở compute
            # (tương tự Presto quay về đường quét bảng thông thường). Ghi nhận để tính tỉ lệ thành công.
            pushdown_ok, fallback_reason = False, str(ex).splitlines()[0][:200]
            storage_sql, residual_sql = None, P.compile(q, 0, dec.agg_complete)[1]
            tables, payload, sstats = self.conn.scan(splits, "raw", q.columns)
        t_transfer = time.perf_counter() - t
        wire = self.conn.tx_bytes() - tx0

        # (5) Gộp kết quả các split + chạy toán tử residual ở compute
        t = time.perf_counter()
        ctx = df.SessionContext()
        nonempty = [b for tb in tables for b in tb.to_batches()]
        if not nonempty:
            nonempty = [pa.RecordBatch.from_pylist([], schema=tables[0].schema if tables else out_schema)]
        ctx.register_record_batches("t", [nonempty])
        result = ctx.sql(residual_sql).to_arrow_table()
        t_post = time.perf_counter() - t

        total = time.perf_counter() - t_start
        timings = {"plan_analysis": t_plan, "substrait_gen": t_substrait,
                   "pushdown_transfer": t_transfer, "post_scan": t_post}
        timings["others"] = max(total - sum(timings.values()), 0.0)
        storage = {
            "read_s": sum(s.get("read_s", 0) for s in sstats),
            "exec_s": sum(s.get("exec_s", 0) for s in sstats),
            "media_bytes": sum(s.get("media_bytes", 0) for s in sstats),
            "in_rows": sum(s.get("in_rows", 0) for s in sstats),
            "out_rows": sum(s.get("out_rows", 0) for s in sstats),
        }
        rr = RunResult(q.name, q.table, dec.mode if not kind_filter_csv else dec.mode + "(csv)", dec.k,
                       dec.agg_complete, result, total, timings, wire, payload, storage, storage_sql,
                       residual_sql, len(plan_bytes or b""), dec, cluster["net_mbit"], dec.notes,
                       started=wall_start, ended=time.time(),
                       pushdown_ok=pushdown_ok, fallback_reason=fallback_reason)
        if record:
            self._record(rr)
        return rr

    def run_join(self, jq, mode="auto", use_history=False, record=False):
        """Truy vấn nhiều bảng (G2.1): mỗi nhánh được đẩy xuống OCS độc lập theo `mode`
        ('none' | 'filter' | 'all' | 'auto'), sau đó JOIN và phần còn lại chạy ở compute như toán tử residual."""
        t0, wall_start = time.perf_counter(), time.time()
        parts, subs = {}, []
        for alias, q in jq.inputs:
            m = mode if mode in ("none", "auto", "filter") else "all"
            r = self.run(q, m, use_history=use_history, record=record)
            parts[alias] = r.result
            subs.append((alias, r))
        t = time.perf_counter()
        ctx = df.SessionContext()
        for alias, tbl in parts.items():
            batches = tbl.to_batches() or [pa.RecordBatch.from_pylist([], schema=tbl.schema)]
            ctx.register_record_batches(alias, [batches])
        result = ctx.sql(jq.final_sql).to_arrow_table()
        t_join = time.perf_counter() - t
        total = time.perf_counter() - t0
        return {
            "query": jq.name, "mode": mode, "result": result, "total_s": total, "join_s": t_join,
            "wire_bytes": sum(r.wire_bytes for _, r in subs), "payload_bytes": sum(r.payload_bytes for _, r in subs),
            "branches": {a: {"mode": r.mode, "k": r.k, "agg_complete": r.agg_complete, "time_s": r.total_s,
                             "wire_bytes": r.wire_bytes, "rows_to_compute": r.storage["out_rows"] or r.result.num_rows,
                             "storage_sql": r.storage_sql, "residual_sql": r.residual_sql}
                         for a, r in subs},
            "started": wall_start, "ended": time.time(),
            "storage": {"media_bytes": sum(r.storage["media_bytes"] for _, r in subs)},
            "timings": {"join": t_join, **{f"branch_{a}": r.total_s for a, r in subs}},
        }

    def _record(self, rr: RunResult):
        """Pushdown history (EventListener trong bài): lưu thống kê từng lần chạy."""
        os.makedirs(os.path.dirname(HISTORY), exist_ok=True)
        with open(HISTORY, "a") as fh:
            fh.write(json.dumps({"ts": time.time(), "query": rr.query, "table": rr.table, "mode": rr.mode,
                                 "k": rr.k, "total_s": rr.total_s, "wire_bytes": rr.wire_bytes,
                                 "net_mbit": rr.net_mbit, "pushdown_ok": rr.pushdown_ok,
                                 "fallback_reason": rr.fallback_reason}) + "\n")

    def pushdown_success_rate(self, window=500):
        """Tỉ lệ pushdown thành công trên cửa sổ trượt các lần chạy gần nhất (chỉ tính lần có pushdown)."""
        if not os.path.exists(HISTORY):
            return None, 0
        with open(HISTORY) as fh:
            rows = [json.loads(x) for x in fh.readlines()[-window:]]
        rows = [r for r in rows if r.get("k", 0) > 0]
        if not rows:
            return None, 0
        return sum(r.get("pushdown_ok", True) for r in rows) / len(rows), len(rows)


def normalize(tbl: pa.Table):
    """Chuẩn hóa kết quả để so sánh giữa các chế độ (decimal->double, sắp xếp dòng)."""
    cols = {}
    for name in tbl.column_names:
        c = tbl.column(name)
        if pa.types.is_decimal(c.type):
            c = c.cast(pa.float64())
        cols[name] = c.to_pylist()
    rows = list(zip(*cols.values()))
    return list(cols), sorted(rows, key=lambda r: tuple((x is None, str(type(x)), x) for x in r))


def same_result(a: pa.Table, b: pa.Table, rtol=1e-6):
    ca, ra = normalize(a)
    cb, rb = normalize(b)
    if ca != cb or len(ra) != len(rb):
        return False
    for x, y in zip(ra, rb):
        for u, v in zip(x, y):
            if isinstance(u, float) and isinstance(v, float):
                if abs(u - v) > rtol * max(1.0, abs(u), abs(v)):
                    return False
            elif u != v:
                return False
    return True
