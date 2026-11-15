"""Presto-OCS Query Lab: trang web gõ SQL tự do trên ba bộ dữ liệu, chạy trên Presto 0.286 thật theo từng
phương pháp pushdown, so thời gian thực thi / dữ liệu OCS -> Presto / kế hoạch truy vấn / tính đúng của kết quả.

  docker compose --profile presto up -d ui      # rồi mở http://localhost:8501

Chỉ dùng thư viện chuẩn của Python (+ pyarrow.flight có sẵn trong image để đổi băng thông mạng của OCS).
Thời gian và số byte lấy từ thống kê của Presto (REST API: elapsedTimeMillis, processedBytes), cùng nguồn với
EventListener. Không ghi vào results/reliability/.
"""
import json
import os
import random
import re
import signal
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, "/app")
from ocsdemo.connector import OcsConnector  # noqa: E402

PRESTO = os.environ.get("PRESTO_URL", "http://presto:8080")
PORT = int(os.environ.get("UI_PORT", "8501"))
HERE = Path(__file__).parent
PREVIEW_ROWS = 100

METHODS = {
    "nopd": {"catalog": "ocsnopd", "schema": "default", "label": "Không pushdown",
             "desc": "TableScan chỉ chọn cột; Presto tự lọc, gom nhóm, sắp xếp"},
    "filter": {"catalog": "ocsfilter", "schema": "default", "label": "Chỉ đẩy Filter",
               "desc": "OCS chỉ lọc và chọn cột (tương đương S3 Select)"},
    "ocs": {"catalog": "ocs", "schema": "default", "label": "Presto-OCS",
            "desc": "Local Optimizer đẩy Filter / Project / Aggregation / TopN xuống OCS"},
    "hive": {"catalog": "hive", "schema": "demo", "label": "Presto gốc + Hive",
             "desc": "Presto đọc thẳng file Parquet (không qua OCS, không giới hạn mạng)"},
}

LAGHOS_PAPER = ("SELECT timestep, vertex_id, min(vertex_id) AS vid, min(x) AS min_x, min(y) AS min_y,\n"
                "       min(z) AS min_z, avg(e) AS avg_e\nFROM {t}\n"
                "WHERE x BETWEEN 0.8 AND 3.2 AND y BETWEEN 0.8 AND 3.2 AND z BETWEEN 0.8 AND 3.2\n"
                "GROUP BY timestep, vertex_id\nORDER BY avg_e, timestep, vertex_id\nLIMIT 100")
DW_PAPER = ("SELECT timestep, max((rowid % 250000) / 500) AS max_height\nFROM {t}\nWHERE v02 > 0.1\n"
            "GROUP BY timestep\nORDER BY timestep")

DATASETS = {
    "laghos": {
        "name": "Laghos", "desc": "Dữ liệu mô phỏng thủy động lực học thật của LANL; mỗi file là một timestep.",
        "tables": [("laghos_one", "1 file · 4,2 triệu dòng"),
                   ("laghos", "64 file · 268 triệu dòng · không pushdown ~100 s/lượt"),
                   ("laghos_full", "256 file · 1,07 tỉ dòng · rất chậm")],
        "hive": False,
        "samples": [
            ("Truy vấn trong bài báo (1 file)", LAGHOS_PAPER.format(t="laghos_one")),
            ("Năng lượng theo timestep, vùng mật độ cao",
             "SELECT timestep, count(*) AS n, avg(e) AS avg_e, max(rho) AS max_rho\nFROM laghos_one\n"
             "WHERE rho > 2.0\nGROUP BY timestep"),
            ("Top 20 điểm năng lượng cao nhất (TopN)",
             "SELECT vertex_id, element_id, x, y, z, e\nFROM laghos_one\nWHERE e > 3.5\n"
             "ORDER BY e DESC, vertex_id, element_id\nLIMIT 20"),
            ("Đếm điểm trong một khối (gom nhóm không GROUP BY)",
             "SELECT count(*) AS n, avg(rho) AS avg_rho\nFROM laghos_one\n"
             "WHERE x BETWEEN 0.8 AND 3.2 AND y BETWEEN 0.8 AND 3.2 AND z BETWEEN 0.8 AND 3.2"),
            ("Truy vấn trong bài báo, 64 file (chậm)", LAGHOS_PAPER.format(t="laghos")),
        ],
    },
    "deepwater": {
        "name": "Deep Water Impact", "desc": "Mô phỏng tiểu hành tinh rơi xuống biển (dữ liệu tổng hợp cùng schema), "
                                             "lưới 500×500, 16 timestep.",
        "tables": [("deepwater", "64 triệu dòng · Parquet không nén"), ("deepwater_snappy", "nén snappy"),
                   ("deepwater_gzip", "nén gzip"), ("deepwater_zstd", "nén zstd")],
        "hive": True,
        "samples": [
            ("Truy vấn trong bài báo", DW_PAPER.format(t="deepwater")),
            ("Cùng truy vấn trên dữ liệu nén zstd", DW_PAPER.format(t="deepwater_zstd")),
            ("Thống kê v03 khi v02 lớn",
             "SELECT timestep, count(*) AS n, avg(v03) AS avg_v03, max(v03) AS max_v03\nFROM deepwater\n"
             "WHERE v02 > 0.5\nGROUP BY timestep\nORDER BY timestep"),
            ("Top 20 ô có v03 lớn nhất (TopN)",
             "SELECT rowid, timestep, v02, v03\nFROM deepwater\nWHERE v02 > 0.5\n"
             "ORDER BY v03 DESC, timestep, rowid\nLIMIT 20"),
        ],
    },
    "tpch": {
        "name": "TPC-H (SF1)", "desc": "Benchmark chuẩn: bảng lineitem 6 triệu dòng, orders 1,5 triệu dòng.",
        "tables": [("lineitem_double", "6 triệu dòng · số dạng double"), ("lineitem", "6 triệu dòng · số dạng decimal"),
                   ("orders", "1,5 triệu dòng")],
        "hive": True,
        "samples": [
            ("TPC-H Q1 (gom nhóm hai pha)",
             "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty,\n"
             "       sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,\n"
             "       avg(l_quantity) AS avg_qty, avg(l_discount) AS avg_disc, count(*) AS count_order\n"
             "FROM lineitem_double\nWHERE l_shipdate <= DATE '1998-09-02'\n"
             "GROUP BY l_returnflag, l_linestatus\nORDER BY l_returnflag, l_linestatus"),
            ("TPC-H Q6 (tổng doanh thu, không GROUP BY)",
             "SELECT sum(l_extendedprice * l_discount) AS revenue\nFROM lineitem_double\n"
             "WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'\n"
             "  AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"),
            ("Có LIKE: phần LIKE phải chạy ở Presto",
             "SELECT l_shipmode, count(*) AS n, sum(l_extendedprice) AS rev\nFROM lineitem_double\n"
             "WHERE l_shipdate >= DATE '1998-06-01' AND l_comment LIKE '%special%'\n"
             "GROUP BY l_shipmode\nORDER BY l_shipmode"),
            ("Gom nhóm theo khóa nhiều giá trị (optimizer có đẩy không?)",
             "SELECT l_orderkey, sum(l_quantity) AS qty\nFROM lineitem_double\n"
             "WHERE l_shipdate >= DATE '1998-01-01'\nGROUP BY l_orderkey\nORDER BY qty DESC\nLIMIT 10"),
            ("JOIN lineitem ⋈ orders (join chạy ở Presto)",
             "SELECT o_orderpriority, count(*) AS n, sum(l_extendedprice) AS rev\n"
             "FROM lineitem_double JOIN orders ON l_orderkey = o_orderkey\n"
             "WHERE l_shipdate >= DATE '1998-06-01'\nGROUP BY o_orderpriority\nORDER BY o_orderpriority"),
        ],
    },
}


# ------------------------------------------------------------------ Presto REST
class Cancelled(Exception):
    pass


def presto(sql, catalog, schema, job=None, keep_rows=PREVIEW_ROWS):
    """Chạy một câu lệnh; trả (cột, dòng xem trước, tổng số dòng, dấu vân tay kết quả, thống kê cuối)."""
    req = urllib.request.Request(f"{PRESTO}/v1/statement", data=sql.encode(), method="POST",
                                 headers={"X-Presto-User": "querylab", "X-Presto-Catalog": catalog,
                                          "X-Presto-Schema": schema, "X-Presto-Source": "ocs-query-lab"})
    r = json.load(urllib.request.urlopen(req, timeout=60))
    cols, rows, n, fp = None, [], 0, 0
    while True:
        if "error" in r:
            raise RuntimeError(r["error"].get("message", "lỗi Presto"))
        cols = cols or ([{"name": c["name"], "type": c["type"]} for c in r.get("columns", [])] or None)
        for row in r.get("data", []):
            n += 1
            fp = (fp + hash(tuple(_norm(v) for v in row))) & 0xFFFFFFFFFFFFFFFF   # không phụ thuộc thứ tự dòng
            if len(rows) < keep_rows:
                rows.append(row)
        if job is not None:
            job.query_id = r.get("id")
        if "nextUri" not in r:
            return cols, rows, n, fp, r.get("stats", {})
        if job is not None and job.cancel:
            try:
                urllib.request.urlopen(urllib.request.Request(r["nextUri"], method="DELETE"), timeout=10)
            finally:
                raise Cancelled()
        r = json.load(urllib.request.urlopen(r["nextUri"], timeout=600))


def _norm(v):
    if isinstance(v, float):
        return f"{v:.6g}"
    return str(v)


def parse_explain(text):
    """EXPLAIN dạng chữ -> danh sách nút [(độ sâu, tên, chi tiết)] + thông tin pushdown của TableScan."""
    nodes = []
    for line in text.splitlines():
        m = re.match(r"^(\s*)- (\w+(?:\([A-Z]+\))?)(.*)$", line)
        if not m:
            continue
        depth, name, rest = len(m.group(1)) // 4, m.group(2), m.group(3)
        info = {"depth": depth, "name": name, "where": "presto"}
        if "TableHandle" in rest or name.startswith("Scan") or name == "TableScan":
            # SQL đẩy xuống có thể chứa dấu nháy (DATE '...'), nên cắt tới ", layout=" thay vì tới dấu nháy kế tiếp
            tm = re.search(r"connectorHandle='(.*?)', layout=", rest) or re.search(r"connectorHandle='([^']*)'", rest)
            handle = tm.group(1) if tm else ""
            pm = re.search(r"pushdown=\[(.*?)\] sql=\[(.*)\]$", handle)
            if pm:
                info.update(where="ocs", pushed=[p.strip() for p in pm.group(1).split("->")], sql=pm.group(2))
            elif handle.startswith("ocs:"):
                info.update(where="ocs", pushed=[], sql=None)
            else:
                info.update(where="hive")
            tbl = re.search(r"(?:ocs:|tableName=)(\w+)", handle)
            info["table"] = tbl.group(1) if tbl else ""
        nodes.append(info)
    return nodes


# ------------------------------------------------------------------ đo
class Job:
    def __init__(self, sql, methods, reps):
        self.id = uuid.uuid4().hex[:8]
        self.sql, self.methods, self.reps = sql, methods, reps
        self.state, self.cancel, self.query_id = "running", False, None
        self.log, self.error = [], None
        self.res = {m: {"samples": [], "error": None, "plan": None, "preview": None} for m in methods}
        self.done_runs, self.total_runs = 0, 0
        self.started = time.time()

    def say(self, msg):
        self.log.append(f"{time.strftime('%H:%M:%S')}  {msg}")

    def to_json(self):
        out = {"id": self.id, "state": self.state, "error": self.error, "log": self.log[-12:],
               "done": self.done_runs, "total": self.total_runs, "sql": self.sql, "reps": self.reps,
               "net_mbit": NET["mbit"], "methods": {}}
        fps = {}
        for m, r in self.res.items():
            s = r["samples"]
            el = [x["elapsed_ms"] for x in s]
            d = {"label": METHODS[m]["label"], "desc": METHODS[m]["desc"], "error": r["error"], "plan": r["plan"],
                 "samples": s, "preview": r["preview"]}
            if el:
                d.update(median_ms=statistics.median(el), min_ms=min(el), max_ms=max(el),
                         bytes=statistics.median(x["bytes"] for x in s), rows_in=statistics.median(x["rows"] for x in s))
            if r["preview"]:
                fps[m] = r["preview"]["fingerprint"]
            out["methods"][m] = d
        out["same_result"] = (len(set(fps.values())) == 1) if len(fps) >= 2 else None
        return out


JOBS, RUN_LOCK = {}, threading.Lock()


def run_job(job):
    try:
        for m in job.methods:
            meth = METHODS[m]
            job.say(f"EXPLAIN – {meth['label']}")
            try:
                cols, rows, *_ = presto("EXPLAIN " + job.sql, meth["catalog"], meth["schema"], job)
                job.res[m]["plan"] = {"text": rows[0][0], "nodes": parse_explain(rows[0][0])}
            except Cancelled:
                raise
            except Exception as e:  # noqa: BLE001 - lỗi riêng của phương pháp này (vd. bảng không có trong Hive)
                job.res[m]["error"] = str(e)[:400]
                job.say(f"✗ {meth['label']}: {job.res[m]['error'][:120]}")
        ok = [m for m in job.methods if not job.res[m]["error"]]
        if not ok:
            raise RuntimeError(job.res[job.methods[0]]["error"])
        job.total_runs = len(ok) * (job.reps + 1)
        rounds = [("khởi động (không tính)", True)] + [(f"lượt {i + 1}/{job.reps}", False) for i in range(job.reps)]
        for name, warm in rounds:
            order = ok[:]
            random.shuffle(order)          # xen kẽ ngẫu nhiên thứ tự các phương pháp trong mỗi lượt
            for m in order:
                meth = METHODS[m]
                job.say(f"{name} – {meth['label']} ...")
                try:
                    cols, rows, n, fp, st = presto(job.sql, meth["catalog"], meth["schema"], job)
                except Cancelled:
                    raise
                except Exception as e:  # noqa: BLE001
                    job.res[m]["error"] = str(e)[:400]
                    job.say(f"✗ {meth['label']}: {job.res[m]['error'][:120]}")
                    job.done_runs += 1
                    continue
                job.done_runs += 1
                job.res[m]["preview"] = {"columns": cols, "rows": rows, "total": n, "fingerprint": f"{fp:016x}"}
                if not warm:
                    job.res[m]["samples"].append({"elapsed_ms": st.get("elapsedTimeMillis", 0),
                                                  "bytes": st.get("processedBytes", 0),
                                                  "rows": st.get("processedRows", 0), "query_id": job.query_id})
                job.say(f"✓ {meth['label']}: {st.get('elapsedTimeMillis', 0) / 1000:.2f} s"
                        + ("  (khởi động)" if warm else ""))
        job.state = "done"
        job.say(f"xong sau {time.time() - job.started:.0f} s")
    except Cancelled:
        job.state = "cancelled"
        job.say("đã dừng")
    except Exception as e:  # noqa: BLE001
        job.state, job.error = "error", str(e)[:600]
        job.say("✗ " + job.error[:200])
    finally:
        RUN_LOCK.release()


# ------------------------------------------------------------------ băng thông mạng OCS
NET = {"mbit": None}
_conn = OcsConnector()


def net_get():
    try:
        NET["mbit"] = _conn.config().get("net_mbit")
    except Exception:  # noqa: BLE001
        pass
    return NET["mbit"]


def net_set(mbit):
    _conn.set_net(float(mbit))
    NET["mbit"] = float(mbit)
    return NET["mbit"]


COLS_CACHE = {}


def columns_of(table):
    if table not in COLS_CACHE:
        try:
            _, rows, *_ = presto(f"SHOW COLUMNS FROM {table}", "ocs", "default")
            COLS_CACHE[table] = [{"name": r[0], "type": r[1]} for r in rows]
        except Exception:  # noqa: BLE001 - Presto chưa sẵn sàng: lần sau thử lại
            return []
    return COLS_CACHE[table]


def meta():
    ds = {}
    for k, d in DATASETS.items():
        ds[k] = {"name": d["name"], "desc": d["desc"], "hive": d["hive"],
                 "tables": [{"name": t, "note": note, "columns": columns_of(t)} for t, note in d["tables"]],
                 "samples": [{"title": a, "sql": b} for a, b in d["samples"]]}
    return {"datasets": ds, "methods": {k: {"label": v["label"], "desc": v["desc"]} for k, v in METHODS.items()},
            "net_mbit": net_get(), "busy": RUN_LOCK.locked()}


def check_sql(sql):
    sql = sql.strip().rstrip(";").strip()
    if not sql:
        raise ValueError("Chưa nhập câu SQL")
    if ";" in sql:
        raise ValueError("Mỗi lần chỉ chạy một câu SQL")
    if not re.match(r"(?is)^\s*(select|with)\b", sql):
        raise ValueError("Chỉ hỗ trợ câu truy vấn SELECT / WITH")
    if re.search(r"\b(ocs|ocsfilter|ocsnopd|hive)\.\w+\.", sql, re.I):
        raise ValueError("Không cần tiền tố catalog: viết thẳng tên bảng, chương trình tự đổi theo phương pháp")
    return sql


# ------------------------------------------------------------------ HTTP
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        data = body if isinstance(body, bytes) else json.dumps(body, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        path, _, qs = self.path.partition("?")
        if path in ("/", "/index.html"):
            return self._send(200, (HERE / "index.html").read_bytes(), "text/html; charset=utf-8")
        if path == "/api/meta":
            return self._send(200, meta())
        if path == "/api/job":
            job = JOBS.get(dict(p.split("=", 1) for p in qs.split("&") if "=" in p).get("id", ""))
            return self._send(200, job.to_json()) if job else self._send(404, {"error": "không tìm thấy"})
        if path == "/api/net":
            return self._send(200, {"net_mbit": net_get()})
        self._send(404, {"error": "not found"})

    def do_POST(self):
        try:
            b = self._body()
            if self.path == "/api/run":
                sql = check_sql(b.get("sql", ""))
                methods = [m for m in b.get("methods", []) if m in METHODS]
                if not methods:
                    raise ValueError("Chọn ít nhất một phương pháp")
                reps = max(1, min(int(b.get("reps", 3)), 10))
                if not RUN_LOCK.acquire(blocking=False):
                    return self._send(409, {"error": "Đang chạy một truy vấn khác, hãy đợi hoặc bấm Dừng"})
                job = Job(sql, methods, reps)
                JOBS[job.id] = job
                threading.Thread(target=run_job, args=(job,), daemon=True).start()
                return self._send(200, {"id": job.id})
            if self.path == "/api/cancel":
                job = JOBS.get(b.get("id", ""))
                if job:
                    job.cancel = True
                return self._send(200, {"ok": True})
            if self.path == "/api/net":
                if RUN_LOCK.locked():
                    return self._send(409, {"error": "Không đổi băng thông khi đang đo"})
                return self._send(200, {"net_mbit": net_set(b["mbit"])})
            self._send(404, {"error": "not found"})
        except ValueError as e:
            self._send(400, {"error": str(e)})
        except Exception as e:  # noqa: BLE001
            self._send(500, {"error": str(e)[:400]})


def shutdown(*_):
    """Khi tắt trang: trả mạng OCS về 1 Gbps (cấu hình chuẩn của mọi thí nghiệm)."""
    try:
        net_set(1000)
    finally:
        os._exit(0)


# Bảng Hive (file metastore) trỏ vào cùng thư mục Parquet với OCS. Metastore nằm trong container presto nên mất
# mỗi khi container được tạo lại; trang tự đăng ký lại để phương pháp "Presto gốc + Hive" luôn chạy được.
HIVE_DIRS = {"lineitem": "tpch", "lineitem_double": "tpch_double", "deepwater": "deepwater_none",
             "deepwater_snappy": "deepwater_snappy", "deepwater_gzip": "deepwater_gzip",
             "deepwater_zstd": "deepwater_zstd", "orders": "tpch_orders"}


def ensure_hive_tables(wait_s=300):
    t0 = time.time()
    while True:
        try:
            presto("CREATE SCHEMA IF NOT EXISTS hive.demo", "hive", "default")
            break
        except Exception as e:  # noqa: BLE001 - Presto chưa sẵn sàng
            if time.time() - t0 > wait_s:
                print(f"không tạo được schema hive.demo: {e}", flush=True)
                return
            time.sleep(5)
    for t, d in HIVE_DIRS.items():
        try:
            _, rows, *_ = presto(f"DESCRIBE ocs.default.{t}", "ocs", "default")
            cols = ", ".join(f"{r[0]} {r[1]}" for r in rows)
            presto(f"CREATE TABLE IF NOT EXISTS hive.demo.{t} ({cols}) "
                   f"WITH (format = 'PARQUET', external_location = 'file:///data/{d}')", "hive", "demo")
        except Exception as e:  # noqa: BLE001
            print(f"không tạo được bảng hive.demo.{t}: {e}", flush=True)
    print("đã đăng ký bảng Hive: " + ", ".join(HIVE_DIRS), flush=True)


def main():
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    threading.Thread(target=ensure_hive_tables, daemon=True).start()
    print(f"Presto-OCS Query Lab: http://localhost:{PORT}  (Presto {PRESTO})", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
