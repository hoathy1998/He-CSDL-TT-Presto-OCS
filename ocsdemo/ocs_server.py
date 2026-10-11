"""OCS giả lập (Object-based Computational Storage).

Vai trò tương ứng trong bài báo (Hình 4): frontend + storage node của OCS.
  - Giao diện gRPC (Arrow Flight) nhận Substrait plan  -> "OCS Pushdown API (gRPC)"
  - Engine SQL nhúng (DataFusion) thực thi plan ngay tại nơi lưu dữ liệu
  - Trả kết quả dạng Apache Arrow (columnar)

Ba kiểu yêu cầu DoGet (ticket JSON):
  raw   : không pushdown - gửi nguyên file/object (như Hive connector đọc object thường)
  plan  : thực thi Substrait plan, trả Arrow                        (OCS)
  csv   : thực thi Substrait plan (chỉ filter+project), trả CSV theo dòng (giống S3 Select)

Mô phỏng phần cứng:
  - CPU node lưu trữ bị giới hạn bởi docker (cpus) + số luồng DataFusion/Arrow
  - Băng thông đọc thiết bị (media) giới hạn bằng token bucket dùng chung
  - Băng thông mạng ra giới hạn bằng `tc tbf` trên eth0 (đổi được lúc chạy qua action set_net)
"""
import base64
import json
import os
import subprocess
import threading
import time

import datafusion as df
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.flight as fl
import pyarrow.parquet as pq
from datafusion import substrait as ss

DATA = os.environ.get("OCS_DATA", "/data")
THREADS = int(os.environ.get("OCS_THREADS", "2"))
CHUNK = 1 << 20  # 1 MiB / batch khi gửi dữ liệu thô
BATCH_ROWS = 65536

pa.set_cpu_count(THREADS)
pa.set_io_thread_count(THREADS)


class MediaLimiter:
    """Giới hạn băng thông đọc của thiết bị lưu trữ (dùng chung cho mọi yêu cầu)."""

    def __init__(self, mbps):
        self.lock = threading.Lock()
        self.next_free = time.perf_counter()
        self.set(mbps)

    def set(self, mbps):
        self.mbps = float(mbps)

    def consume(self, nbytes):
        if self.mbps <= 0 or nbytes <= 0:
            return
        with self.lock:
            start = max(time.perf_counter(), self.next_free)
            self.next_free = start + nbytes / (self.mbps * 1e6)
            wake = self.next_free
        delay = wake - time.perf_counter()
        if delay > 0:
            time.sleep(delay)


MEDIA = MediaLimiter(os.environ.get("OCS_MEDIA_MBPS", "500"))

# Mô phỏng engine SQL nhúng CHẬM hơn DataFusion (thí nghiệm G2.3): sau khi thực thi, cộng thêm
#   in_rows × sim_units × ENGINE_NS ns
# với sim_units = tổng trọng số chi phí của các toán tử được đẩy xuống (client gửi theo plan).
# Semaphore với số slot = số CPU storage: phần xử lý cộng thêm tranh chấp CPU như công việc thật.
ENGINE = {"ns": float(os.environ.get("OCS_ENGINE_NS", "0"))}
ENGINE_SLOTS = threading.Semaphore(THREADS)

# Hàng đợi yêu cầu của storage node: tối đa OCS_MAX_CONCURRENT yêu cầu được đọc/thực thi/truyền cùng lúc
# (các yêu cầu khác chờ), tránh hết bộ nhớ khi Presto mở rất nhiều split đồng thời. Mặc định 8 > độ song song
# 4 mà engine thu nhỏ dùng, nên không ảnh hưởng các thí nghiệm trước đó.
REQUEST_SLOTS = threading.Semaphore(int(os.environ.get("OCS_MAX_CONCURRENT", "8")))




class ThrottledFile:
    """File-like cho pyarrow: mỗi byte đọc từ 'thiết bị' đều đi qua MediaLimiter.
    Parquet chỉ đọc các column chunk cần thiết, nên column pruning + nén giảm I/O thật sự."""

    def __init__(self, path):
        self.f = open(path, "rb")
        self.nread = 0

    def read(self, n=-1):
        b = self.f.read(n)
        self.nread += len(b)
        MEDIA.consume(len(b))
        return b

    def seek(self, off, whence=0):
        return self.f.seek(off, whence)

    def tell(self):
        return self.f.tell()

    def close(self):
        self.f.close()

    @property
    def closed(self):
        return self.f.closed

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False


def set_net(mbit):
    """Giới hạn băng thông mạng ra (egress) của node OCS. mbit<=0: bỏ giới hạn."""
    subprocess.run(["tc", "qdisc", "del", "dev", "eth0", "root"], capture_output=True)
    if mbit and mbit > 0:
        burst = max(64 * 1024, int(mbit * 1e6 / 8 / 250))
        subprocess.run(["tc", "qdisc", "add", "dev", "eth0", "root", "tbf", "rate", f"{mbit}mbit",
                        "burst", str(burst), "latency", "100ms"], check=True)
    return mbit


def tx_bytes():
    with open("/sys/class/net/eth0/statistics/tx_bytes") as fh:
        return int(fh.read())


def _read_split(split, columns):
    """Đọc các cột vật lý cần thiết của một object + gắn cột partition (hằng số theo split)."""
    pv = split.get("partition_values", {})
    phys = [c for c in columns if c not in pv]
    tf = ThrottledFile(os.path.join(DATA, split["file"]))
    try:
        tbl = pq.read_table(pa.PythonFile(tf, mode="r"), columns=phys, use_threads=True)
    finally:
        tf.close()
    for c in columns:
        if c in pv:
            tbl = tbl.append_column(c, pa.array([pv[c]] * tbl.num_rows, pa.int64()))
    return tbl.select(columns), tf.nread


def _execute_plan(table, plan_bytes):
    cfg = df.SessionConfig().with_target_partitions(THREADS)
    ctx = df.SessionContext(cfg)
    ctx.register_record_batches("t", [table.to_batches(max_chunksize=BATCH_ROWS)])
    plan = ss.Serde.deserialize_bytes(plan_bytes)
    lp = ss.Consumer.from_substrait_plan(ctx, plan)
    return ctx.create_dataframe_from_logical_plan(lp).to_arrow_table()


def _chunks(data: bytes):
    schema = pa.schema([("chunk", pa.binary())])
    for i in range(0, len(data), CHUNK):
        yield pa.record_batch([pa.array([data[i:i + CHUNK]], pa.binary())], schema=schema)


class OcsServer(fl.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)
        self.req_stats = {}
        self.plan_stats = {}   # R2: số yêu cầu chạy kế hoạch Substrait từ Presto / SQL dự phòng
        self.lock = threading.Lock()

    # ---------------- DoGet: đường dữ liệu ----------------
    def do_get(self, context, ticket):
        # Hàng đợi chỉ giới hạn pha ĐỌC + THỰC THI (pha tốn bộ nhớ); kết quả vẫn gửi bằng đường C++ nhanh
        # (RecordBatchStream) như các thí nghiệm trước, để không làm thay đổi hiệu năng đo được.
        with REQUEST_SLOTS:
            schema, batches, options = self._serve(json.loads(ticket.ticket.decode()))
        if isinstance(batches, list):
            table = pa.Table.from_batches(batches, schema=schema)
            return fl.RecordBatchStream(table, options=options) if options else fl.RecordBatchStream(table)
        return fl.GeneratorStream(schema, batches)

    def _serve(self, req):
        """Đọc + thực thi một yêu cầu; trả (schema, các batch, tùy chọn IPC)."""
        kind, split = req["kind"], req["split"]
        t0 = time.perf_counter()
        st = {"kind": kind, "file": split["file"]}
        if kind == "raw":
            tf = ThrottledFile(os.path.join(DATA, split["file"]))
            data = tf.read()
            tf.close()
            st.update(read_s=time.perf_counter() - t0, media_bytes=len(data), exec_s=0.0,
                      out_rows=0, out_bytes=len(data))
            self._save(req, st)
            return pa.schema([("chunk", pa.binary())]), _chunks(data), None

        if req.get("inject_failure"):  # dùng để demo/kiểm thử cơ chế fallback của connector
            raise fl.FlightServerError("OCS: lỗi thực thi pushdown (giả lập)")
        table, nread = _read_split(split, req["columns"])
        t1 = time.perf_counter()
        result = None
        if kind == "sql" and req.get("substrait"):
            # R2: kế hoạch Substrait do connector Presto (Java, substrait-java/isthmus) sinh ra, như trong bài báo.
            # Connector đặt Calcite giữ nguyên chữ thường; tên cột ra (Calcite tự đặt) được đổi lại theo alias c0..cN.
            try:
                result = _execute_plan(table, base64.b64decode(req["substrait"]))
                if req.get("out_names") and len(req["out_names"]) == result.num_columns:
                    result = result.rename_columns(req["out_names"])
                st["plan_from"] = "presto-substrait"
            except Exception as e:  # noqa: BLE001 - kế hoạch không tương thích -> dùng SQL dự phòng
                st["substrait_error"] = str(e)[:300]
        if result is None and kind == "sql":
            # Đường dự phòng (và mặc định trước R2): nhận SQL đã dựng lại, dịch sang Substrait ngay tại OCS
            pctx = df.SessionContext()
            pctx.register_record_batches("t", [table.to_batches(max_chunksize=BATCH_ROWS) or
                                               [pa.RecordBatch.from_pylist([], schema=table.schema)]])
            result = _execute_plan(table, ss.Serde.serialize_bytes(req["sql"], pctx))
            st.setdefault("plan_from", "ocs-sql")
        elif result is None:
            result = _execute_plan(table, bytes.fromhex(req["plan"]))
        sim_s = table.num_rows * float(req.get("sim_units", 0)) * ENGINE["ns"] * 1e-9
        if sim_s > 0:
            with ENGINE_SLOTS:
                time.sleep(sim_s)
        st["sim_s"] = sim_s
        t2 = time.perf_counter()
        st.update(read_s=t1 - t0, exec_s=t2 - t1, media_bytes=nread, in_rows=table.num_rows,
                  out_rows=result.num_rows, out_bytes=result.nbytes)
        if kind == "csv":
            sink = pa.BufferOutputStream()
            pacsv.write_csv(result, sink)
            data = sink.getvalue().to_pybytes()
            st.update(csv_s=time.perf_counter() - t2, out_bytes=len(data))
            self._save(req, st)
            return pa.schema([("chunk", pa.binary())]), _chunks(data), None
        self._save(req, st)
        batches = result.to_batches(max_chunksize=BATCH_ROWS)
        if req.get("ipc_compression"):  # ablation: nén Arrow IPC (zstd) trước khi gửi qua mạng
            return result.schema, batches, pa.ipc.IpcWriteOptions(compression="zstd")
        return result.schema, batches, None

    def _save(self, req, st):
        if st.get("plan_from") or st.get("substrait_error"):
            with self.lock:
                k = st.get("plan_from", "?")
                self.plan_stats[k] = self.plan_stats.get(k, 0) + 1
                if st.get("substrait_error"):
                    self.plan_stats["last_substrait_error"] = st["substrait_error"]
        if req.get("rid"):
            with self.lock:
                self.req_stats.setdefault(req["rid"], []).append(st)

    # ---------------- Actions: điều khiển / metadata ----------------
    def list_actions(self, context):
        return [("catalog", "catalog.json (metastore)"), ("net_stats", "tx_bytes eth0"),
                ("set_net", "{mbit}"), ("set_media", "{mbps}"), ("req_stats", "{rid}"),
                ("config", "cấu hình hiện tại")]

    def do_action(self, context, action):
        body = json.loads(action.body.to_pybytes() or b"{}")
        t = action.type
        if t == "catalog":
            with open(os.path.join(DATA, "catalog.json"), "rb") as fh:
                out = fh.read()
        elif t == "net_stats":
            out = json.dumps({"tx_bytes": tx_bytes()}).encode()
        elif t == "set_net":
            out = json.dumps({"net_mbit": set_net(float(body["mbit"]))}).encode()
            self.net_mbit = float(body["mbit"])
        elif t == "set_media":
            MEDIA.set(body["mbps"])
            out = json.dumps({"media_mbps": MEDIA.mbps}).encode()
        elif t == "set_engine_cost":
            ENGINE["ns"] = float(body["ns"])
            out = json.dumps({"engine_ns": ENGINE["ns"]}).encode()
        elif t == "req_stats":
            with self.lock:
                out = json.dumps(self.req_stats.pop(body["rid"], [])).encode()
        elif t == "plan_stats":
            with self.lock:
                out = json.dumps(self.plan_stats).encode()
                if body.get("reset"):
                    self.plan_stats = {}
        elif t == "config":
            out = json.dumps({"threads": THREADS, "net_mbit": self.net_mbit,
                              "media_mbps": MEDIA.mbps, "engine_ns": ENGINE["ns"]}).encode()
        else:
            raise fl.FlightServerError(f"unknown action {t}")
        yield fl.Result(out)


def main():
    server = OcsServer("grpc://0.0.0.0:8815")
    server.net_mbit = float(os.environ.get("OCS_NET_MBIT", "1000"))
    set_net(server.net_mbit)
    print(f"OCS emulator: threads={THREADS} net={server.net_mbit}Mbit media={MEDIA.mbps}MB/s", flush=True)
    server.serve()


if __name__ == "__main__":
    main()
