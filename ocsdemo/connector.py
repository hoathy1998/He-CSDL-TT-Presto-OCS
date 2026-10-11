"""OCS connector phía compute (tương ứng PageSourceProvider + Substrait Plan Generator/Sender +
Execution Result Receiver ở Hình 4 của bài báo).

Hỗ trợ cụm OCS nhiều storage node (G2.2): mỗi object được gán cố định cho một node theo
crc32(tên file) % N (mô phỏng data placement); connector đóng vai frontend, gửi plan Substrait tới đúng
node giữ object. Lệnh điều khiển (băng thông, đĩa, engine) áp dụng cho mọi node; thống kê được cộng dồn.
"""
import json
import os
import threading
import uuid
import zlib
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.flight as fl
import pyarrow.parquet as pq


class OcsConnector:
    def __init__(self, addr=None, parallelism=4):
        addr = addr or os.environ.get("OCS_ADDR", "grpc://ocs:8815")
        self.addrs = list(addr) if isinstance(addr, (list, tuple)) else [a for a in addr.split(",") if a]
        self.parallelism = parallelism          # số yêu cầu đồng thời MỖI node
        self._tls = threading.local()

    @property
    def n_nodes(self):
        return len(self.addrs)

    def _client(self, node=0):
        cs = getattr(self._tls, "clients", None)
        if cs is None:
            cs = self._tls.clients = {}
        if node not in cs:
            cs[node] = fl.FlightClient(self.addrs[node])
        return cs[node]

    def node_of(self, split):
        return zlib.crc32(split["file"].encode()) % self.n_nodes

    def action(self, name, body=None, node=0):
        res = list(self._client(node).do_action(fl.Action(name, json.dumps(body or {}).encode())))
        return res[0].body.to_pybytes()

    def _all(self, name, body=None):
        return [json.loads(self.action(name, body, n)) for n in range(self.n_nodes)]

    # metastore (node 0 giữ catalog) + điều khiển phần cứng giả lập (mọi node)
    def catalog(self):
        return json.loads(self.action("catalog"))

    def tx_bytes(self):
        return sum(r["tx_bytes"] for r in self._all("net_stats"))

    def set_net(self, mbit):
        return self._all("set_net", {"mbit": mbit})[0]

    def set_media(self, mbps):
        return self._all("set_media", {"mbps": mbps})[0]

    def set_engine_cost(self, ns):
        return self._all("set_engine_cost", {"ns": ns})[0]

    def config(self):
        cfgs = self._all("config")
        c = dict(cfgs[0])
        c["nodes"] = self.n_nodes
        c["threads_per_node"] = c["threads"]
        c["threads"] = sum(x["threads"] for x in cfgs)          # tổng CPU tầng lưu trữ
        return c

    def req_stats(self, rid):
        out = []
        for n in range(self.n_nodes):
            out += json.loads(self.action("req_stats", {"rid": rid}, n))
        return out

    # ---------------- đường dữ liệu ----------------
    def _fetch(self, split, kind, columns, plan_hex, out_schema, rid, ipc_compression=False, sim_units=0.0,
               inject_failure=False):
        ticket = {"kind": kind, "split": split, "columns": columns, "rid": rid,
                  "ipc_compression": ipc_compression, "sim_units": sim_units, "inject_failure": inject_failure}
        if plan_hex:
            ticket["plan"] = plan_hex
        reader = self._client(self.node_of(split)).do_get(fl.Ticket(json.dumps(ticket).encode()))
        tbl = reader.read_all()
        if kind == "plan":
            return tbl, tbl.nbytes
        data = b"".join(tbl.column("chunk").to_pylist())
        if kind == "raw":
            # Như Hive connector: nhận nguyên object rồi tự giải mã Parquet ở compute
            pv = split.get("partition_values", {})
            t = pq.read_table(pa.BufferReader(data), columns=[c for c in columns if c not in pv])
            for c in columns:
                if c in pv:
                    t = t.append_column(c, pa.array([pv[c]] * t.num_rows, pa.int64()))
            return t.select(columns), len(data)
        # csv: kết quả kiểu S3 Select (theo dòng, dạng văn bản) -> phải parse lại
        t = pacsv.read_csv(pa.BufferReader(data),
                           convert_options=pacsv.ConvertOptions(column_types=out_schema))
        return t.select(out_schema.names).cast(out_schema), len(data)

    def scan(self, splits, kind, columns, plan_bytes=None, out_schema=None, ipc_compression=False,
             sim_units=0.0, inject_failure=False):
        rid = uuid.uuid4().hex
        plan_hex = plan_bytes.hex() if plan_bytes else None
        with ThreadPoolExecutor(self.parallelism * self.n_nodes) as ex:
            res = list(ex.map(lambda s: self._fetch(s, kind, columns, plan_hex, out_schema, rid,
                                                    ipc_compression, sim_units, inject_failure), splits))
        tables = [t for t, _ in res]
        payload = sum(b for _, b in res)
        return tables, payload, self.req_stats(rid)
