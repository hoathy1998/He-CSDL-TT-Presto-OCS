"""Catalog = vai trò Hive Metastore trong bài báo: schema, danh sách split (file/object)
và thống kê bảng (row count, min/max, NDV) để Selectivity Analyzer ước lượng độ chọn lọc.

catalog.json do node lưu trữ giữ; node tính toán lấy về qua Flight action `catalog`
(node tính toán không bao giờ đọc file trực tiếp)."""
import base64
import datetime
import decimal
import json
import os
import re

import pyarrow as pa
import pyarrow.parquet as pq

# Bảng logic -> thư mục; partition = cột ảo lấy từ tên file (kiểu Hive partition column)
TABLES = {
    # "laghos" giữ đúng 64 file đầu (các thí nghiệm trước); "laghos_full" = toàn bộ file đã tải (R4: 256 file)
    "laghos": {"dir": "laghos", "partition": ("timestep", r"xx_(\d+)\.parquet"), "limit": 64},
    "laghos_full": {"dir": "laghos", "partition": ("timestep", r"xx_(\d+)\.parquet")},
    "deepwater": {"dir": "deepwater_none"},
    "deepwater_snappy": {"dir": "deepwater_snappy"},
    "deepwater_gzip": {"dir": "deepwater_gzip"},
    "deepwater_zstd": {"dir": "deepwater_zstd"},
    "lineitem": {"dir": "tpch"},
    "lineitem_double": {"dir": "tpch_double"},   # ablation C: decimal -> DOUBLE
    "orders": {"dir": "tpch_orders"},            # G2.1: truy vấn có join
    "laghos_one": {"dir": "laghos_one", "partition": ("timestep", r"xx_(\d+)\.parquet")},  # Q4 trên Presto: 1 file
}


def _jsonable(v):
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace")
    return v


def _width(t: pa.DataType) -> float:
    if pa.types.is_decimal(t):
        return 16
    try:
        return t.bit_width / 8
    except ValueError:
        return 8  # chuỗi: ước lượng


def build(data_dir):
    import duckdb
    con = duckdb.connect()
    cat = {}
    for name, spec in TABLES.items():
        d = os.path.join(data_dir, spec["dir"])
        if not os.path.isdir(d):
            continue
        files = sorted(f for f in os.listdir(d) if f.endswith(".parquet"))[:spec.get("limit")]
        if not files:
            continue
        part = spec.get("partition")
        schema = pq.read_schema(os.path.join(d, files[0]))
        if part:
            schema = schema.append(pa.field(part[0], pa.int64()))
        splits, colstats = [], {f.name: {"min": None, "max": None, "constant_per_split": True} for f in schema}
        for f in files:
            path = os.path.join(d, f)
            md = pq.ParquetFile(path).metadata
            pv = {}
            if part:
                pv[part[0]] = int(re.match(part[1], f).group(1))
            splits.append({"file": f"{spec['dir']}/{f}", "rows": md.num_rows,
                           "bytes": os.path.getsize(path), "partition_values": pv,
                           "codec": md.row_group(0).column(0).compression})
            for ci in range(md.num_columns):
                col = md.schema.column(ci).name
                mn = mx = None
                for rg in range(md.num_row_groups):
                    st = md.row_group(rg).column(ci).statistics
                    try:
                        smin, smax = st.min, st.max
                    except (AttributeError, pa.ArrowNotImplementedError):
                        smin = smax = None
                    if smin is None:
                        mn = mx = None
                        break
                    mn = smin if mn is None else min(mn, smin)
                    mx = smax if mx is None else max(mx, smax)
                cs = colstats[col]
                if mn is None or mn != mx:
                    cs["constant_per_split"] = False
                if mn is not None:
                    cs["min"] = mn if cs["min"] is None else min(cs["min"], mn)
                    cs["max"] = mx if cs["max"] is None else max(cs["max"], mx)
            for k, v in pv.items():
                cs = colstats[k]
                cs["min"] = v if cs["min"] is None else min(cs["min"], v)
                cs["max"] = v if cs["max"] is None else max(cs["max"], v)
        # NDV (tương đương ANALYZE ... COMPUTE STATISTICS của Hive)
        flist = ", ".join(f"'{os.path.join(d, f)}'" for f in files)
        phys = [f.name for f in schema if not (part and f.name == part[0])]
        ndv_sql = ", ".join(f'approx_count_distinct("{c}")' for c in phys)
        ndvs = con.execute(f"SELECT {ndv_sql} FROM read_parquet([{flist}])").fetchone()
        for c, n in zip(phys, ndvs):
            colstats[c]["ndv"] = int(n)
        if part:
            colstats[part[0]]["ndv"] = len(files)
        for f in schema:
            cs = colstats[f.name]
            cs["min"], cs["max"] = _jsonable(cs["min"]), _jsonable(cs["max"])
            cs["width"] = _width(f.type)
            cs["type"] = str(f.type)
        cat[name] = {
            "schema": base64.b64encode(schema.serialize().to_pybytes()).decode(),
            "partition_column": part[0] if part else None,
            "rows": sum(s["rows"] for s in splits),
            "bytes": sum(s["bytes"] for s in splits),
            "splits": splits,
            "columns": colstats,
        }
        print(f"  {name}: {len(splits)} split, {cat[name]['rows']:,} dòng, "
              f"{cat[name]['bytes'] / 1e6:,.0f} MB", flush=True)
    with open(os.path.join(data_dir, "catalog.json"), "w") as fh:
        json.dump(cat, fh, indent=1)
    return cat


def schema_of(entry) -> pa.Schema:
    return pa.ipc.read_schema(pa.py_buffer(base64.b64decode(entry["schema"])))
