"""Oracle độc lập: tính kết quả đúng của các truy vấn bằng DuckDB, đọc thẳng file Parquet,
KHÔNG đi qua engine/OCS/Substrait của demo. Chạy trong container `prep` (có quyền đọc /data).

  docker compose --profile prep run --rm prep python scripts/oracle.py

Ghi ra results/oracle/<key>.parquet để scripts/reliability.py so khớp.
"""
import os

import duckdb

DATA = os.environ.get("OCS_DATA", "/data")
OUT = "/app/results/oracle"


def laghos_sql(n):
    files = sorted(f for f in os.listdir(f"{DATA}/laghos") if f.endswith(".parquet"))[:n]
    lst = ", ".join(f"'{DATA}/laghos/{f}'" for f in files)
    return f"""
    SELECT timestep, vertex_id, min(vertex_id) AS vid, min(x) AS min_x, min(y) AS min_y,
           min(z) AS min_z, avg(e) AS avg_e
    FROM (SELECT *, CAST(regexp_extract(filename, 'xx_(\\d+)', 1) AS BIGINT) AS timestep
          FROM read_parquet([{lst}], filename = true))
    WHERE x BETWEEN 0.8 AND 3.2 AND y BETWEEN 0.8 AND 3.2 AND z BETWEEN 0.8 AND 3.2
    GROUP BY timestep, vertex_id
    ORDER BY avg_e, timestep, vertex_id LIMIT 100"""


DEEPWATER = f"""
    SELECT timestep, max((rowid % 250000) // 500) AS max_height
    FROM read_parquet('{DATA}/deepwater_none/*.parquet') WHERE v02 > 0.1 GROUP BY timestep"""


def tpch_q1(table_dir):
    return f"""
    SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price,
           sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
           sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
           avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc,
           count(*) AS count_order
    FROM read_parquet('{DATA}/{table_dir}/*.parquet')
    WHERE l_shipdate <= DATE '1998-09-02'
    GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"""


TPCH_PROJ = f"""
    SELECT l_orderkey, l_extendedprice, l_discount, l_tax,
           l_extendedprice * (1 - l_discount) * (1 + l_tax) AS charge
    FROM read_parquet('{DATA}/tpch/*.parquet') WHERE l_shipdate >= DATE '1998-06-01'"""


TPCH_JOIN = f"""
    WITH l AS (SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue
               FROM read_parquet('{DATA}/tpch/*.parquet') WHERE l_shipdate > DATE '1995-03-15'
               GROUP BY l_orderkey),
         o AS (SELECT o_orderkey, o_orderpriority FROM read_parquet('{DATA}/tpch_orders/*.parquet')
               WHERE o_orderdate < DATE '1995-03-15')
    SELECT o_orderpriority, sum(revenue) AS revenue, count(*) AS n_orders
    FROM l JOIN o ON l.l_orderkey = o.o_orderkey GROUP BY o_orderpriority ORDER BY o_orderpriority"""


def main():
    os.makedirs(OUT, exist_ok=True)
    con = duckdb.connect()
    jobs = {"laghos_16": laghos_sql(16), "deepwater": DEEPWATER, "tpch_q1": tpch_q1("tpch"),
            "tpch_q1_double": tpch_q1("tpch_double"), "tpch_proj": TPCH_PROJ}
    if os.path.isdir(f"{DATA}/tpch_orders"):
        jobs["tpch_join"] = TPCH_JOIN
    n_laghos = len([f for f in os.listdir(f"{DATA}/laghos") if f.endswith(".parquet")])
    for n in (1, 4, 8, 32, 64, 128, 256):
        if n <= n_laghos:
            jobs[f"laghos_{n}"] = laghos_sql(n)
    for key, sql in jobs.items():
        tbl = con.execute(sql).fetch_arrow_table()
        import pyarrow.parquet as pq
        pq.write_table(tbl, f"{OUT}/{key}.parquet")
        print(f"oracle {key}: {tbl.num_rows} dòng", flush=True)


if __name__ == "__main__":
    main()
