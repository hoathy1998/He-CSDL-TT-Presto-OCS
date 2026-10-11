"""Ba truy vấn của Bảng 2 trong bài báo, viết dưới dạng logical plan.

Ghi chú Laghos: truy vấn gốc của LANL chạy trên TỪNG file (một timestep); vertex_id lặp lại ở mọi
timestep. Vì vậy ta nhóm theo (timestep, vertex_id), với timestep là cột partition của object.
Nhờ vậy Aggregation và TopN ở từng object cho kết quả chính xác ("COMPLETE"), đúng như cách
OCS thực thi trên từng object trong bài báo.
"""
from dataclasses import dataclass

from .plan import AggCall, Aggregate, Filter, Project, Query, Sort, TopN

LAGHOS = Query(
    name="laghos",
    title="Laghos (LANL, dữ liệu thật)",
    table="laghos",
    columns=["vertex_id", "x", "y", "z", "e", "timestep"],
    ops=[
        Filter("x BETWEEN 0.8 AND 3.2 AND y BETWEEN 0.8 AND 3.2 AND z BETWEEN 0.8 AND 3.2",
               ranges=[("x", 0.8, 3.2), ("y", 0.8, 3.2), ("z", 0.8, 3.2)],
               output=["vertex_id", "x", "y", "z", "e", "timestep"]),
        Aggregate(keys=["timestep", "vertex_id"],
                  aggs=[AggCall("min", "vertex_id", "vid"), AggCall("min", "x", "min_x"),
                        AggCall("min", "y", "min_y"), AggCall("min", "z", "min_z"),
                        AggCall("avg", "e", "avg_e")]),
        TopN("avg_e, timestep, vertex_id", 100),
    ],
    paper_sql=("SELECT min(vertex_id) AS VID, min(x), min(y), min(z), avg(e) AS E FROM parquet\n"
               "WHERE x, y, z BETWEEN 0.8 AND 3.2 GROUP BY vertex_id ORDER BY E LIMIT 100"),
)

DEEPWATER = Query(
    name="deepwater",
    title="Deep Water Impact (tổng hợp)",
    table="deepwater",
    columns=["rowid", "v02", "timestep"],
    ops=[
        Filter("v02 > 0.1", ranges=[("v02", 0.1, None)], output=["rowid", "timestep"]),
        Project([("height_idx", "(rowid % 250000) / 500", 8), ("timestep", "timestep", 8)], complexity=2),
        Aggregate(keys=["timestep"], aggs=[AggCall("max", "height_idx", "max_height")]),
    ],
    paper_sql=("SELECT MAX((rowid % (500*500))/500), timestep FROM parquet\n"
               "WHERE v02 > 0.1 GROUP BY timestep"),
)

TPCH_Q1 = Query(
    name="tpch_q1",
    title="TPC-H Q1 (SF1)",
    table="lineitem",
    columns=["l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount", "l_tax",
             "l_shipdate"],
    ops=[
        Filter("l_shipdate <= DATE '1998-09-02'", ranges=[("l_shipdate", None, "1998-09-02")],
               output=["l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount",
                       "l_tax"]),
        Project([("l_returnflag", "l_returnflag", 1), ("l_linestatus", "l_linestatus", 1),
                 ("l_quantity", "l_quantity", 16), ("l_extendedprice", "l_extendedprice", 16),
                 ("l_discount", "l_discount", 16),
                 ("disc_price", "l_extendedprice * (1 - l_discount)", 16),
                 ("charge", "l_extendedprice * (1 - l_discount) * (1 + l_tax)", 16)], complexity=5),
        Aggregate(keys=["l_returnflag", "l_linestatus"],
                  aggs=[AggCall("sum", "l_quantity", "sum_qty"),
                        AggCall("sum", "l_extendedprice", "sum_base_price"),
                        AggCall("sum", "disc_price", "sum_disc_price"),
                        AggCall("sum", "charge", "sum_charge"),
                        AggCall("avg", "l_quantity", "avg_qty"),
                        AggCall("avg", "l_extendedprice", "avg_price"),
                        AggCall("avg", "l_discount", "avg_disc"),
                        AggCall("count", "*", "count_order")]),
        Sort("l_returnflag, l_linestatus"),
    ],
    paper_sql=("SELECT returnflag, linestatus, SUM(quantity), SUM(extendedprice),\n"
               "  SUM(extendedprice*(1-discount)), SUM(extendedprice*(1-discount)*(1+tax)),\n"
               "  AVG(quantity), AVG(extendedprice), AVG(discount), COUNT(*) FROM lineitem\n"
               "WHERE shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY\n"
               "GROUP BY returnflag, linestatus ORDER BY returnflag, linestatus"),
)

# Truy vấn phụ (không có trong bài): không có Aggregation phía sau Project.
# Dùng để cho thấy Selectivity Analyzer tự TỪ CHỐI đẩy Project (giảm 0 byte, tốn CPU storage).
TPCH_PROJ = Query(
    name="tpch_proj",
    title="TPC-H: chỉ Filter + Project (truy vấn phụ)",
    table="lineitem",
    columns=["l_orderkey", "l_extendedprice", "l_discount", "l_tax", "l_shipdate"],
    ops=[
        Filter("l_shipdate >= DATE '1998-06-01'", ranges=[("l_shipdate", "1998-06-01", None)],
               output=["l_orderkey", "l_extendedprice", "l_discount", "l_tax"]),
        Project([("l_orderkey", "l_orderkey", 8), ("l_extendedprice", "l_extendedprice", 16),
                 ("l_discount", "l_discount", 16), ("l_tax", "l_tax", 16),
                 ("charge", "l_extendedprice * (1 - l_discount) * (1 + l_tax)", 16)], complexity=4),
    ],
)

# Biến thể cho phân tích độ nhạy (thí nghiệm A): cùng kết quả với DEEPWATER, nhưng biểu thức chiếu
# tốn CPU hơn nhiều: round(sqrt((exp(ln(x+1))-1)^2)) = x với x >= 0. Dùng để tách riêng chi phí tính
# biểu thức ở storage (giả thuyết của bài cho mức chậm 7%) khỏi yếu tố lượng byte truyền.
_HEAVY = ("CAST(round(sqrt(power(exp(ln(CAST((rowid % 250000) / 500 AS DOUBLE) + 1.0)) - 1.0, 2))) "
          "AS BIGINT)")
DEEPWATER_HEAVY = Query(
    name="deepwater_heavy",
    title="Deep Water – projection nặng (biến thể độ nhạy)",
    table="deepwater",
    columns=DEEPWATER.columns,
    ops=[DEEPWATER.ops[0],
         Project([("height_idx", _HEAVY, 8), ("timestep", "timestep", 8)], complexity=8),
         DEEPWATER.ops[2]],
)

QUERIES = {q.name: q for q in (LAGHOS, DEEPWATER, TPCH_Q1, TPCH_PROJ, DEEPWATER_HEAVY)}


# ---------------------------------------------------------------- G2.1: truy vấn có JOIN
# Kiểu TPC-H Q3: doanh thu theo mức ưu tiên của các đơn đặt trước 15/03/1995 có hàng giao sau ngày đó.
# Mỗi nhánh được đẩy xuống OCS độc lập; JOIN là toán tử residual chạy ở compute (Mục 3.4 bước 5 của bài).
@dataclass
class JoinQuery:
    name: str
    title: str
    inputs: list          # [(alias, Query)]
    final_sql: str        # chạy ở compute trên các bảng alias
    paper_note: str = ""


LINEITEM_REV = Query(
    name="tpch_join_lineitem",
    title="lineitem: doanh thu theo đơn (nhánh của join)",
    table="lineitem",
    columns=["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"],
    ops=[
        Filter("l_shipdate > DATE '1995-03-15'", ranges=[("l_shipdate", "1995-03-15", None)],
               output=["l_orderkey", "l_extendedprice", "l_discount"]),
        Project([("l_orderkey", "l_orderkey", 8), ("rev", "l_extendedprice * (1 - l_discount)", 16)], complexity=2),
        Aggregate(keys=["l_orderkey"], aggs=[AggCall("sum", "rev", "revenue")]),
    ],
)

ORDERS_SEL = Query(
    name="tpch_join_orders",
    title="orders: đơn đặt trước 15/03/1995 (nhánh của join)",
    table="orders",
    columns=["o_orderkey", "o_orderpriority", "o_orderdate"],
    ops=[Filter("o_orderdate < DATE '1995-03-15'", ranges=[("o_orderdate", None, "1995-03-15")],
                output=["o_orderkey", "o_orderpriority"])],
)

TPCH_JOIN = JoinQuery(
    name="tpch_join",
    title="TPC-H kiểu Q3: lineitem ⋈ orders (join chạy ở compute)",
    inputs=[("l", LINEITEM_REV), ("o", ORDERS_SEL)],
    final_sql=("SELECT o.o_orderpriority, sum(l.revenue) AS revenue, count(*) AS n_orders "
               "FROM l JOIN o ON l.l_orderkey = o.o_orderkey "
               "GROUP BY o.o_orderpriority ORDER BY o.o_orderpriority"),
)
JOIN_QUERIES = {TPCH_JOIN.name: TPCH_JOIN}


def with_table(q: Query, table: str) -> Query:
    """Cùng truy vấn trên bảng khác (vd. deepwater_zstd cho thí nghiệm nén)."""
    from dataclasses import replace
    return replace(q, table=table, name=f"{q.name}@{table}")
