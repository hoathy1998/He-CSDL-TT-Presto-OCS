"""Logical plan dạng chuỗi toán tử (giống cột "Execution Plan" của Bảng 2 trong bài):

    TableScan -> Filter -> [Project] -> [Aggregation] -> [TopN | Sort]

`compile(query, k, agg_complete)` cắt chuỗi tại vị trí k:
  - k toán tử đầu (sau TableScan) được đẩy xuống OCS  -> storage SQL (sẽ dịch sang Substrait)
  - phần còn lại + bước gộp (merge) chạy ở compute  -> residual SQL
Giống bước "Selected operators ... merged into a modified TableScan" của Presto-OCS connector.
"""
from dataclasses import dataclass, field


@dataclass
class Filter:
    predicate: str
    ranges: list            # [(col, lo, hi)] để Selectivity Analyzer ước lượng; lo/hi có thể None
    output: list            # cột cần giữ lại sau filter (column projection kiểu S3 Select)
    kind: str = "filter"
    label: str = "Filter"


@dataclass
class Project:
    exprs: list             # [(alias, sql_expr, width_bytes)]
    complexity: int         # số phép toán số học (chi phí tính biểu thức)
    kind: str = "proj"
    label: str = "Project"


@dataclass
class AggCall:
    func: str               # min | max | sum | avg | count
    arg: str                # biểu thức hoặc '*'
    alias: str


@dataclass
class Aggregate:
    keys: list
    aggs: list
    kind: str = "aggr"
    label: str = "Aggregation"


@dataclass
class TopN:
    order: str
    limit: int
    kind: str = "topN"
    label: str = "TopN"


@dataclass
class Sort:
    order: str
    kind: str = "sort"
    label: str = "Sort"


@dataclass
class Query:
    name: str
    title: str
    table: str
    columns: list           # cột TableScan cần đọc
    ops: list
    paper_sql: str = ""
    note: str = ""
    extra: dict = field(default_factory=dict)

    def kinds(self):
        return [op.kind for op in self.ops]


def _partial_aggs(agg: Aggregate):
    """Aggregation từng phần (partial) ở storage + bước gộp (final) ở compute."""
    part, final = [], []
    for a in agg.aggs:
        if a.func in ("min", "max", "sum"):
            part.append(f"{a.func}({a.arg}) AS {a.alias}")
            final.append(f"{a.func}({a.alias}) AS {a.alias}")
        elif a.func == "count":
            part.append(f"count({a.arg}) AS {a.alias}")
            final.append(f"sum({a.alias}) AS {a.alias}")
        elif a.func == "avg":
            part += [f"sum({a.arg}) AS {a.alias}__sum", f"count({a.arg}) AS {a.alias}__cnt"]
            final.append(f"sum({a.alias}__sum) / sum({a.alias}__cnt) AS {a.alias}")
        else:
            raise ValueError(a.func)
    return part, final


def _apply(op, src, partial=False):
    keys = None
    if op.kind == "filter":
        return f"SELECT {', '.join(op.output)} FROM ({src}) AS s WHERE {op.predicate}"
    if op.kind == "proj":
        return f"SELECT {', '.join(f'{e} AS {a}' for a, e, _ in op.exprs)} FROM ({src}) AS s"
    if op.kind == "aggr":
        keys = ", ".join(op.keys)
        aggs = _partial_aggs(op)[0] if partial else [f"{a.func}({a.arg}) AS {a.alias}" for a in op.aggs]
        return f"SELECT {keys}, {', '.join(aggs)} FROM ({src}) AS s GROUP BY {keys}"
    if op.kind == "topN":
        return f"SELECT * FROM ({src}) AS s ORDER BY {op.order} LIMIT {op.limit}"
    if op.kind == "sort":
        return f"SELECT * FROM ({src}) AS s ORDER BY {op.order}"
    raise ValueError(op.kind)


def _merge(op, src, partial):
    """Bước gộp ở compute cho một toán tử đã đẩy xuống (kết quả từ nhiều split)."""
    if op.kind == "aggr" and partial:
        keys = ", ".join(op.keys)
        return f"SELECT {keys}, {', '.join(_partial_aggs(op)[1])} FROM ({src}) AS s GROUP BY {keys}"
    if op.kind in ("topN", "sort"):
        return _apply(op, src)
    return src  # filter / project / aggregation hoàn chỉnh: chỉ cần UNION các split


def compile(q: Query, k: int, agg_complete: bool):
    """Trả về (storage_sql | None, residual_sql). k=0: không pushdown (đọc nguyên object)."""
    scan = f"SELECT {', '.join(q.columns)} FROM t"
    if k == 0:
        sql = scan
        for op in q.ops:
            sql = _apply(op, sql)
        return None, sql
    storage = scan
    for op in q.ops[:k]:
        storage = _apply(op, storage, partial=not agg_complete)
    residual = "SELECT * FROM t"
    for op in q.ops[:k]:
        residual = _merge(op, residual, partial=not agg_complete)
    for op in q.ops[k:]:
        residual = _apply(op, residual)
    return storage, residual


def mode_to_k(q: Query, mode: str) -> int:
    """'none' | 'filter' | 'filter+proj' | 'filter+aggr+topN' | 'all' -> số toán tử đẩy xuống."""
    if mode == "none":
        return 0
    if mode == "all":
        return len(q.ops)
    want = mode.split("+")
    if want != q.kinds()[:len(want)]:
        raise ValueError(f"mode '{mode}' không phải tiền tố của plan {q.kinds()} "
                         f"(chỉ đẩy được các toán tử liên tiếp từ TableScan)")
    return len(want)


def k_to_mode(q: Query, k: int) -> str:
    return "none" if k == 0 else "+".join(q.kinds()[:k])


def explain(q: Query, k: int, agg_complete: bool) -> str:
    """In plan kiểu Presto EXPLAIN, đánh dấu ranh giới pushdown."""
    lines = []
    residual = q.ops[k:][::-1]
    merges = [op for op in q.ops[:k][::-1] if (op.kind == "aggr" and not agg_complete) or op.kind in ("topN", "sort")]
    depth = 0
    for op in residual:
        lines.append("  " * depth + f"- {op.label}   [compute]")
        depth += 1
    for op in merges:
        tag = "Aggregation(FINAL)" if op.kind == "aggr" else f"{op.label}(FINAL)"
        lines.append("  " * depth + f"- {tag}   [compute, gộp kết quả các split]")
        depth += 1
    if k == 0:
        lines.append("  " * depth + f"- TableScan[{q.table}]   [đọc nguyên object qua mạng]")
    else:
        pushed = []
        for op in q.ops[:k]:
            if op.kind == "aggr":
                pushed.append("Aggregation(" + ("COMPLETE" if agg_complete else "PARTIAL") + ")")
            else:
                pushed.append(op.label)
        lines.append("  " * depth + f"- OcsTableScan[{q.table}]  pushdown = {' -> '.join(pushed)}   [OCS]")
    return "\n".join(lines)
