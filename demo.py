"""Demo trực tiếp Presto-OCS (bản giả lập).

  docker compose exec compute python demo.py                       # menu tương tác
  docker compose exec compute python demo.py explain laghos         # plan + Selectivity Analyzer
  docker compose exec compute python demo.py run tpch_q1 --mode auto
  docker compose exec compute python demo.py compare deepwater [--splits 4]
  docker compose exec compute python demo.py net 100                # đổi băng thông mạng (Mbps)
  docker compose exec compute python demo.py history
"""
import argparse
import json
import os

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.syntax import Syntax
from rich.table import Table

from ocsdemo import plan as P
from ocsdemo.engine import Engine, same_result
from ocsdemo.optimizer import HISTORY, choose
from ocsdemo.queries import JOIN_QUERIES, QUERIES

con = Console(width=118)


def mb(b):
    return f"{b / 1e6:,.3f} MB" if b < 1e9 else f"{b / 1e9:,.2f} GB"


def show_cluster(eng):
    c = eng.cluster()
    con.print(Panel(f"Compute node: {c['compute_threads']} luồng   |   OCS storage node: {c['storage_threads']} CPU, "
                    f"SSD {c['media_mbps']:.0f} MB/s   |   Mạng OCS→compute: {c['net_mbit']:.0f} Mbps",
                    title="Cụm giả lập", style="cyan"))


def entry_for(eng, q, splits):
    e = eng.catalog[q.table]
    if splits:
        ss = e["splits"][:splits]
        e = dict(e, splits=ss, rows=sum(s["rows"] for s in ss), bytes=sum(s["bytes"] for s in ss))
    return e


def explain(eng, q, mode="auto", splits=None):
    e = entry_for(eng, q, splits)
    con.rule(f"[bold]{q.title}[/]")
    if q.paper_sql:
        con.print(Panel(Syntax(q.paper_sql, "sql", word_wrap=True), title="SQL trong bài báo (Bảng 2)"))
    con.print(f"Bảng [bold]{q.table}[/]: {len(e['splits'])} object, {e['rows']:,} dòng, {mb(e['bytes'])}")
    con.print("Logical plan: TableScan → " + " → ".join(op.label for op in q.ops))
    d = choose(q, e, eng.cluster(), mode, use_history=True)
    t = Table(title="Selectivity Analyzer – ước lượng cho từng điểm cắt pushdown (min/max/NDV từ catalog)")
    for h in ("Đẩy xuống OCS", "Selectivity op cuối", "Dòng truyền (ước lượng)", "Byte truyền (ước lượng)",
              "Truyền mạng", "CPU storage", "CPU compute", "Tổng ước lượng", ""):
        t.add_column(h, justify="right" if h not in ("Đẩy xuống OCS", "") else "left")
    for x in d.estimates:
        mark = "[green]◀ chọn[/]" if x.k == d.k else ("[red]" + x.why_not + "[/]" if not x.allowed else "")
        t.add_row(x.mode, f"{x.selectivity:.4f}", f"{x.rows:,.0f}", mb(x.bytes), f"{x.transfer_s:.2f}s",
                  f"{x.storage_cpu_s:.2f}s", f"{x.compute_cpu_s:.2f}s", f"{x.total_s:.2f}s", mark)
    con.print(t)
    con.print(f"Quyết định: [bold green]{d.mode}[/]  (theo {d.chosen_by});  Aggregation ở OCS: "
              f"{'COMPLETE (GROUP BY chứa cột hằng theo object)' if d.agg_complete else 'PARTIAL + FINAL ở compute'}")
    for n in d.notes:
        con.print(f"  • {n}", style="yellow")
    con.print(Panel(P.explain(q, d.k, d.agg_complete), title="Plan sau Local Optimizer (kiểu Presto EXPLAIN)"))
    s_sql, r_sql = P.compile(q, d.k, d.agg_complete)
    if s_sql:
        con.print(Panel(Syntax(s_sql, "sql", word_wrap=True), title="SQL dựng lại cho OCS → dịch sang Substrait IR"))
    con.print(Panel(Syntax(r_sql, "sql", word_wrap=True), title="Residual SQL chạy ở compute"))
    return d


def run(eng, q, mode="auto", splits=None, csv=False, show=True, inject_failure=False):
    r = eng.run(q, mode, kind_filter_csv=csv, max_splits=splits, inject_failure=inject_failure)
    if show and not r.pushdown_ok:
        con.print(Panel(f"OCS báo lỗi: {r.fallback_reason}\nConnector đã FALLBACK: đọc nguyên object và chạy "
                        f"toàn bộ truy vấn ở compute. Kết quả vẫn đúng, nhưng mất lợi ích pushdown.",
                        title="Pushdown thất bại", style="red"))
    if show:
        t = Table(title=f"Kết quả – {q.name} – {r.mode}")
        for c in r.result.column_names:
            t.add_column(c)
        for row in r.result.slice(0, 8).to_pylist():
            t.add_row(*[f"{v:.4f}" if isinstance(v, float) else str(v) for v in row.values()])
        con.print(t)
        tm = r.timings
        con.print(f"[bold]{r.total_s:.2f}s[/]  | data movement {mb(r.wire_bytes)}  | Substrait plan "
                  f"{r.substrait_bytes} B  | OCS đọc {mb(r.storage['media_bytes'])}, xử lý "
                  f"{r.storage['in_rows']:,}→{r.storage['out_rows']:,} dòng")
        con.print("  phân rã: " + ", ".join(f"{k}={v * 1000:.0f}ms" for k, v in tm.items()))
    return r


def compare(eng, q, splits=None):
    modes = [P.k_to_mode(q, k) for k in range(len(q.ops) + 1)]
    show_cluster(eng)
    res, ref = [], None
    for m in modes:
        try:
            with con.status(f"đang chạy {q.name} – {m} ..."):
                r = run(eng, q, m, splits, show=False)
        except ValueError as e:
            con.print(f"[red]{m}: {e}[/]")
            continue
        ref = ref if ref is not None else r.result
        res.append((r, same_result(ref, r.result)))
    base = next((r for r, _ in res if r.mode == "filter"), res[0][0])
    tmax = max(r.total_s for r, _ in res)
    t = Table(title=f"So sánh các mức pushdown – {q.title}")
    for h in ("Pushdown", "Thời gian", "", "vs filter", "Data movement", "Kết quả"):
        t.add_column(h)
    for r, ok in res:
        bar = "█" * max(1, int(40 * r.total_s / tmax))
        color = "dark_orange" if r.mode in ("none", "filter") else "dodger_blue1"
        t.add_row(r.mode, f"{r.total_s:.2f}s", f"[{color}]{bar}[/]", f"{base.total_s / r.total_s:.2f}×",
                  mb(r.wire_bytes), "[green]đúng[/]" if ok else "[red]SAI[/]")
    con.print(t)


def join_demo(eng, jq):
    """G2.1: truy vấn nhiều bảng – mỗi nhánh đẩy xuống OCS, JOIN chạy ở compute (toán tử residual)."""
    show_cluster(eng)
    con.rule(f"[bold]{jq.title}[/]")
    con.print(Panel(Syntax(jq.final_sql, "sql", word_wrap=True), title="Residual chạy ở compute (JOIN + gom nhóm cuối)"))
    res, ref = [], None
    for m in ("none", "filter", "all"):
        with con.status(f"đang chạy join – {m} ..."):
            j = eng.run_join(jq, m)
        ref = ref if ref is not None else j["result"]
        res.append((m, j, same_result(ref, j["result"])))
    t = Table(title="So sánh mức pushdown cho truy vấn có join")
    for h in ("Chế độ", "Nhánh lineitem", "Nhánh orders", "Thời gian", "Data movement", "Kết quả"):
        t.add_column(h)
    for m, j, ok in res:
        b = j["branches"]
        desc = lambda a: f"{b[a]['mode']}" + (" (agg PARTIAL)" if b[a]["k"] >= 3 and not b[a]["agg_complete"] else "")
        t.add_row(m, desc("l"), desc("o"), f"{j['total_s']:.2f}s", mb(j["wire_bytes"]),
                  "[green]đúng[/]" if ok else "[red]SAI[/]")
    con.print(t)
    tb = Table(title="Kết quả")
    for c in ref.column_names:
        tb.add_column(c)
    for row in ref.to_pylist():
        tb.add_row(*[f"{v:,.2f}" if not isinstance(v, (str, int)) else str(v) for v in row.values()])
    con.print(tb)


def history(eng=None):
    if not os.path.exists(HISTORY):
        con.print("chưa có lịch sử")
        return
    t = Table(title="Pushdown history (EventListener) – 15 lần chạy gần nhất")
    for h in ("query", "table", "mode", "Mbps", "time", "data movement", "pushdown"):
        t.add_column(h)
    with open(HISTORY) as fh:
        for line in fh.readlines()[-15:]:
            h = json.loads(line)
            ok = h.get("pushdown_ok", True)
            status = "-" if h.get("k", 0) == 0 else ("[green]thành công[/]" if ok else "[red]fallback[/]")
            t.add_row(h["query"], h["table"], h["mode"], f"{h['net_mbit']:.0f}", f"{h['total_s']:.2f}s",
                      mb(h["wire_bytes"]), status)
    con.print(t)
    if eng:
        rate, n = eng.pushdown_success_rate()
        if rate is not None:
            con.print(f"Tỉ lệ pushdown thành công (cửa sổ trượt {n} lần có pushdown gần nhất): [bold]{rate:.1%}[/]")


def menu(eng, splits):
    while True:
        show_cluster(eng)
        con.print("[bold]1[/] explain  [bold]2[/] run  [bold]3[/] compare  [bold]4[/] đổi băng thông  "
                  "[bold]5[/] history  [bold]q[/] thoát")
        c = Prompt.ask("chọn", default="3")
        if c == "q":
            return
        if c in "123":
            qn = Prompt.ask("truy vấn", choices=list(QUERIES), default="laghos")
            q = QUERIES[qn]
            if c == "1":
                explain(eng, q, splits=splits)
            elif c == "2":
                mode = Prompt.ask("mode", default="auto")
                explain(eng, q, mode, splits)
                run(eng, q, mode, splits)
            else:
                compare(eng, q, splits)
        elif c == "4":
            eng.conn.set_net(float(Prompt.ask("Mbps", default="1000")))
        elif c == "5":
            history(eng)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", nargs="?", default="menu",
                    choices=["menu", "explain", "run", "compare", "net", "history", "join"])
    ap.add_argument("arg", nargs="?")
    ap.add_argument("--mode", default="auto")
    ap.add_argument("--splits", type=int, default=None, help="chỉ dùng N object đầu (demo nhanh)")
    ap.add_argument("--csv", action="store_true", help="trả kết quả CSV kiểu S3 Select")
    ap.add_argument("--inject-failure", action="store_true", help="giả lập OCS lỗi để xem cơ chế fallback")
    a = ap.parse_args()
    eng = Engine()
    if a.cmd == "menu":
        menu(eng, a.splits)
    elif a.cmd == "explain":
        explain(eng, QUERIES[a.arg], a.mode, a.splits)
    elif a.cmd == "run":
        show_cluster(eng)
        explain(eng, QUERIES[a.arg], a.mode, a.splits)
        run(eng, QUERIES[a.arg], a.mode, a.splits, a.csv, inject_failure=a.inject_failure)
    elif a.cmd == "compare":
        compare(eng, QUERIES[a.arg], a.splits)
    elif a.cmd == "net":
        con.print(eng.conn.set_net(float(a.arg)))
        show_cluster(eng)
    elif a.cmd == "history":
        history(eng)
    elif a.cmd == "join":
        join_demo(eng, JOIN_QUERIES[a.arg or "tpch_join"])


if __name__ == "__main__":
    main()
