# Demo tái hiện: Presto-OCS (Object-based Computational Storage)

Tái hiện ý tưởng bài báo *"Integrating Distributed SQL Query Engines with Object-Based Computational Storage"*
(Ryu et al., SC Workshops '25) trên một laptop, bằng phần mềm mã nguồn mở. Vì OCS của SK hynix không công khai,
ta **giả lập OCS** nhưng giữ đúng các giao thức trong bài: **Substrait IR** cho plan, **gRPC** (Arrow Flight)
cho giao tiếp, **Apache Arrow** cho kết quả.

## Kiến trúc

```
┌─ compute (container) ────────────────────────────┐          ┌─ ocs (container, 2 CPU) ─────────────────┐
│ ocsdemo/engine.py     "Presto thu nhỏ"            │  gRPC    │ ocsdemo/ocs_server.py                     │
│  ├ optimizer.py       Local Optimizer +           │  Arrow   │  ├ nhận Substrait plan (Flight DoGet)     │
│  │                    Selectivity Analyzer        │  Flight  │  ├ DataFusion thực thi trên Parquet        │
│  ├ plan.py            Operator Extractor,         │◄────────►│  ├ trả Arrow (hoặc CSV kiểu S3 Select,    │
│  │                    cắt plan, partial/final agg │  tc tbf  │  │   hoặc nguyên object khi không pushdown)│
│  ├ connector.py       PageSourceProvider          │  1 Gbps  │  └ SSD giả lập 500 MB/s (token bucket)     │
│  └ DataFusion         thực thi residual operators │          │ catalog.json = Hive Metastore (thống kê)  │
└───────────────────────────────────────────────────┘          └───────────────────────────────────────────┘
```

| Thành phần trong bài (Hình 4) | Trong demo |
|---|---|
| Presto coordinator: Parser/Planner | `plan.py` (logical plan dạng chuỗi toán tử, như Bảng 2) |
| Local Optimizer (ConnectorPlanOptimizer), Selectivity Analyzer, Operator Extractor | `optimizer.py` |
| Hive Metastore (min/max, NDV, row count) | `catalog.py` → `catalog.json` |
| Substrait Plan Generator / Sender, Execution Result Receiver, PageSourceProvider | `connector.py` + `engine.py` |
| OCS frontend + storage node với engine SQL nhúng | `ocs_server.py` (Arrow Flight + DataFusion) |
| Residual Task Execution trên worker | DataFusion ở compute |
| EventListener / pushdown history | `results/pushdown_history.jsonl` |

## Dữ liệu (`scripts/prepare_data.py`)

| Bộ dữ liệu | Nguồn | Quy mô |
|---|---|---|
| Laghos | **Dữ liệu thật** của LANL (`lanl-ocs/laghos-sample-dataset`), đủ 256 file (`--laghos-files 256`); bảng `laghos` = 64 file đầu, `laghos_full` = 256 file, `laghos_one` = 1 file | 1,07 tỉ dòng, 26 GB |
| Deep Water Impact | Tổng hợp, cùng schema/ý nghĩa (rowid, v02, v03, timestep; lưới 500×500) | 64 triệu dòng; raw 2 GB / snappy / gzip / zstd |
| TPC-H lineitem | DuckDB dbgen SF1, chia 8 object | 6 triệu dòng, 386 MB |

## Chạy

```powershell
docker build -t ocs-demo:latest .
docker compose --profile prep run --rm prep          # tải + sinh dữ liệu, xây catalog (~5 phút)
docker compose up -d ocs compute                     # dựng cụm
docker compose exec compute python scripts/smoke_test.py   # kiểm tra mọi chế độ cho kết quả đúng
docker compose exec compute python scripts/bench.py --reps 3   # E1–E6 (~30–40 phút)
docker compose exec compute python scripts/plots.py        # -> results/figures/*.png
```

Đánh giá độ tin cậy (oracle DuckDB, hiệu chuẩn, đo lặp xen kẽ ngẫu nhiên, kiểm định thống kê, ~90 phút):

```powershell
docker compose --profile prep run --rm prep python scripts/prepare_data.py --laghos-files 32
docker compose --profile prep run --rm prep python scripts/oracle.py
docker compose exec compute python scripts/reliability.py oracle
# giám sát nguồn điện + giữ máy thức (chạy song song, ở cửa sổ khác)
powershell -ExecutionPolicy Bypass -File scripts\power_monitor.ps1
powershell -ExecutionPolicy Bypass -File scripts\run_reliability.ps1   # calib, e7, media, ablation, bandwidth, scale, cpus, stats
```

Kết quả: `results/reliability/summary.md` (bảng thống kê), `results/reliability/*.json` (toàn bộ số đo thô),
`results/figures/fig_r*.png`.

Presto 0.286 thật với connector Presto-OCS (G3) và baseline MinIO Select thật (G4b):

```powershell
docker compose --profile build run --rm maven                      # build plugin Java -> presto-ocs/target/plugin
docker compose --profile presto up -d presto                       # catalog: ocs (all), ocsfilter, ocsnopd, hive
docker compose --profile presto exec presto presto-cli --catalog ocs --schema default
#   presto> EXPLAIN SELECT timestep, max((rowid % 250000) / 500) FROM deepwater WHERE v02 > 0.1 GROUP BY timestep;
docker compose exec compute python scripts/presto_bench.py explain  # EXPLAIN Presto gốc (Hive) vs Presto-OCS
docker compose exec compute python scripts/presto_bench.py check    # so khớp kết quả với oracle DuckDB

docker compose --profile minio up -d --build minio minio-tc         # MinIO build từ mã nguồn tag RELEASE.2025-04-22
docker compose --profile prep run --rm prep python scripts/minio_baseline.py upload
docker compose exec compute python scripts/minio_baseline.py bench
```

Cụm OCS nhiều node (G2.2): `docker compose --profile multi up -d ocs2 ocs3`, rồi
`python scripts/reliability.py nodes` / `concurrency`.

Demo trực tiếp (trên lớp):

```powershell
docker compose exec compute python demo.py                          # menu
docker compose exec compute python demo.py explain laghos           # Selectivity Analyzer + EXPLAIN
docker compose exec compute python demo.py compare laghos           # so sánh mọi mức pushdown
docker compose exec compute python demo.py compare tpch_q1
docker compose exec compute python demo.py explain tpch_proj        # optimizer từ chối đẩy Project
docker compose exec compute python demo.py net 100                  # hạ băng thông xuống 100 Mbps
docker compose exec compute python demo.py run laghos --mode filter --csv   # kiểu S3 Select
docker compose exec compute python demo.py join                     # lineitem ⋈ orders: join chạy ở compute
docker compose exec compute python demo.py run laghos --mode all --splits 4 --inject-failure   # OCS lỗi -> fallback
docker compose exec compute python demo.py history                  # lịch sử + tỉ lệ pushdown thành công
```

Thêm `--splits 4` để demo nhanh hơn. Đổi cấu hình phần cứng: `OCS_CPUS`, `OCS_NET_MBIT`, `OCS_MEDIA_MBPS`
(biến môi trường của `docker compose up`).

## Các thí nghiệm ↔ câu hỏi nghiên cứu của bài

| Thí nghiệm | Câu hỏi trong bài | Hình |
|---|---|---|
| E1 | Q1: giảm data movement nhờ pushdown có rút ngắn thời gian? (Laghos) | `fig_q1_laghos.png` |
| E2 | Q2: pushdown có luôn có lợi? (Deep Water, TPC-H Q1 – Project) | `fig_q2_deepwater.png`, `fig_q2_tpch.png` |
| E3 | Q3: pushdown + nén (raw/snappy/gzip/zstd) | `fig_q3_compression.png` |
| E4 | Q4: overhead của connector | `fig_q4_breakdown.png` |
| E5 | (mở rộng) lợi ích phụ thuộc băng thông mạng | `fig_e5_bandwidth.png` |
| E6 | (mở rộng) độ chính xác Selectivity Analyzer, quyết định `auto` | `fig_e6_estimates.png` |

## Đơn giản hóa so với bài báo (cần nêu khi báo cáo)

- Không dùng Presto thật (đó là phương án B). Engine compute là "Presto thu nhỏ" viết bằng Python + DataFusion,
  nhưng đi đúng 5 bước của Mục 3.4 và cùng giao thức (Substrait/gRPC/Arrow).
- SQL được dựng lại từ các toán tử đẩy xuống rồi dịch sang Substrait bằng DataFusion (bài: tự viết bộ dịch trong Java).
- Phần cứng được giả lập: storage 2 CPU, mạng 1 Gbps (`tc`), SSD 500 MB/s; dữ liệu nhỏ hơn bài ~15 lần.
  Do đó **chỉ so sánh xu hướng và tỉ lệ**, không so sánh số giây tuyệt đối.
- Laghos: truy vấn gốc của LANL chạy trên từng file (timestep); ta nhóm theo `(timestep, vertex_id)` để
  pushdown Aggregation + TopN ở từng object vẫn chính xác. Với TPC-H, nhóm trải nhiều object → OCS chạy
  aggregation **từng phần (PARTIAL)**, compute gộp **FINAL** (avg → sum + count).
- Mọi chế độ pushdown được kiểm tra cho cùng kết quả với chế độ không pushdown (`correct=True`).

## Cấu trúc thư mục (dành cho người kiểm tra)

| Thư mục / file | Nội dung |
|---|---|
| `Dockerfile`, `docker-compose.yml`, `requirements.txt` | Môi trường chạy (phiên bản thư viện được ghim) |
| `ocsdemo/` | OCS giả lập (`ocs_server.py`) và engine thu nhỏ đóng vai Presto: optimizer, connector, catalog |
| `presto-ocs/` | Connector Presto-OCS bằng Java cho Presto 0.286 (`src/`), thư viện Substrait đóng gói riêng (`substrait-lib/`) |
| `presto/etc/` | Cấu hình Presto, các catalog `ocs` / `ocsfilter` / `ocsnopd` / `hive`, EventListener |
| `minio/` | Build MinIO từ mã nguồn chính thức (baseline S3 Select thật) |
| `scripts/` | Chuẩn bị dữ liệu, oracle DuckDB, các thí nghiệm, thống kê, vẽ hình, script đo trên Windows |
| `demo.py` | Demo trực tiếp (explain / run / compare / net / history / join) |
| `results/reliability/` | Dữ liệu đo thô của mọi lượt dùng trong báo cáo (JSON), log nguồn điện và hiệu năng CPU, `summary.md` (bảng thống kê đầy đủ) |
| `results/presto/` | EXPLAIN của Presto gốc (Hive) và Presto-OCS, lịch sử EventListener, kiểm tra tính năng, phân rã Q4 |
| `results/oracle/` | Kết quả đúng tính độc lập bằng DuckDB |
| `results/figures/` | Toàn bộ hình trong báo cáo |
| `results/bench.json` | Lượt chạy E1–E6 ban đầu (nguồn của hình E6 về độ chính xác Selectivity Analyzer) |

Dữ liệu (khoảng 35 GB) không nằm trong repo; tạo lại bằng `docker compose --profile prep run --rm prep python scripts/prepare_data.py --laghos-files 256`.
Connector Java build bằng `docker compose --profile build run --rm maven` (ra `presto-ocs/target/ocs-plugin/`).

Cách kiểm tra lại nhanh:

1. Tính lại thống kê và hình từ dữ liệu đo thô: `docker compose exec compute python scripts/stats.py` (so với `results/reliability/summary.md`).
2. Kiểm tra tính đúng: `docker compose exec compute python scripts/smoke_test.py` và `python scripts/presto_bench.py check` / `features` (so với oracle DuckDB và Presto + Hive).
3. Xem Local Optimizer trên Presto thật: `results/presto/explain_ocs_*.txt` so với `explain_hive_*.txt`.

