# Demo tái hiện: Presto-OCS (Object-based Computational Storage)

Tái hiện ý tưởng bài báo *"Integrating Distributed SQL Query Engines with Object-Based Computational Storage"*
(Ryu et al., SC Workshops '25) trên một laptop, bằng phần mềm mã nguồn mở. Vì OCS của SK hynix không công khai,
ta **giả lập OCS** nhưng giữ đúng các giao thức trong bài: **Substrait IR** cho plan, **gRPC** (Arrow Flight)
cho giao tiếp, **Apache Arrow** cho kết quả.

Mã nguồn này đi kèm **Báo cáo thực nghiệm của Nhóm 3** (môn Hệ cơ sở dữ liệu tiên tiến, UIT). Hệ thống có hai bản cài đặt
dùng chung một OCS giả lập:

- **Bản Python** (`ocsdemo/`): "Presto thu nhỏ", chọn toán tử đẩy xuống bằng mô hình chi phí (ước lượng độ chọn lọc từ
  thống kê, cộng chi phí mạng + CPU, chọn phương án rẻ nhất). Dùng cho các thí nghiệm chính Q1–Q4 và thí nghiệm mở rộng.
- **Bản Java** (`presto-ocs/`): connector cho **Presto 0.286 thật** qua Connector SPI. Bộ tối ưu hóa dùng quy tắc cố định:
  phép lọc luôn được đẩy; phép tổng hợp chỉ được đẩy khi số nhóm ước lượng ≤ 50% số dòng của mỗi object
  (`OcsPlanOptimizer.java`). Dùng để kiểm chứng kết quả trên một hệ truy vấn phân tán thực tế.

## Đối chiếu báo cáo ↔ mã nguồn

| Trong báo cáo | Tệp / thư mục |
|---|---|
| Hình 1, Bảng 5 – kiến trúc và các thành phần | `ocsdemo/` (bản Python), `presto-ocs/` (bản Java); bảng *Thành phần trong bài* bên dưới |
| Mục 4.2 – giữ kết quả đúng khi dữ liệu nằm trên nhiều object | `ocsdemo/plan.py` (Top-N cục bộ → toàn cục, tổng hợp hai pha, AVG → SUM + COUNT) |
| Mục 5, Bảng 6 – đáp án độc lập, hiệu chuẩn, đo lặp, kiểm định | `scripts/oracle.py`, `scripts/reliability.py`, `scripts/stats.py`; kết quả trong `results/oracle/`, `results/reliability/summary.md` |
| Hình 2 – Q1 Laghos | `results/figures/fig_q1_laghos.png` |
| Hình 3, Hình 4 – Q2 Deep Water, TPC-H Q1 | `results/figures/fig_q2_deepwater.png`, `fig_q2_tpch.png` |
| Bảng 7, Hình 5 – Q3 nén dữ liệu | `results/figures/fig_q3_compression.png`; nhóm `e7` trong `summary.md` |
| Bảng 8 – Q4 phân rã thời gian trên Presto 0.286 | `results/presto/q4_breakdown.md` (`scripts/presto_q4.py`) |
| Bảng 9, Hình 6 – băng thông mạng | `results/figures/fig_e5_bandwidth.png`; nhóm `bandwidth` trong `summary.md` |
| Bảng 10 – ước lượng độ chọn lọc | `results/bench.json`, `results/figures/fig_e6_estimates.png` |
| Mục 7 – thí nghiệm mở rộng (đĩa, số node, CPU, Presto thật, MinIO Select) | `fig_r2_media_sweep.png`, `fig_r3_scale.png`, `fig_r4_cpus.png`, `fig_g3_*.png`, `fig_g4b_*.png` |
| Bảng 11, Hình 7 – so sánh với bài báo | `results/figures/fig_r1_speedup_ci.png`; mục so sánh trong `summary.md` |
| Hình 8 – tốc độ bộ máy SQL nhúng | `results/figures/fig_r5_engine_cost.png` |
| Phụ lục – chạy lại thực nghiệm | mục **Chạy** bên dưới |

Các tệp `*_vs_paper.png` trong `results/figures/` vẽ cùng số liệu theo kiểu Figure 5/6 của bài báo, đặt cạnh số liệu
của bài báo; chúng dùng cho slide thuyết trình, không có trong báo cáo.

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
docker compose --profile build run --rm maven                      # build plugin Java -> presto-ocs/target/ocs-plugin
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

Trang web thử truy vấn trên Presto 0.286 thật (gõ SQL tự do, so sánh các mức pushdown về thời gian, dữ liệu
OCS → Presto, kế hoạch truy vấn và tính đúng của kết quả):

```powershell
docker compose --profile presto up -d ui                            # rồi mở http://localhost:8501
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

- Các thí nghiệm chính chạy trên "Presto thu nhỏ" viết bằng Python + DataFusion, đi đúng 5 bước của Mục 3.4 và cùng
  giao thức (Substrait/gRPC/Arrow). Kết quả được kiểm chứng lại trên Presto 0.286 thật với connector Java (`presto-ocs/`).
- OCS của SK hynix được thay bằng DataFusion chạy trong container; tốc độ bộ máy SQL nhúng vì vậy khác bài báo.
- Bản Python dựng lại SQL từ các toán tử đẩy xuống rồi dịch sang Substrait bằng DataFusion; bản Java sinh Substrait
  bằng substrait-java (isthmus).
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
| `ui/` | Trang web thử truy vấn trên Presto 0.286 thật (`server.py` + `index.html`, cổng 8501) |
| `presto/etc/` | Cấu hình Presto, các catalog `ocs` / `ocsfilter` / `ocsnopd` / `hive`, EventListener |
| `minio/` | Build MinIO từ mã nguồn chính thức (baseline S3 Select thật) |
| `scripts/` | Chuẩn bị dữ liệu, oracle DuckDB, các thí nghiệm, thống kê, vẽ hình, script đo trên Windows |
| `demo.py` | Demo trực tiếp (explain / run / compare / net / history / join) |
| `results/reliability/` | Dữ liệu đo thô của mọi lượt dùng trong báo cáo (JSON), log nguồn điện và hiệu năng CPU, `summary.md` (bảng thống kê đầy đủ) |
| `results/presto/` | EXPLAIN của Presto gốc (Hive) và Presto-OCS, lịch sử EventListener, kiểm tra tính năng, phân rã Q4 |
| `results/oracle/` | Kết quả đúng tính độc lập bằng DuckDB |
| `results/figures/` | Toàn bộ hình trong báo cáo (xem bảng đối chiếu ở đầu tệp) và các hình `*_vs_paper.png` dùng cho slide |
| `results/bench.json` | Lượt chạy E1–E6 ban đầu (nguồn của hình E6 về độ chính xác Selectivity Analyzer) |

Dữ liệu (khoảng 35 GB) không nằm trong repo; tạo lại bằng `docker compose --profile prep run --rm prep python scripts/prepare_data.py --laghos-files 256`.
Connector Java build bằng `docker compose --profile build run --rm maven` (ra `presto-ocs/target/ocs-plugin/`).

Cách kiểm tra lại nhanh:

1. Tính lại thống kê và hình từ dữ liệu đo thô: `docker compose exec compute python scripts/stats.py` (so với `results/reliability/summary.md`).
2. Kiểm tra tính đúng: `docker compose exec compute python scripts/smoke_test.py` và `python scripts/presto_bench.py check` / `features` (so với oracle DuckDB và Presto + Hive).
3. Xem Local Optimizer trên Presto thật: `results/presto/explain_ocs_*.txt` so với `explain_hive_*.txt`.

