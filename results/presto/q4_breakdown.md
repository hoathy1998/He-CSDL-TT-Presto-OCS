| catalog | n | tổng (ms) | planning | optimizer | substrait | scan (OCS + mạng) | residual (wall, cộng dồn) | overhead connector | dữ liệu qua mạng |
|---|---|---|---|---|---|---|---|---|---|
| ocsnopd | 10 | 1928 | 15 | 0.00 | 4.2 | 3250 | 219 | 0.22% | 185.04 MB |
| ocsfilter | 10 | 708 | 14 | 0.09 | 6.3 | 1037 | 209 | 0.90% | 39.78 MB |
| ocs | 10 | 362 | 12 | 0.16 | 8.6 | 648 | 0 | 2.41% | 0.01 MB |
