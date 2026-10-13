# REPORT — Lab Day 9

| | |
|---|---|
| Họ và tên | Dương Dương |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602498 |
| GitHub | duongduong1606 |
| Trip dùng cho bài tập sập và chạy lại | trip-06667 |

## Kết quả cuối

- `dataset_version` của bạn: `10a0ac439a38` (khớp `report/evidence/run_summary.json`).
- Số khung hình train / val / test: 653 / 190 / 176, tổng cộng 1019 khung.
- `report/verify.txt` có kết quả `27/27 checks passed`.
- Bốn ảnh minh chứng nằm trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, `airflow_failure_log.png`.

## Bước 1 · Bronze

### Câu 1

Nguồn A dùng `ts_utc` dạng ISO-8601, còn B dùng `timestamp_ms` dạng epoch mili-giây. A ghi tốc độ ở `speed_mps`, B ghi `spd_kmh`; A dùng `heading_deg`, `lat`, `lon`, `acc_x/acc_y`, còn B dùng `hdg`, `latitude`, `longitude`, `ax/ay`. Chân ga và phanh của A là số thập phân `gas/brake`, trong khi B là phần trăm `throttle_pct/brake_pct`. Đồng hồ B còn chạy nhanh hơn video 1500 ms, A không lệch. Tách hai bảng bronze và giữ giá trị dạng chữ giúp lưu nguyên trạng nguồn, theo dõi schema drift và tránh làm mất các giá trị lỗi khi ép kiểu; việc hợp nhất chỉ diễn ra có kiểm soát ở silver.

## Bước 2 · Silver telemetry

### Câu 2

Hai model staging đổi tên các cột về cùng schema, ép thời gian thành `timestamptz`, đổi tốc độ B từ km/h sang m/s bằng cách chia 3,6, đổi phần trăm bàn đạp về tỷ lệ 0–1 và trừ 1500 ms khỏi đồng hồ B. Sau chuẩn hoá có 2968 dòng sạch ở A và 3181 ở B; mẫu đầu B là `2025-10-08 14:30:00.009+00:00`, lệch chưa tới 0,5 giây so với lúc video bắt đầu.

| Drive | duplicate | speed_missing | speed_out_of_range | sạch | nhận vào |
|---|---:|---:|---:|---:|---:|
| drive_a | 20 | 30 | 3 | 2968 | 3021 |
| drive_b | 20 | 30 | 3 | 3181 | 3234 |

Các dòng lỗi được đưa vào `rejects` cùng lý do thay vì xoá để không mất bằng chứng, có thể kiểm toán chất lượng nguồn, sửa luật và tái xử lý về sau; tổng sạch cộng quarantine vẫn đúng bằng số dòng nhận vào.

### Câu 3

`max_gap_ms` là cảnh báo vì khoảng ngắt lấy mẫu có thể do mất tín hiệu tạm thời nhưng không làm giá trị từng bản ghi sai; pipeline vẫn có thể xuất dữ liệu và báo người vận hành. `in_range` là lỗi vì tốc độ ngoài miền 0–70 m/s là dữ liệu vô lý, nếu đi tiếp sẽ làm sai feature và thống kê. Khi nới luật silver lên 1000 m/s, test `in_range_silver_telemetry_speed_mps__70__0` bắt đúng 6 dòng: kết quả chuyển từ `PASS=6 WARN=1 ERROR=0` thành `PASS=5 WARN=1 ERROR=1`. Cổng chất lượng vì vậy đã chặn dữ liệu tuy hợp schema nhưng sai ngữ nghĩa trước khi sang gold.

## Bước 3 · Silver frames

### Câu 4

Với `trip-00003`, bảng có 1246 dòng trước và vẫn 1246 dòng sau hai lần chạy lại; fingerprint cũng giữ nguyên `6c7480dfde7e`. Biến thể ghi `append` tạo 120 dòng sau hai lần cho trip vốn chỉ có 60 khung, chứng minh dữ liệu bị nhân đôi. Delta `overwrite` có predicate theo đúng partition trip thay thế dữ liệu bằng một transaction nguyên tử: hoặc phiên bản mới được commit đầy đủ, hoặc phiên bản cũ vẫn đọc được. Cách xoá rồi ghi có một khoảng trống nguy hiểm; nếu tiến trình lỗi giữa hai thao tác thì partition có thể mất hoặc dở dang.

### Câu 5

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---:|---:|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen gần như không có thông tin do camera/lỗi giải mã nên bị loại; khung tối vẫn có nội dung lái xe và được gắn cờ nhưng giữ lại. Tôi đồng ý vì loại toàn bộ cảnh tối sẽ làm mô hình thiếu tình huống ban đêm; kết quả thực tế giữ 4 khung tối ở A và 61 ở B. Tôi chọn ngưỡng dHash 2 như pipeline: nó bỏ được chuỗi gần đứng hình nhưng chưa loại quá mạnh như ngưỡng 6. Khi xe chạy chậm, các khung liên tiếp tự nhiên giống nhau hơn nên cùng ngưỡng sẽ đánh dấu nhiều hơn; ở tốc độ cao ảnh đổi nhanh, có thể cần cân nhắc ngưỡng lớn hơn hoặc kết hợp tốc độ/chuyển động.

## Bước 4 · Gold

### Câu 6

Chia ngẫu nhiên theo từng frame làm cả 26/26 trip bị cắt qua nhiều split, và cả 26 trip đều xuất hiện đồng thời ở train và test. Chia theo trip cho kết quả 0 trip bị cắt. Các frame liền nhau trong một chuyến có cảnh đường, thời tiết, camera và vật thể gần như giống nhau; nếu chúng nằm ở cả train lẫn test thì mô hình đã gián tiếp nhìn thấy bối cảnh kiểm thử. Điểm test khi đó cao giả tạo do đo khả năng nhớ những frame gần trùng, không phản ánh khả năng tổng quát sang một chuyến xe mới.

### Câu 7

`dataset_version` được băm từ danh sách `frame_id|split` đã sắp xếp nên cùng nội dung và cách chia luôn cho cùng mã, bất kể giờ chạy hay số lần chạy. Hai lần export liên tiếp đều cho `10a0ac439a38`, nhờ đó đường dẫn và manifest ổn định, dễ cache và đối chiếu. MSSV `2A202602498` được trộn vào thứ tự chia trip, nên sinh viên dùng cùng dữ liệu đầu vào vẫn có assignment train/val/test khác và vì vậy có content hash khác. Ngày giờ hoặc số đếm không có tính chất này: nội dung không đổi vẫn có thể mang phiên bản mới và không thể tự chứng minh sự đồng nhất.

### Câu 8

Lineage ghi các phiên bản Delta: `bronze.raw_telemetry_a: 0`, `bronze.raw_telemetry_b: 0`, `bronze.trips_raw: 0`, `bronze.video_files: 1`, `silver.frames: 50`, `silver.telemetry: 0`, `silver.frames_flagged: 0`, `silver.frame_telemetry: 0`. Hai video đầu vào có SHA-256 lần lượt là drive A `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa` và drive B `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Sau ba tháng, các version bảng, hash video, luật drop/split, MSSV, `dataset_version` và manifest giúp lấy lại đúng input lẫn phép biến đổi để tái tạo dataset.

## Bước 5 · Điều phối và phục vụ

### Câu 9

Trip được chọn là `trip-06667`. Lần chạy đầu có 1 mapped task `silver_frames` chuyển đỏ vì lỗi mô phỏng và 7 task phụ thuộc thành `upstream_failed`; log trong `airflow_failure_log.png` ghi rõ trip này. Tôi clear nhóm task đỏ trong giao diện; do cách Airflow hiển thị dynamic task mapping, thao tác đó requeue cả 26 map index của `silver_frames` (đều có `try_number=2`), rồi 7 task downstream chạy lại, tổng cộng 33 task instance được thực thi lại. DAG sau đó xanh như `airflow_graph.png`; dữ liệu không nhân đôi, verify vẫn 27/27 và `dataset_version` vẫn là `10a0ac439a38`, chứng minh các bước idempotent dù phạm vi clear rộng hơn một map index.

### Câu 10

Thứ nhất, trích frame bằng FFmpeg theo từng trip trên một Airflow worker sẽ nghẽn CPU/I/O; tôi sẽ dùng Kubernetes/Celery executor, hàng đợi và worker autoscale, đồng thời xử lý video song song. Thứ hai, DuckDB cục bộ và thao tác Delta từ một máy sẽ thành điểm nghẽn/điểm lỗi đơn; tôi sẽ chuyển compute sang Spark hoặc một lakehouse engine phân tán, giữ object storage và partition hợp lý. Thứ ba, Postgres/Metabase nhận toàn bộ mart bằng `CREATE OR REPLACE` sẽ quá chậm; tôi sẽ nạp incremental/CDC vào warehouse có partition, aggregate trước và thêm cache/materialized view. Cần kèm data catalog, lineage, SLA và quan sát tài nguyên để vận hành quy mô 500 xe/ngày.

## Câu hỏi riêng của bạn trên Metabase

Tôi thêm câu hỏi “Top 10 trips có tỷ lệ frame bị loại cao nhất” trên nguồn `warehouse`. SQL tính `100 * (frames - frames_kept) / frames` từ `marts.mart_trip_summary`, sắp xếp giảm dần và lấy 10 trip. Kết quả cho thấy `trip-04622` cao nhất với 56,67%, tiếp theo là `trip-03904` 50%, `trip-02086` 46,67%, `trip-08888` 40% và `trip-07774` 38,24%. Câu hỏi giúp ưu tiên điều tra các chuyến mất nhiều dữ liệu vì frame đen, mờ, gần trùng hoặc mất đồng bộ; ảnh kết quả nằm ở `report/screenshots/metabase_dashboard.png`.

```sql
SELECT trip_id, drive_id, frames, frames_kept,
       ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 2) AS discard_rate_pct
FROM marts.mart_trip_summary
ORDER BY discard_rate_pct DESC
LIMIT 10;
```

## Nhìn lại

Điều bất ngờ nhất là drive B có nhiều khung tối nhưng vẫn hữu ích: pipeline giữ 61 khung tối thay vì coi “tối” đồng nghĩa với “đen”. Tôi còn chưa rõ nên hiệu chỉnh ngưỡng dHash cố định theo tốc độ xe hay dùng thêm optical flow để tránh loại nhầm lúc xe dừng. Một lỗi chưa được bắt tốt là GPS hợp lệ về kiểu và miền nhưng nhảy vị trí đột ngột giữa hai mẫu; tương tự, heading có thể đổi phi vật lý dù từng giá trị vẫn nằm trong miền. Có thể bổ sung kiểm tra vận tốc suy ra từ GPS, gia tốc/góc quay cực đại và tính liên tục không gian-thời gian.
