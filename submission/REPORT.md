# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Vũ Minh Hoàng / 2A202602371 | **Repo:** [K4-Track02-Day17-Data-Pipeline-Engineering](https://github.com/minhhoangvu111/K4-Track02-Day17-Data-Pipeline-Engineering) | **Commit:** xem lịch sử Git.

**AI đã dùng:** OpenAI Codex hỗ trợ đọc đề/mã, sửa lỗi, chạy kiểm tra và soạn báo cáo; người học cần tự rà soát và giải thích các thay đổi. **Nguồn khác:** Không có.

## 1. Ba lỗi

| | Silver key | Late data | CDC delete |
|---|---|---|---|
| **Triệu chứng** | 24 hàng/12 ticket; T-91 có ba trạng thái. | Gold lệch full recompute; u05 08-12 có (2,1,0), cần (5,3,1); lookback 0 < P99 3. | T-97 còn hai hàng sống; snapshot mới nhất còn một dòng, RAG còn hai chunks. |
| **Nguyên nhân** | `INSERT` từng batch; dedup trong batch nhưng thiếu upsert và LSN guard giữa các lần chạy. | Lookback 0 chỉ tính ngày chạy, bỏ partition event-time cũ khi event đến muộn. | Delete có `after=null`; staging không lấy ticket ID từ `before`/key nên bỏ mất change. |
| **Sửa** | `pipeline/silver.py`: `MERGE` theo `ticket_id`, chỉ update khi LSN mới hơn. | `pipeline/config.py`: `LOOKBACK_DAYS=3=ceil(P99)`; Gold overwrite partition trong cửa sổ. | `pipeline/staging.py`: ID fallback `after→before→key`; Silver tombstone sạch PII; Gold lọc T-97 khỏi snapshot mới nhất/RAG. |
| **Khái niệm** | Keyed current-state, LSN ordering, idempotent replay; history SCD2 riêng. | Event time ≠ ingest time; đo lateness từ Bronze và lookback để reconcile. | Debezium delete khác Kafka tombstone; giữ tombstone/LSN để xóa lan và chống hồi sinh khi replay. |

## 2. Kết quả

Lateness trên 43 events: P50 **0.00**, P95 **2.90**, P99 **3.00**, max **3 ngày**; lookback chọn **3 ngày**. Rerun PASS, C0=C1=C2=C3=`39e115c510ecdf526800eac227158a4f`. Verify **18/18**, pytest **34 passed**, dbt **PASS=19**, parity **PARITY** (Silver `3c15dfd43701`, Gold `8630e04a61d1`).

## 3. Lựa chọn kỹ thuật

- MERGE theo khóa giữ một trạng thái hiện tại và guard LSN; overwrite partition tính lại aggregate event-time trong lookback mà không cộng trùng khi replay.
- Tombstone giữ LSN để chặn dữ liệu cũ hồi sinh; PII bị xóa khỏi Silver/Gold, còn Bronze bất biến phục vụ audit có kiểm soát.
- Snapshot “as of” giữ khả năng tái lập lịch sử; quyền xóa cần quy trình purge/ẩn danh riêng, ưu tiên hơn bất biến khi có yêu cầu hợp lệ.
- DuckDB/dbt nhẹ, zero-key, tái lập và đủ cho seed nhỏ; Spark chỉ cần khi tải phân tán ở quy mô lớn.

## 4. Suy ngẫm

1. Snapshot cũ không miễn trừ quyền xóa. Tôi sẽ giữ metadata kiểm toán tối thiểu, mã hóa nội dung bằng khóa riêng từng chủ thể; khi có yêu cầu hợp lệ thì hủy khóa, purge/rebuild snapshot và embedding liên quan, tạo version mới ghi nhận việc xóa và đặt retention để bản cũ không được phục vụ.
2. Đặt chốt PII ở staging trước khi lưu Silver/tạo embedding: NER tiếng Việt kết hợp regex/validator; trường hợp không chắc chắn vào quarantine hoặc human review. Đo precision/recall trên tập gán nhãn tiếng Việt và chạy leakage scan trên Silver, transcript, snapshots, chunks/cache cùng tên canary.

## 5. Output kiểm tra

Verify, rerun, dbt và parity chạy trên bản sao tạm của source/seed cuối vì workspace khóa thao tác reset warehouse; pytest chạy trên repo bằng fixture cô lập. Các file kiểm tra và dữ liệu seed không bị sửa.

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
RESULT: 18/18 checks — ALL PASS

$ .\.venv\Scripts\python.exe -m pytest -q -o addopts=
..................................                                       [100%]
34 passed in 7.85s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
fresh build             8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ Push-Location dbt_project
$ ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
$ Pop-Location

$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
