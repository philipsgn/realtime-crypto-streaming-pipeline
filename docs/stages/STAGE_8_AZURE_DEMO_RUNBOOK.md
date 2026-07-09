# Stage 8 - Azure Demo Runbook: Production-oriented CV Demo

> **Mục tiêu:** Deploy pipeline end-to-end lên Azure trong một phiên demo ngắn để lấy bằng
> chứng CV, sau đó teardown ngay để kiểm soát chi phí. Đây là production-oriented demo theo
> nghĩa có cloud deploy, security boundary, CI/CD, monitoring và evidence rõ ràng; không phải
> môi trường production 24/7 hay high availability.

> **Status:** Deferred — project is complete for CV purposes without cloud deploy. Azure remains
> planned evidence collection, not an implemented runtime environment.

---

## Bối cảnh

Day 5 thay AWS bằng Azure Free Account. Tài khoản dùng `$200 credit` trong 30 ngày đầu.
Workload chạy trên một Azure VM `Standard_B2s` tại region `Southeast Asia` để có đủ 2 vCPU
và 4GB RAM cho Kafka, Spark, TimescaleDB và Airflow. Không dùng B1s vì 1GB RAM không đủ
cho full pipeline.

Luồng demo giữ nguyên data thật từ Binance:

```text
Binance WebSocket -> Kafka KRaft -> PySpark -> TimescaleDB
                  -> dbt Gold -> Airflow -> Gemini -> Grafana Cloud
                                      |
                                      +-> Azure Blob Parquet
```

Nguyên tắc chi phí:

- `Standard_B2s` không phải lựa chọn "free forever"; nó dùng Azure credit.
- Demo target 4-6 giờ, đặt Azure VM auto-shutdown ở mốc tối đa 8 giờ.
- Không upgrade sang Pay-as-you-go cho project demo này.
- Sau khi lấy evidence, xóa VM, disk, public IP, storage account, managed identity, NSG và
  resource group nếu resource group chỉ dùng cho demo.
- Sau teardown, mở Cost Management và All resources để xác nhận không còn tài nguyên chạy.

---

## Mapping AWS -> Azure

| AWS cũ | Azure mới | Ghi chú |
|---|---|---|
| EC2 | Azure VM `Standard_B2s` | 2 vCPU, 4GB RAM, dùng credit |
| S3 | Azure Blob Storage | Private container cho Parquet |
| IAM Role | Managed Identity | Không tạo access key |
| Security Group | Network Security Group (NSG) | Chỉ mở port cần thiết từ source cụ thể |
| VPC | Virtual Network (VNet) | Network boundary của VM |
| us-east-1 | Southeast Asia | Gần Việt Nam hơn |
| AWS Budget | Cost Management + auto-shutdown | Cảnh báo và safety net, không phải hard cap tuyệt đối |

---

## Thứ tự phase bắt buộc

| Phase | Công việc | Điều kiện chuyển phase |
|---|---|---|
| 0 | Pre-flight local, không tạo Azure resource | Repo sạch, Day 1-4 verified, credit còn hạn |
| 1 | Tạo resource group, storage account, private container | Blob private, region đúng |
| 2 | Tạo user-assigned Managed Identity | Có quyền Storage Blob Data Contributor |
| 3 | Launch VM + NSG + auto-shutdown | Chỉ SSH/Airflow từ IP hiện tại |
| 4 | Bootstrap VM qua SSH | Docker, Git, Java 17 sẵn sàng |
| 5 | Deploy bằng GitHub Actions `deploy-azure.yml` | Compose services healthy |
| 6 | Chạy producer + Spark container | Có real Binance data vào TimescaleDB và Blob |
| 7 | Grafana Cloud + evidence collection | Dashboard có Gold data và AI summary |
| 8 | Teardown | Azure All resources không còn resource demo |

Không bỏ qua teardown. Không để VM ở trạng thái stopped nếu public IP/disk/storage vẫn còn
khả năng phát sinh phí.

---

## Phase 0 - Pre-flight local

Checklist trước khi mở Azure Portal:

- [ ] Azure Free Account đã active và còn trong 30 ngày credit.
- [ ] Cost Management hiển thị credit còn đủ cho phiên demo.
- [ ] Không upgrade Pay-as-you-go.
- [ ] Current public IP được kiểm tra ngay trước demo bằng `https://whatismyip.com`.
- [ ] Repo local đang ở branch `main`, Day 1-4 code đã push lên `origin/main`.
- [ ] CI xanh trên commit sẽ deploy.
- [ ] Không có `.env`, private key, account ID hoặc secret trong Git history.
- [ ] `.env` tạm nằm ngoài repo, chứa giá trị thật:
  - `POSTGRES_PASSWORD=<strong_random>`
  - `GEMINI_API_KEY=<real_key>`
  - `PARQUET_OUTPUT=abfss://raw-trades@<account>.dfs.core.windows.net/`
  - `AZURE_STORAGE_ACCOUNT=<account_name>`

Lệnh kiểm tra Git trước khi deploy:

```bash
git status --short
git branch --show-current
git fetch origin
git rev-parse HEAD
git rev-parse origin/main
git log -1 --oneline --decorate
```

Điều kiện pass: branch là `main`, `HEAD` khớp `origin/main`, không có thay đổi cần deploy
chưa commit.

---

## Phase 1 - Azure Blob Storage

Tạo storage account:

- Resource group: `rg-crypto-pipeline-demo`
- Storage account: `cryptopipelinedemo<random4digits>`
- Region: `Southeast Asia`
- Performance: Standard
- Redundancy: LRS
- Public blob access: Disabled
- Container: `raw-trades`, access level private
- Data Lake Storage Gen2 / hierarchical namespace: enable if using the preferred
  `abfss://` Spark path with Managed Identity

Spark ghi Parquet tới:

```dotenv
PARQUET_OUTPUT=abfss://raw-trades@<account>.dfs.core.windows.net/
AZURE_STORAGE_ACCOUNT=<account>
```

Không commit connection string hoặc storage key. Day 5 ưu tiên Managed Identity qua `abfss`.
`wasbs://...blob.core.windows.net/` chỉ dùng như fallback debug nếu có cấu hình secret ngoài repo
và phải xóa sau demo.

---

## Phase 2 - Managed Identity

Tạo user-assigned Managed Identity:

- Name: `crypto-pipeline-identity`
- Region: `Southeast Asia`
- Role assignment: `Storage Blob Data Contributor` trên storage account
- Attach identity vào VM ở Phase 3

Managed Identity thay cho access key. Không tạo Azure access key trong code hoặc `.env`
nếu không có lý do debug ngắn hạn đã ghi rõ.

---

## Phase 3 - VM Launch + NSG

Tạo Azure VM:

- Image: Ubuntu 22.04 LTS
- Size: `Standard_B2s` (2 vCPU, 4GB RAM)
- Authentication: SSH public key
- OS disk: 64GB, xóa cùng VM khi teardown
- Public IP: Dynamic, không chọn static cho demo ngắn
- Managed Identity: `crypto-pipeline-identity`
- Region: `Southeast Asia`

Inbound NSG rules:

| Port | Source | Mục đích |
|---:|---|---|
| 22 | `<CURRENT_PUBLIC_IP>/32` | SSH |
| 8080 | `<CURRENT_PUBLIC_IP>/32` | Airflow UI |

Không mở Kafka `9092/9093`, PostgreSQL `5432/5433` hoặc Grafana local ra Internet. Nếu
Grafana Cloud cần direct PostgreSQL tạm thời, chỉ mở rule theo egress CIDR chính thức và
xóa ngay sau evidence. Ưu tiên kết nối an toàn hơn thay vì `0.0.0.0/0`.

Set Auto-shutdown ngay khi tạo VM: thời điểm bắt đầu + tối đa 8 giờ.

---

## Phase 4 - Bootstrap VM

File bootstrap nằm tại `infrastructure/azure-bootstrap.sh`.

Chạy từ máy local:

```bash
scp -i ~/.ssh/azure_key.pem infrastructure/azure-bootstrap.sh azureuser@<VM_PUBLIC_IP>:~
ssh -i ~/.ssh/azure_key.pem azureuser@<VM_PUBLIC_IP> "bash ~/azure-bootstrap.sh"
```

Sau bootstrap:

```bash
cd ~/realtime-crypto-streaming-pipeline
cp .env.example .env
nano .env
chmod 600 .env
```

Không paste secret vào GitHub Actions input, Azure tag/description hoặc command có thể lộ
trong shell history.

---

## Phase 5 - Deploy bằng GitHub Actions

Workflow: `.github/workflows/deploy-azure.yml`

Trigger: `workflow_dispatch`

Input:

- `azure_vm_ip`

GitHub secret:

- `AZURE_SSH_KEY`

Workflow SSH vào VM, pull `main`, chạy:

```bash
docker compose -f infrastructure/docker-compose.azure.yml up -d --build
docker compose -f infrastructure/docker-compose.azure.yml ps
curl -f http://localhost:8080/health
```

Producer và Spark chạy trong Compose để Azure deploy chỉ cần một lệnh, có auto-restart và
Spark checkpoint nằm trong named volume.

---

## Phase 6 - Verify producer + Spark containers

Trên VM:

```bash
cd ~/realtime-crypto-streaming-pipeline
docker compose -f infrastructure/docker-compose.azure.yml ps
docker compose -f infrastructure/docker-compose.azure.yml logs producer --tail 20
docker compose -f infrastructure/docker-compose.azure.yml logs spark --tail 20
```

Acceptance:

- Kafka topic `crypto-trades` nhận trade thật từ Binance.
- Spark ghi `trade_metrics_1min` và `trade_metrics_5min` vào TimescaleDB.
- Spark ghi Parquet vào Azure Blob container `raw-trades`.
- dbt Gold models có dữ liệu.
- 4 DAG Airflow có latest successful run.
- AI summary ghi `source=gemini` hoặc `fallback_template`.

---

## Phase 7 - Grafana Cloud + Evidence

Evidence cần chụp, đã redact account ID và secret:

- Azure Cost Management: credit còn lại trước và sau demo.
- VM running, size `Standard_B2s`, region `Southeast Asia`.
- NSG inbound rules chỉ mở port cần thiết.
- GitHub Actions deploy workflow xanh.
- Airflow UI: 4 DAGs successful.
- Azure Blob: Parquet objects trong `raw-trades`.
- PostgreSQL/TimescaleDB: Gold tables có dữ liệu.
- Grafana Cloud: dashboard có live data và AI summary.

CV wording:

> Deployed an end-to-end real-time crypto data pipeline on Azure using VM,
> Blob Storage, Managed Identity, GitHub Actions, Airflow, dbt, Gemini and
> Grafana Cloud; collected production-style evidence and tore down resources
> after the demo to control cost.

---

## Phase 8 - Teardown

Thứ tự xóa:

1. Xóa temporary NSG rule cho PostgreSQL nếu từng mở cho Grafana Cloud.
2. Stop producer/Spark và `docker compose -f infrastructure/docker-compose.azure.yml down`.
3. Delete VM, tick xóa OS disk và public IP nếu Azure Portal hỏi.
4. Delete storage container và storage account.
5. Delete Managed Identity `crypto-pipeline-identity`.
6. Delete NSG.
7. Delete VNet nếu chỉ dùng cho demo.
8. Delete SSH key resource nếu tạo trong Azure Portal.
9. Delete resource group `rg-crypto-pipeline-demo` nếu toàn bộ resource trong đó chỉ phục vụ demo.

Final check:

- [ ] Azure Portal -> All resources không còn resource demo.
- [ ] Cost Management không còn cost bất thường.
- [ ] Không upgrade Pay-as-you-go.
- [ ] Evidence folder đã lưu screenshot cần thiết và đã redact.

---

## Memory footprint dự kiến trên Azure VM

| Thành phần | Giới hạn/ước lượng | Ghi chú |
|---|---:|---|
| Kafka KRaft | 400MB | Heap `-Xmx256m` |
| PostgreSQL/TimescaleDB | 300MB | Azure VM có 4GB, nhưng vẫn giữ thấp |
| Airflow standalone | 700MB | Dưới hard rule 768MB |
| PySpark host | 512MB driver | `local[2]` |
| Producer + dbt | ~150MB | Chạy theo nhu cầu |
| OS + Docker overhead | Phần còn lại | Theo dõi bằng `free -h` và `docker stats` |

Không thêm service mới nếu chưa tính RAM. Không dùng CeleryExecutor, KubernetesExecutor,
Flink, Redpanda, Superset hoặc mock data.

---

## Definition of Done

- [ ] Azure deployment chạy từ GitHub Actions.
- [ ] Pipeline dùng real Binance WebSocket data.
- [ ] Kafka, Spark, TimescaleDB, dbt, Airflow, Gemini và Grafana Cloud có evidence.
- [ ] Azure Blob có Parquet output.
- [ ] Airflow/Gemini degrade gracefully khi Gemini quota lỗi.
- [ ] Tài nguyên Azure đã teardown ngay sau demo.
- [ ] Không có secret hoặc private key trong repo.
