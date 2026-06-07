# Airflow 自動排程指南

> 本文件介紹本專案如何用 Airflow 達成「**每天自動爬 ETF 資料**」,對應老師架構圖的 🟡 **排程** 區塊。

---

## 🎯 為什麼要 Airflow

原本 ETF 爬蟲是「**手動**」執行:

```bash
.venv/bin/python -m crawler.producer_crawler_etf_top20_v2
```

整合 Airflow 後變成「**自動**」,每天平日 18:00 自己跑,不用人盯。

---

## 🏗️ 架構

```
              ┌──────────────────────────────────────┐
              │       Airflow Stack (新增)           │
              │                                      │
              │   scheduler                          │
              │      │                               │
              │      │ (每天 18:00 觸發)            │
              │      ▼                               │
              │   DAG: etf_top20_crawler             │
              │      │                               │
              │      │ DockerOperator                │
              │      ▼                               │
              │   啟動 ETF Worker image 跑爬蟲       │
              └──────────────┬───────────────────────┘
                             │
                             │ 派任務
                             ▼
              ┌──────────────────────────────────────┐
              │  RabbitMQ → ETF Worker → MySQL       │
              │  (本專案原本就有的 pipeline)         │
              └──────────────────────────────────────┘
```

Airflow 不取代爬蟲,而是**自動派任務給原本的 ETF Worker**。

---

## 📂 Airflow 模組位置

本專案的 Airflow 部署檔放在 [`../airflow/`](../airflow/) 子目錄,包含:

| 檔案 | 用途 |
|------|------|
| `docker-compose-airflow.yml` | Airflow Stack 部署 (webserver/scheduler/worker/redis) |
| `dataflow/dags/etf_crawler_dag.py` | ETF Top 20 主 DAG (DockerOperator 版) |
| `dataflow/dags/etf_top20_dag.py` | ETF Top 20 替代 DAG (PythonOperator 版) |
| `with.env.Dockerfile` | Build 自建 Airflow image |
| `SETUP.md` | 完整部署指南 |
| `local.ini.example` | 環境設定範本(含密碼預留位置)|

---

## 🚀 部署步驟

完整步驟見 [`../airflow/SETUP.md`](../airflow/SETUP.md),重點摘要:

```bash
# 1. 進入 airflow 子目錄
cd airflow

# 2. 設定環境 (照 SETUP.md 步驟改密碼)
cp local.ini.example local.ini
# 編輯 local.ini, 把 <YOUR_MYSQL_PASSWORD> 改成真密碼
ENV=DOCKER python3 genenv.py

# 3. Build 自建 image
docker build -f with.env.Dockerfile -t <你的_docker_username>/tibame_dataflow:0.0.1 .

# 4. 改 docker-compose-airflow.yml 用你的 image
sed -i 's|howardlch/tibame_dataflow|<你的_docker_username>/tibame_dataflow|g' docker-compose-airflow.yml

# 5. 部署
DOCKER_IMAGE_VERSION=0.0.1 docker stack deploy \
  --resolve-image=never \
  -c docker-compose-airflow.yml airflow

# 6. 等 60 秒, 打開 http://127.0.0.1:5000
# 登入: admin / admin
```

---

## ✅ 驗證

```bash
# 看所有 service 都 1/1
docker service ls | grep airflow

# 看 DAG 是否被掃到
docker exec $(docker ps -q -f name=airflow_scheduler) uv run airflow dags list

# 開瀏覽器手動 Trigger 一次測試
# http://127.0.0.1:5000 → etf_top20_crawler → ▶ Trigger DAG
```

---

## 🔄 跟手動模式如何切換

- **白天測試** → 手動 producer (`crawler.producer_crawler_etf_top20_v2`)
- **生產環境** → 讓 Airflow 自動跑(上面部署完就是生產模式)

兩種模式**不互斥**,都用同一個 ETF Worker container,同一個 RabbitMQ queue。

---

## 🐛 常見坑

| 症狀 | 原因 | 解法 |
|------|------|------|
| DAG 看不到 | DAG 檔不在 mount 位置 | 確認 `docker-compose-airflow.yml` 有掛 `dataflow/dags` volume |
| 任務在跑但 MySQL 沒新資料 | ETF Worker 不在同個 network | ETF Worker 需要加入 `my_attachable_network` |
| scheduler 一直 0/1 | 密碼錯 | 確認 `airflow.cfg` 跟 `local.ini` 的 MYSQL_PASSWORD 都對齊 |
| Trigger 後等很久 | ETF Worker concurrency=1 | 正常,一次處理 1 個任務,100 個任務約 5-10 分鐘 |
| 週末 Trigger 沒新資料 | 證交所非交易日 | 正常,爬蟲遇非交易日會跳過 |

---

## 🔗 配套文件

| 文件 | 用途 |
|------|------|
| [`../README.md`](../README.md) | 主專案說明 |
| [`./DATAFLOW.md`](DATAFLOW.md) | 資料流向完整教學 |
| [`../airflow/SETUP.md`](../airflow/SETUP.md) | Airflow 詳細部署指南 |
| https://github.com/howardliang110/dataflow | dataflow 獨立 repo |
