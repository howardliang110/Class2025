# ETF Top 20 v2 — 證交所 + yfinance (免費版)
> 本專案在老師的 Celery + RabbitMQ + MySQL + Swarm 架構上，新增 ETF Top 20 三大法人爬蟲。
> 老師的原始範例檔案都保留供參考，但 **ETF 專案實際只用下列標 🟢 的檔案**。

---

## 📖 這個專案在做什麼 (新手先看這裡)

本專案每天自動完成以下流程：

1. 從**證券交易所**抓取「三大法人 (外資、投信、自營商) 買賣超的 ETF 資料」
2. 挑出買超最多的**前 20 名 ETF**
3. 從 **Yahoo 股市 (yfinance)** 抓這 20 檔的股價 (開盤、收盤、成交量等)
4. 把結果**存進 MySQL 資料庫**，供後續視覺化、分析使用

### 用餐廳廚房比喻理解架構

| 專案元件 | 比喻 | 角色 |
|----------|------|------|
| Producer (派工程式) | 點餐櫃台 | 把「要爬哪天的資料」寫成任務單 |
| RabbitMQ (訊息佇列) | 廚房點單夾 | 任務單一張張掛著排隊 |
| Worker (執行程式) | 廚師 | 從點單夾拿單，實際去爬資料 |
| MySQL (資料庫) | 倉庫 | 把爬好的資料存起來 |
| Docker (容器) | 貨櫃 | 把廚師和工具打包，搬到哪都能跑 |

資料流：**Producer 派單 → RabbitMQ 排隊 → Worker 執行爬蟲 → 存進 MySQL**

---

### 🟢 ETF 專案核心檔案 (要用這些)

| 檔案 | 用途 |
|------|------|
| `Dockerfile` | build ETF worker image (基於老師 Ubuntu+uv base, 加 ETF code + yfinance) |
| `.dockerignore` | build 時排除 .git/.venv/.env |
| `docker-compose-etf-worker-swarm.yml` | **部署 ETF worker 用這個** (image: howardlch/etf_crawler:1.1, concurrency=1) |
| `crawler/tasks_crawler_etf_top20_v2.py` | ETF 爬蟲核心 (證交所 T86 + yfinance 批次下載) |
| `crawler/producer_crawler_etf_top20_v2.py` | 派送單日任務 (寫 DB) |
| `crawler/producer_crawler_etf_top20_v2_print.py` | 派送單日任務 (只印, 測試用) |
| `crawler/producer_crawler_etf_top20_v2_range.py` | 派送一段日期範圍 (自動跳週末) |
| `crawler/sql/etf_top20_v2_schema.sql` | DB schema + Redash 查詢範例 |
| `crawler/airflow_dags/etf_top20_dag.py` | Airflow DAG 範例 |

### 🔵 基礎服務 (必要, 老師提供)

| 檔案 | 用途 |
|------|------|
| `mysql.yml` | MySQL 部署 (port 3307 對外, 避開 Windows 3306) |
| `rabbitmq.yml` | RabbitMQ 部署 |
| `crawler/__init__.py` `config.py` `worker.py` | Celery 核心 (config 已改成自動讀 .env) |
| `docker-compose-redash-local.yml` | Redash 視覺化 (給視覺化團隊) |

### ⚪ 老師原始範例 (參考用, ETF 專案不直接使用)

`docker-compose-worker*.yml`、`docker-compose-producer*.yml`、`docker-compose-scheduler*.yml`,
以及 `crawler/` 下的 finmind / margin / bigquery 等檔案，都是老師課程的原始範例，保留供學習參考。

---

## 📂 核心檔案詳解 (想深入了解再看)

上面表格是一句話速查，這裡是每個核心檔案的完整白話說明。

### 部署檔案

**`Dockerfile` — 打造容器的施工圖。**
告訴 Docker 如何一步步建立執行環境：安裝 Python 3.11、安裝 uv 套件管理工具、安裝相依套件 (含 yfinance)、複製爬蟲程式進去。依照這張施工圖，任何電腦都能 build 出完全相同的環境。本專案基於 Ubuntu 22.04，在老師的 base 上加入 ETF 程式與 yfinance。

**`.dockerignore` — 打包時要排除的清單。**
build 容器時，版控紀錄 (.git)、虛擬環境 (.venv)、密碼檔 (.env) 等不該被打包進去。這個清單讓容器保持輕巧，也避免密碼外洩到 image 裡。

**`docker-compose-etf-worker-swarm.yml` — 啟動 worker 容器的部署說明書 (最重要)。**
定義 worker 容器如何啟動：用哪個 image (`howardlch/etf_crawler:1.1`)、啟動指令 (Celery worker + `--concurrency=1`)、連到哪個資料庫與訊息佇列。部署就是用這個檔案。

### 爬蟲程式

**`crawler/tasks_crawler_etf_top20_v2.py` — 爬蟲核心邏輯，專案心臟。**
完整流程：(1) 向證交所 T86 抓三大法人資料 → (2) 篩 ETF、排名、取前 20 → (3) 一次批次向 Yahoo 抓 20 檔股價 → (4) 算 5 日趨勢 → (5) 寫入 MySQL。兩個關鍵設計：批次下載降低被 Yahoo 限流機率；某檔股價抓不到時仍保留排名 (價格留空)，確保前 20 名完整。

**`crawler/producer_crawler_etf_top20_v2.py` — 派送單日任務 (寫入 DB)。**
派一張任務單讓 worker 爬「指定某天」並存進資料庫。不給日期則預設抓「昨天」。

**`crawler/producer_crawler_etf_top20_v2_print.py` — 測試用，只印不寫 DB。**
只把結果印在畫面上，不寫資料庫，用來確認爬出來的資料是否正確，不弄髒資料庫。

**`crawler/producer_crawler_etf_top20_v2_range.py` — 派送一段日期範圍。**
給定起訖日期，自動為每個交易日各派一張單，自動跳過週末。適合一次補齊大量歷史資料。

### 設定檔

**`crawler/config.py` — 集中管理連線設定。**
資料庫帳密、RabbitMQ 位置等都從這裡讀。已改成自動讀取根目錄的 `.env`，密碼不寫死在程式裡。

**`crawler/worker.py` — 定義並啟動 Celery worker。**
註冊 worker 可執行哪些任務，啟動時連上 RabbitMQ 等待接單。

### 下游交付檔案

**`crawler/sql/etf_top20_v2_schema.sql` — 資料表藍圖 + 查詢範例。**
資料表完整結構定義，附數個現成 SQL 查詢，視覺化同學可直接取用。

**`crawler/airflow_dags/etf_top20_dag.py` — 自動排程範本。**
給 Airflow 負責人參考，照此設定可讓爬蟲每日自動執行。

---

## 🚀 ETF 專案快速啟動

```bash
# 0. 確認 Docker Desktop 開著, swarm 已 init
docker swarm init   # 若還沒 init

# 1. 部署基礎服務
docker stack deploy -c mysql.yml mysql
docker stack deploy -c rabbitmq.yml rabbitmq

# 2. 部署 ETF worker (用我們的 image, 從 Docker Hub 拉)
docker stack deploy --with-registry-auth -c docker-compose-etf-worker-swarm.yml crawler

# 3. 確認服務都起來
docker service ls

# 4. 安裝本機 Python 環境 (派送任務用)
uv sync

# 5. 派送任務
.venv/bin/python -m crawler.producer_crawler_etf_top20_v2 2026-05-20          # 單日
.venv/bin/python -m crawler.producer_crawler_etf_top20_v2_range 2026-05-01 2026-05-20  # 區間

# 6. 看結果 (phpMyAdmin http://localhost:8080 或直接查)
#    MySQL: host=mysql:3306 (容器內) / localhost:3307 (外部), db=mydb, user=root
```

## 🐳 Docker Image 版本

- **`howardlch/etf_crawler:1.1`** = `latest`：正式版 (批次下載 + 保留 rank, concurrency=1)

重新 build / 更新 image:
```bash
docker build -t howardlch/etf_crawler:1.1 .
docker push howardlch/etf_crawler:1.1
docker service update --image howardlch/etf_crawler:1.1 crawler_etf_worker
```

## ⚠️ 重要設定 / 新手注意事項

1. **務必先啟動 Docker Desktop。** 否則所有 docker 指令都會失敗 (確認右下角鯨魚圖示是綠的)。
2. **worker 必須用 `--concurrency=1`** (compose 已設)，避免多任務並發寫入 MySQL 造成資料缺漏，請勿改大。
3. **容器內外連線位置不同：** 容器內用服務名 `mysql:3306`、`rabbitmq:5672`；本機用 `localhost:3307` (對外 port 3307，避開 Windows 佔用的 3306)。
4. **image 請用 1.1 或 latest，不要用 1.0** (1.0 有 yfinance 限流與資料缺漏問題，已淘汰)。

## Table 結構

詳見 `crawler/sql/etf_top20_v2_schema.sql`

| 欄位 | 說明 |
|------|------|
| date | 交易日期 |
| stock_id | ETF 代號 |
| stock_name | ETF 中文名稱 |
| open_price | 開盤價 |
| close_price | 收盤價 |
| trading_volume_shares | 成交股數 |
| trading_value | 成交金額 (元, 近似) |
| five_day_trend_pct | 5 日趨勢 (%) |
| rank_num | 三大法人淨買超名次 |
--------
