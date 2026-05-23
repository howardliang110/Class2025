# ETF Top 20 v2 — 證交所 + yfinance (免費版)
> 本專案在老師的 Celery + RabbitMQ + MySQL + Swarm 架構上，新增 ETF Top 20 三大法人爬蟲。
> 老師的原始範例檔案都保留供參考，但 **ETF 專案實際只用下列標 🟢 的檔案**。

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

## ⚠️ 重要設定

- worker 必須用 **`--concurrency=1`** (compose 已設)，避免多任務並發寫入 MySQL 造成資料缺漏
- 容器內連線用服務名: `mysql:3306`、`rabbitmq:5672` (非 host 的 127.0.0.1)
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

