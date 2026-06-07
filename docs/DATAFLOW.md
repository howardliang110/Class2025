# 資料收集 → 資料儲存 完整流程教學

> 本文件對照「**緯育大數據專案架構圖**」中的 🟢 **資料收集** 與 🔵 **資料儲存** 兩塊區域，逐步說明同學如何用這個 repo 把資料從證交所抓回來、存進 MySQL。

---

## 🗺️ 對照架構圖
證交所 T86 + yfinance │ ▼ ┌─ 資料收集 (🟢 本專案核心) ──────┐ │ │ │ Producer │ │ (派任務 Script) │ │ │ │ │ ▼ │ │ RabbitMQ │ │ (任務佇列) │ │ │ │ │ ▼ │ │ Celery Worker (Docker) │ │ (爬蟲 Request) │ └──────────┬───────────────────────┘ │ ▼ ┌─ 資料儲存 (🔵 本專案核心) ──────┐ │ MySQL │ │ EtfTop20BuyInstitutionalV2 │ └──────────────────────────────────┘ │ ▼ API / 視覺化 / Airflow (下游隊友負責)
---

## 📂 流程上每個元件對應的程式檔案

| 架構圖元件 | 角色 | 本專案檔案 |
|----------|------|---------|
| **Producer** | 派送任務「告訴 worker 要爬哪天」 | `crawler/producer_crawler_etf_top20_v2.py` <br> `crawler/producer_crawler_etf_top20_v2_range.py` |
| **RabbitMQ** | 訊息佇列（任務排隊區） | `rabbitmq.yml` (Docker 部署) |
| **Celery Worker** | 接任務、跑爬蟲、寫 DB | `docker-compose-etf-worker-swarm.yml` (部署) <br> `crawler/worker.py` (Celery 入口) <br> `crawler/tasks_crawler_etf_top20_v2.py` (爬蟲邏輯) |
| **爬蟲 Request** | 真正去打 API 的邏輯 | `tasks_crawler_etf_top20_v2.py` 內的 `fetch_twse_t86()` 與 `fetch_ohlcv_batch()` |
| **MySQL** | 結構化資料庫 | `mysql.yml` (Docker 部署) <br> `crawler/sql/etf_top20_v2_schema.sql` (Schema 定義) |

---

## 🚀 同學要怎麼跑這條 pipeline

### 前置（只要做一次）

```bash
# 1. clone repo
git clone https://github.com/howardliang110/Class2025
cd Class2025

# 2. 環境設定 (照老師流程: local.ini → genenv.py → .env)
cp local.ini.example local.ini
# 開 local.ini, 把 <YOUR_MYSQL_PASSWORD> 改成你的密碼
ENV=DEV python3 genenv.py

# 3. 安裝 Python 環境
uv sync

# 4. 啟動 Docker Swarm (如還沒)
docker swarm init   # 第一次才需要

# 5. 部署基礎服務 (MySQL + RabbitMQ)
docker stack deploy -c mysql.yml mysql
docker stack deploy -c rabbitmq-network.yml rabbitmq

# 6. 部署 ETF Worker (從 Docker Hub 拉 image)
docker stack deploy --with-registry-auth -c docker-compose-etf-worker-swarm.yml crawler

# 7. 確認所有 service 都跑起來
docker service ls
```

### 派任務（每次想爬資料時做）

#### 方式 A：爬單一日期

```bash
# 派昨天 (預設)
.venv/bin/python -m crawler.producer_crawler_etf_top20_v2

# 派指定日期
.venv/bin/python -m crawler.producer_crawler_etf_top20_v2 2026-06-05
```

#### 方式 B：爬一段範圍

```bash
# 自動跳過週末
.venv/bin/python -m crawler.producer_crawler_etf_top20_v2_range 2026-01-01 2026-06-05
```

#### 方式 C：印出來測試 (不寫 DB)

```bash
# 想看資料但不弄髒資料庫時用
.venv/bin/python -m crawler.producer_crawler_etf_top20_v2_print 2026-06-05
```

---

## 🔄 一次任務內部的詳細流程

當你執行 `producer` 後，背後發生這些事：
[你的電腦] [Docker Swarm] ───────── ──────────────────────────────────── producer
│ │ 1. 包裝任務 │ {date: "2026-06-05"} │ └──── 2. send_task ───────► RabbitMQ (任務佇列) │ │ 3. 排隊等 worker ▼ Celery Worker (容器) │ │ 4. 接到任務 │ │ 5. 呼叫 fetch_twse_t86() │ ↓ │ 證交所 T86 API │ (回傳 14000+ 筆三大法人) │ │ 6. 篩 ETF + 排名取 Top 20 │ 呼叫 calculate_net_buy() │ │ 7. 呼叫 fetch_ohlcv_batch() │ ↓ │ Yahoo (yfinance) │ (一次批次拉 20 檔股價) │ │ 8. 算 5 日趨勢 │ 呼叫 compute_metrics() │ │ 9. upload_to_mysql_v2() ▼ MySQL (EtfTop20BuyInstitutionalV2) │ │ 每天 20 筆乾淨資料
---

## 🔍 驗證資料有沒有真的進 MySQL

### 方法 A：用 phpMyAdmin（網頁，最直覺）
打開 http://localhost:8080 登入 (root / Aa1234567890 或你自己設的) 左側選 mydb → EtfTop20BuyInstitutionalV2 → Browse
### 方法 B：用 Python 查（最方便）

```python
from sqlalchemy import create_engine, text

engine = create_engine('mysql+pymysql://root:<密碼>@127.0.0.1:3307/mydb')
with engine.connect() as conn:
    # 看最新一天前 5 名
    rows = conn.execute(text("""
        SELECT date, rank_num, stock_id, stock_name,
               foreign_net, trust_net, dealer_net
        FROM EtfTop20BuyInstitutionalV2
        WHERE date = (SELECT MAX(date) FROM EtfTop20BuyInstitutionalV2)
        ORDER BY rank_num LIMIT 5
    """)).fetchall()
    for r in rows:
        print(r)
```

### 方法 C：用 Docker exec 直接查（最快）

```bash
docker exec $(docker ps -q -f name=mysql_mysql) mysql -uroot -p<密碼> -e "
  SELECT COUNT(*), MAX(date), MIN(date) FROM mydb.EtfTop20BuyInstitutionalV2;
"
```

---

## 📋 資料表欄位清單

完整 schema 定義在 [`crawler/sql/etf_top20_v2_schema.sql`](../crawler/sql/etf_top20_v2_schema.sql)。

| 欄位 | 型別 | 說明 |
|------|------|------|
| `id` | BIGINT | 自動編號 |
| `date` | DATE | 交易日期 |
| `stock_id` | VARCHAR(10) | ETF 代號 |
| `stock_name` | VARCHAR(50) | ETF 中文名稱 |
| `open_price` | FLOAT | 開盤價 |
| `close_price` | FLOAT | 收盤價 |
| `trading_volume_shares` | BIGINT | 成交股數 |
| `trading_value` | BIGINT | 成交金額（元，近似）|
| `five_day_trend_pct` | FLOAT | 5 日趨勢（%）|
| `rank_num` | INT | 三大法人淨買超名次（1-20）|
| **`foreign_buy`** | BIGINT | **外資買進股數** |
| **`foreign_sell`** | BIGINT | **外資賣出股數** |
| **`foreign_net`** | BIGINT | **外資買賣超股數** |
| **`trust_buy`** | BIGINT | **投信買進股數** |
| **`trust_sell`** | BIGINT | **投信賣出股數** |
| **`trust_net`** | BIGINT | **投信買賣超股數** |
| **`dealer_buy`** | BIGINT | **自營商買進股數** |
| **`dealer_sell`** | BIGINT | **自營商賣出股數** |
| **`dealer_net`** | BIGINT | **自營商買賣超股數** |
| `created_at` | TIMESTAMP | 寫入時間 |

> **粗體欄位為新增**：三大法人各別買賣量，方便視覺化團隊分組分析。

---

## ⚠️ 常見坑

1. **Docker Desktop 沒開**：所有指令都會卡住，先確認右下角鯨魚圖示是綠色
2. **port 3306 被佔用**：本機若已裝 MySQL，本專案用 3307 對外（容器內 3306）
3. **concurrency 不要改**：worker 必須 `--concurrency=1` 避免並發寫入衝突
4. **容器內外連線名稱不同**：容器內用 service name (`mysql:3306`, `rabbitmq:5672`)，本機用 `localhost:3307` / `localhost:5672`
5. **新欄位是 NULL**：表示資料是用舊版 image 爬的，重派任務即可填值（DELETE+INSERT 機制）

---

## 🔗 配套工具

| 角色 | repo |
|------|------|
| Airflow 自動排程 | https://github.com/howardliang110/dataflow |
| API 提供 | （隊友 A 負責）|
| 視覺化 | （隊友 B 負責，可直接讀本表）|

