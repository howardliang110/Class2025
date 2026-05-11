# ETF 三大法人買超 Top 20 爬蟲模組

> 每日爬取台股市場中, 三大法人（外資、投信、自營商）淨買超**前 20 名 ETF** 的明細資料,
> 包含 OHLCV、當日漲幅、5 日趨勢, 寫入 MySQL 供 Redash 視覺化與 Airflow 排程使用。

---

## 📦 模組組成

| 檔案 | 角色 | 說明 |
|------|------|------|
| `crawler/tasks_crawler_etf_top20.py` | **Celery Task** | 核心爬蟲邏輯 (寫 DB 版 + Print 版) |
| `crawler/producer_crawler_etf_top20.py` | **Producer** | 手動派送任務到 RabbitMQ |
| `crawler/producer_crawler_etf_top20_print.py` | **Producer (Print)** | 測試用, 不寫 DB |
| `crawler/scheduler_etf_top20.py` | **Scheduler** | 每日 18:00 自動派送 (APScheduler) |
| `crawler/airflow_dags/etf_top20_dag.py` | **Airflow DAG** | 給 Airflow 團隊的範例 DAG |
| `crawler/sql/etf_top20_schema.sql` | **SQL Schema** | 表結構 + Redash 查詢範例 |

---

## 🗄️ 輸出資料表 `EtfTop20BuyInstitutional`

| 欄位 | 型別 | 說明 |
|------|------|------|
| `date` | DATE | 交易日期 |
| `stock_id` | VARCHAR(10) | ETF 代號 |
| `stock_name` | VARCHAR(50) | ETF 中文名稱 |
| `trading_volume` | BIGINT | 當日交易量 (股) |
| `open_price` | FLOAT | 開盤價 |
| `close_price` | FLOAT | 收盤價 |
| `high_price` | FLOAT | 最高價 |
| `low_price` | FLOAT | 最低價 |
| `daily_change_pct` | FLOAT | 當日漲跌幅 (%) |
| `five_day_trend_pct` | FLOAT | 5 日趨勢漲跌幅 (%) |
| `institutional_net_buy` | BIGINT | 三大法人淨買超 (股) |
| `rank_num` | INT | 當日名次 (1-20) |

> Unique key: `(date, stock_id)` — 同日重跑會覆寫, 不會重複。

---

## 🚀 快速啟動

### 1. 啟動基礎服務

```bash
cd ~/Class2025_backup

# 確認 .env 已建立 (從 .env.example 複製)
cp .env.example .env

# 啟動 MySQL (含 phpMyAdmin)
docker network create my_network 2>/dev/null || true
docker compose -f mysql.yml up -d

# 啟動 RabbitMQ
docker compose -f rabbitmq.yml up -d
# 或 rabbitmq-network.yml

# 設定 Image 版本
export DOCKER_IMAGE_VERSION=0.0.9

# 啟動 Celery worker (twse, tpex queue)
docker compose -f docker-compose-worker-network-version.yml up -d
```

驗證:

```bash
# MySQL
docker exec class2025_backup-mysql-1 mysql -u root -pAb1234567890 -e "SHOW DATABASES;"

# RabbitMQ 管理介面
open http://localhost:15672    # worker / worker

# phpMyAdmin
open http://localhost:8000     # root / Ab1234567890
```

---

### 2. 測試爬蟲 (Print 模式, 不寫 DB)

```bash
# 派送任務 (用昨天的資料)
uv run python -m crawler.producer_crawler_etf_top20_print

# 指定日期
uv run python -m crawler.producer_crawler_etf_top20_print 2025-05-12

# 看 worker log 確認結果
docker compose -f docker-compose-worker-network-version.yml logs -f crawler_twse
```

---

### 3. 正式執行 (寫入 MySQL)

```bash
# 手動觸發 (預設昨天)
uv run python -m crawler.producer_crawler_etf_top20

# 指定日期
uv run python -m crawler.producer_crawler_etf_top20 2025-05-12
```

查詢結果:

```bash
docker exec -it class2025_backup-mysql-1 mysql -u root -pAb1234567890 mydb \
  -e "SELECT rank_num, stock_id, stock_name, close_price, daily_change_pct, five_day_trend_pct
      FROM EtfTop20BuyInstitutional
      WHERE date = (SELECT MAX(date) FROM EtfTop20BuyInstitutional)
      ORDER BY rank_num;"
```

---

### 4. 啟用自動排程 (選擇 A 或 B)

**選項 A: 用內建 APScheduler**

```bash
uv run python -m crawler.scheduler_etf_top20
# 平日 18:00 自動執行
```

**選項 B: 用 Airflow (交給 Airflow 負責人)**

把 `crawler/airflow_dags/etf_top20_dag.py` 放到 Airflow 的 `dags/` 目錄。

---

## 📊 給「視覺化負責人 (Redash)」的資訊

### 連線資訊

- **Host**: `mysql` (Docker 內) / `<server_ip>` (對外)
- **Port**: 3306
- **Database**: `mydb`
- **User**: `root`
- **Password**: 見 `.env` (預設 `Ab1234567890`, 正式環境請改)
- **Table**: `EtfTop20BuyInstitutional`

### 建議圖表

1. **每日 Top 20 排行榜** — 表格 (今日資料)
2. **Top 20 進榜次數熱度圖** — 過去 30 天進榜頻率
3. **某檔 ETF 排名走勢** — 折線圖, 日期 vs rank_num
4. **5 日趨勢分佈** — 直方圖, 看市場動能集中度
5. **三大法人總買超金額** — 趨勢線, 整體市場熱度

詳細 SQL 範例見 `crawler/sql/etf_top20_schema.sql`。

---

## ⏰ 給「Airflow 負責人」的資訊

範例 DAG: `crawler/airflow_dags/etf_top20_dag.py`

- DAG ID: `etf_top20_institutional_crawler`
- 排程: 平日 (Mon-Fri) UTC 10:00 (= Asia/Taipei 18:00)
- 任務: 派送 Celery task 到 `twse` queue
- 重試: 失敗時重試 2 次, 間隔 10 分鐘
- catchup=False: 不補跑歷史日期

部署條件:
- Airflow 需要安裝本專案依賴 (`pyproject.toml`)
- Airflow worker 要能連到同一個 RabbitMQ
- 環境變數 (MYSQL_*, RABBITMQ_*) 需正確設定

替代方案: 也可保留現有 `scheduler_etf_top20.py`, 不一定要遷移到 Airflow。

---

## 🔧 故障排除

| 症狀 | 可能原因 | 解法 |
|------|----------|------|
| `Access denied for user 'root'` | `.env` 密碼與 `mysql.yml` 不一致 | 統一密碼, 必要時清 volume 重建 |
| `unable to get image ':'` | `DOCKER_IMAGE_VERSION` 未設 | `export DOCKER_IMAGE_VERSION=0.0.9` |
| Worker 連不上 RabbitMQ | network 沒建立 / 帳密錯 | `docker network create my_network` |
| FinMind 抓不到資料 | 假日 / 資料未更新 / rate limit | 改抓昨天, 或晚點重試 |
| `industry_category` 找不到 ETF | FinMind API 欄位變更 | 檢查 `TaiwanStockInfo` 回傳結構 |

---

## 🗺️ 架構圖

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│  Scheduler  │─────▶│   RabbitMQ   │─────▶│   Worker    │
│ (APScheduler│      │  (twse queue)│      │  (Celery)   │
│  或 Airflow)│      └──────────────┘      └──────┬──────┘
└─────────────┘                                   │
                                                  ▼
                                         ┌────────────────┐
                                         │  FinMind API   │
                                         └────────┬───────┘
                                                  │
                                                  ▼
                                         ┌────────────────┐
                                         │     MySQL      │
                                         │ EtfTop20...    │
                                         └────────┬───────┘
                                                  │
                          ┌───────────────────────┼───────────────────────┐
                          ▼                       ▼                       ▼
                  ┌──────────────┐        ┌──────────────┐        ┌──────────────┐
                  │   Redash     │        │ phpMyAdmin   │        │ 下游應用     │
                  │ (視覺化)     │        │ (DB 管理)    │        │              │
                  └──────────────┘        └──────────────┘        └──────────────┘
```

---

## 📧 聯絡

- 模組維護: <你的名字 / Email>
- Repo: https://github.com/howard010-pixel/Class2025
