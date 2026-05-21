# ETF Top 20 v2 — 證交所 + yfinance (免費版)

## 使用方式

```bash
cd ~/Class2025_backup
uv sync  # 安裝 yfinance

# 啟動 worker
.venv/bin/celery -A crawler.worker worker --loglevel=info --hostname=local-dev@%h -Q twse

# 在新 terminal 派送
.venv/bin/python -m crawler.producer_crawler_etf_top20_v2_print 2025-05-12  # 測試
.venv/bin/python -m crawler.producer_crawler_etf_top20_v2 2025-05-12        # 正式
```

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
