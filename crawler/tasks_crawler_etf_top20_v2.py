"""
ETF 三大法人買超 Top 20 - v2 版本
=====================================

資料源切換: FinMind → 證交所 OpenAPI + yfinance

差別與 v1 (FinMind 版):
- v1: 三大法人 + OHLCV + 漲幅/5日趨勢/名次/淨買超 全部存
- v2: 簡化版, 只存 8 個欄位 + rank_num
- v1: 需要 FinMind 付費 token
- v2: 完全免費 (證交所公開 API + yfinance)

資料來源:
- 證交所 T86 三大法人買賣超日報 (免費, 無 token)
  https://www.twse.com.tw/rwd/zh/fund/T86
- yfinance: 抓 OHLCV, 算 5 日趨勢
"""

import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from loguru import logger
from sqlalchemy import create_engine, text

from crawler.config import (
    MYSQL_ACCOUNT,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
)
from crawler.worker import app

# ============================================================
# 常數設定
# ============================================================
TWSE_T86_URL = "https://www.twse.com.tw/rwd/zh/fund/T86"
MYSQL_DATABASE = "mydb"
TABLE_NAME = "EtfTop20BuyInstitutionalV2"
TOP_N = 20
TREND_DAYS = 5


def get_mysql_engine():
    address = (
        f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    )
    return create_engine(address)


def is_etf_stock_id(stock_id: str) -> bool:
    sid = str(stock_id).strip()
    if sid == "0050":
        return True
    if len(sid) >= 4 and sid.startswith("00"):
        return True
    return False


def fetch_twse_t86(target_date: str, retries: int = 3) -> pd.DataFrame:
    date_str = target_date.replace("-", "")
    params = {"response": "json", "date": date_str, "selectType": "ALL"}
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
        )
    }

    for attempt in range(retries):
        try:
            resp = requests.get(TWSE_T86_URL, params=params, headers=headers, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"[fetch_twse_t86] HTTP {resp.status_code}")
                time.sleep(2)
                continue
            data = resp.json()
            if data.get("stat") != "OK":
                logger.warning(f"[fetch_twse_t86] 證交所回應: {data.get('stat')}")
                return pd.DataFrame()
            fields = data.get("fields", [])
            rows = data.get("data", [])
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows, columns=fields)
            logger.info(f"[fetch_twse_t86] {target_date} 抓到 {len(df)} 筆三大法人資料")
            return df
        except Exception as e:
            logger.error(f"[fetch_twse_t86] attempt {attempt + 1} 失敗: {e}")
            time.sleep(3)
    return pd.DataFrame()


def calculate_net_buy(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    target_col = None
    for col in df.columns:
        clean = col.replace(" ", "").replace("　", "")
        if clean == "三大法人買賣超股數":
            target_col = col
            break
    if target_col is None:
        for col in df.columns:
            if "三大法人" in col and "買賣超" in col:
                target_col = col
                break
    if target_col is None:
        logger.error(f"[calculate_net_buy] 找不到三大法人買賣超欄位: {list(df.columns)}")
        return pd.DataFrame()

    df = df.copy()
    df["net_buy"] = (
        df[target_col]
        .astype(str)
        .str.replace(",", "")
        .str.replace(" ", "")
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
        .astype("int64")
    )

    sid_col = next((c for c in df.columns if "代號" in c or "代碼" in c), None)
    name_col = next((c for c in df.columns if "名稱" in c), None)
    if sid_col is None or name_col is None:
        logger.error(f"[calculate_net_buy] 找不到代號/名稱欄位: {list(df.columns)}")
        return pd.DataFrame()

    return df[[sid_col, name_col, "net_buy"]].rename(
        columns={sid_col: "stock_id", name_col: "stock_name"}
    )


def fetch_ohlcv_yfinance(stock_id: str, target_date: str, lookback_days: int = 15) -> pd.DataFrame:
    try:
        import yfinance as yf
    except ImportError:
        logger.error("[fetch_ohlcv_yfinance] yfinance 未安裝, 請執行 uv sync")
        return pd.DataFrame()

    ticker = f"{stock_id}.TW"
    end_dt = datetime.strptime(target_date, "%Y-%m-%d")
    end_str = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    start_str = (end_dt - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    try:
        df = yf.download(ticker, start=start_str, end=end_str, progress=False, auto_adjust=False)
        if df.empty:
            return df
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.reset_index()
        return df
    except Exception as e:
        logger.warning(f"[fetch_ohlcv_yfinance] {ticker} 失敗: {e}")
        return pd.DataFrame()


@app.task()
def crawler_etf_top20_v2(target_date: str = None):
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"========== ETF Top20 v2 開始: {target_date} ==========")

    raw_df = fetch_twse_t86(target_date)
    if raw_df.empty:
        logger.error(f"{target_date} 無 T86 資料 (可能非交易日)")
        return

    inst_df = calculate_net_buy(raw_df)
    if inst_df.empty:
        return

    inst_df["stock_id"] = inst_df["stock_id"].str.strip()
    inst_df["stock_name"] = inst_df["stock_name"].str.strip()
    etf_df = inst_df[inst_df["stock_id"].apply(is_etf_stock_id)].copy()
    logger.info(f"過濾後共 {len(etf_df)} 檔 ETF 有三大法人資料")
    if etf_df.empty:
        logger.error("過濾後無 ETF 資料")
        return

    top_df = (
        etf_df.sort_values("net_buy", ascending=False)
        .head(TOP_N)
        .reset_index(drop=True)
    )
    top_df["rank_num"] = top_df.index + 1
    logger.info(f"取出前 {TOP_N} 名 ETF")

    enriched = []
    for _, row in top_df.iterrows():
        sid = row["stock_id"]
        logger.info(f"[yfinance] 抓 {sid} {row['stock_name']} (rank {row['rank_num']})")
        price_df = fetch_ohlcv_yfinance(sid, target_date, lookback_days=15)
        if price_df.empty:
            logger.warning(f"  {sid} 無 OHLCV, 跳過")
            continue

        price_df["Date"] = pd.to_datetime(price_df["Date"])
        target_dt = pd.to_datetime(target_date)
        price_df = price_df[price_df["Date"] <= target_dt].sort_values("Date")
        if len(price_df) == 0:
            continue

        today = price_df.iloc[-1]

        if len(price_df) >= TREND_DAYS + 1:
            past_close = price_df.iloc[-(TREND_DAYS + 1)]["Close"]
            five_day_trend = (today["Close"] - past_close) / past_close * 100
        else:
            five_day_trend = None

        volume_shares = int(today["Volume"]) if pd.notna(today["Volume"]) else 0
        trading_value = int(volume_shares * float(today["Close"]))

        enriched.append({
            "date": target_date,
            "stock_id": sid,
            "stock_name": row["stock_name"],
            "open_price": round(float(today["Open"]), 4),
            "close_price": round(float(today["Close"]), 4),
            "trading_volume_shares": volume_shares,
            "trading_value": trading_value,
            "five_day_trend_pct": (
                round(five_day_trend, 4) if five_day_trend is not None else None
            ),
            "rank_num": int(row["rank_num"]),
        })
        time.sleep(0.3)

    if not enriched:
        logger.error("最終無可寫入資料")
        return

    final_df = pd.DataFrame(enriched)
    upload_to_mysql_v2(final_df, target_date)
    logger.info(f"========== ETF Top20 v2 完成: {target_date} ==========")


def upload_to_mysql_v2(df: pd.DataFrame, target_date: str):
    engine = get_mysql_engine()
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
        `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
        `date` DATE NOT NULL,
        `stock_id` VARCHAR(10) NOT NULL,
        `stock_name` VARCHAR(50),
        `open_price` FLOAT,
        `close_price` FLOAT,
        `trading_volume_shares` BIGINT,
        `trading_value` BIGINT,
        `five_day_trend_pct` FLOAT,
        `rank_num` INT,
        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY `uk_date_stock` (`date`, `stock_id`),
        KEY `idx_date` (`date`),
        KEY `idx_rank` (`date`, `rank_num`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))
        conn.execute(
            text(f"DELETE FROM `{TABLE_NAME}` WHERE `date` = :d"),
            {"d": target_date},
        )

    df.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False)
    logger.info(f"[upload_v2] 寫入 {len(df)} 筆到 {TABLE_NAME}")


@app.task()
def crawler_etf_top20_v2_print(target_date: str = None):
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"[PRINT v2] {target_date}")

    raw_df = fetch_twse_t86(target_date)
    if raw_df.empty:
        return
    inst_df = calculate_net_buy(raw_df)
    if inst_df.empty:
        return
    inst_df["stock_id"] = inst_df["stock_id"].str.strip()
    inst_df["stock_name"] = inst_df["stock_name"].str.strip()
    etf_df = inst_df[inst_df["stock_id"].apply(is_etf_stock_id)].copy()
    top_df = (
        etf_df.sort_values("net_buy", ascending=False)
        .head(TOP_N)
        .reset_index(drop=True)
    )
    top_df["rank_num"] = top_df.index + 1
    print(top_df.to_string(index=False))
    logger.info(f"[PRINT v2] 共 {len(top_df)} 檔")