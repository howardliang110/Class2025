import time
from datetime import datetime, timedelta

import pandas as pd
import requests
import yfinance as yf
from loguru import logger
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.dialects.mysql import (
    insert,
)  # 專用於 MySQL 的 insert 語法，可支援 on_duplicate_key_update

from crawler.config import MYSQL_ACCOUNT, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT
from crawler.worker import app


# 教學用: 最簡單版本, 只抓資料並印出, 不上傳資料庫
# 適合初學者第一次派送任務時驗證流程
@app.task()
def crawler_etf_top20_print(date: str = None):
    # 輸入 date 格式 YYYYMMDD, 例如 20260508
    # 若沒傳, 預設抓今天 (假日會回傳空資料, 由呼叫端處理)
    if date is None:
        date = datetime.now().strftime("%Y%m%d")

    logger.info(f"[crawler_etf_top20_print] start, date={date}")

    df = fetch_twse_etf_volume(date)
    if df.empty:
        logger.warning(f"{date} 無 ETF 成交資料 (可能為假日)")
        return

    df_top20 = df.sort_values("Trading_Volume", ascending=False).head(20).reset_index(drop=True)
    df_top20["rank"] = df_top20.index + 1

    print(df_top20)


# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
# 正式版: 抓資料 → 排序取前 20 → 用 yfinance 補資料 → 寫入 MySQL
@app.task()
def crawler_etf_top20(date: str = None):
    if date is None:
        date = datetime.now().strftime("%Y%m%d")

    logger.info(f"[crawler_etf_top20] start, date={date}")

    # Step 1: 從 TWSE 抓當日所有 ETF 的成交資料
    df = fetch_twse_etf_volume(date)
    if df.empty:
        logger.warning(f"{date} 無 ETF 成交資料 (可能為假日)")
        return

    # Step 2: 依成交量排序, 取前 20 名
    df_top20 = df.sort_values("Trading_Volume", ascending=False).head(20).reset_index(drop=True)
    df_top20["rank"] = df_top20.index + 1

    # Step 3: 用 yfinance 補上「漲跌幅」欄位 (TWSE API 沒有直接給)
    df_top20 = enrich_with_yfinance(df_top20, date)

    print(df_top20)

    # Step 4: 寫入 MySQL, 主鍵是 (date, stock_id), 重複會自動 update
    upload_etf_top20_to_mysql(df_top20)


# ---------------------------------------------------------------
# 以下是 task 會用到的工具函式 (helper functions)
# ---------------------------------------------------------------


def fetch_twse_etf_volume(date: str) -> pd.DataFrame:
    """從 TWSE (證交所) 抓取指定日期的 ETF 成交資料

    Args:
        date: YYYYMMDD 格式, ex: 20260508

    Returns:
        DataFrame, 含每支 ETF 的當日成交資訊
    """
    # TWSE 官方 API, type=0099P 代表 ETF 類股
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/BFIAUU"
    params = {
        "date": date,
        "type": "0099P",  # 0099P = ETF
        "response": "json",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
    }

    resp = requests.get(url, params=params, headers=headers, timeout=30)
    data = resp.json()

    # TWSE 假日 / 無資料時 stat 不會是 OK
    if data.get("stat") != "OK":
        logger.info(f"TWSE API 回應: {data.get('stat')}")
        return pd.DataFrame()

    rows = data.get("data", [])
    if not rows:
        return pd.DataFrame()

    # 欄位順序對應 TWSE API 回傳格式
    df = pd.DataFrame(
        rows,
        columns=[
            "stock_id",
            "stock_name",
            "Trading_Volume",
            "Trading_turnover",
            "Trading_money",
            "open",
            "max",
            "min",
            "close",
            "spread_sign",
            "spread",
            "last_bid_price",
            "last_bid_volume",
            "last_ask_price",
            "last_ask_volume",
            "PE_ratio",
        ],
    )

    # 加上 date 欄位 (轉成 YYYY-MM-DD 格式, 方便存入 MySQL DATE 型態)
    df["date"] = pd.to_datetime(date).strftime("%Y-%m-%d")

    # 數字欄位清洗: 拿掉逗號、處理 "--", 轉型
    int_cols = ["Trading_Volume", "Trading_money", "Trading_turnover"]
    float_cols = ["open", "max", "min", "close"]

    for col in int_cols:
        df[col] = (
            df[col].astype(str).str.replace(",", "").str.replace("--", "0").astype("int64")
        )
    for col in float_cols:
        df[col] = (
            df[col].astype(str).str.replace(",", "").str.replace("--", "0").astype(float)
        )

    # 只保留需要的欄位 (對應後續 MySQL table 結構)
    df = df[
        [
            "date",
            "stock_id",
            "stock_name",
            "Trading_Volume",
            "Trading_money",
            "Trading_turnover",
            "open",
            "max",
            "min",
            "close",
        ]
    ]

    return df


def enrich_with_yfinance(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """用 yfinance 補上「漲跌幅 spread」欄位

    yfinance 拿前一交易日收盤價, 算當日漲跌幅:
        spread = close - prev_close

    這樣組員後續做分析時能直接看到漲跌, 不用再算
    """
    target_date = pd.to_datetime(date).strftime("%Y-%m-%d")
    spreads = []

    for stock_id in df["stock_id"]:
        try:
            # 台股 ETF 在 yahoo finance 的代號是 {stock_id}.TW
            ticker = yf.Ticker(f"{stock_id}.TW")
            # 抓前後 5 天, 確保能拿到「前一交易日」的資料
            hist = ticker.history(
                start=(pd.to_datetime(date) - timedelta(days=7)).strftime("%Y-%m-%d"),
                end=(pd.to_datetime(date) + timedelta(days=1)).strftime("%Y-%m-%d"),
            )
            if len(hist) < 2:
                spreads.append(0.0)
                continue
            # 取最後兩筆 (前一交易日 + 當日)
            prev_close = hist["Close"].iloc[-2]
            today_close = hist["Close"].iloc[-1]
            spreads.append(round(today_close - prev_close, 2))
            time.sleep(0.2)  # 避免太頻繁打 yfinance
        except Exception as e:
            logger.warning(f"yfinance 抓 {stock_id} 失敗: {e}")
            spreads.append(0.0)

    df["spread"] = spreads
    return df


def upload_etf_top20_to_mysql(df: pd.DataFrame):
    """將 ETF 前 20 名寫入 MySQL, 重複資料以 on_duplicate_key_update 處理

    主鍵設計: (date, stock_id)
    這樣同一天同一支 ETF 不會重複寫入, 重跑會自動覆蓋更新
    """
    # 上傳到 tibame database, 同學可切換成自己的 database
    address = (
        f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/tibame"
    )
    engine = create_engine(address)

    # 定義 table 結構, 對應 MySQL 中的 EtfDailyTop20 表
    metadata = MetaData()
    etf_top20_table = Table(
        "EtfDailyTop20",
        metadata,
        Column("date", Date, primary_key=True),
        Column("stock_id", String(20), primary_key=True),
        Column("stock_name", String(50)),
        Column("rank", Integer),
        Column("Trading_Volume", BigInteger),
        Column("Trading_money", BigInteger),
        Column("Trading_turnover", BigInteger),
        Column("open", Float),
        Column("max", Float),
        Column("min", Float),
        Column("close", Float),
        Column("spread", Float),
    )
    # ✅ 自動建立 table (若不存在才建立)
    metadata.create_all(engine)

    # 一筆一筆 upsert (主鍵衝突就 update)
    for _, row in df.iterrows():
        insert_stmt = insert(etf_top20_table).values(**row.to_dict())
        update_stmt = insert_stmt.on_duplicate_key_update(
            **{
                col.name: insert_stmt.inserted[col.name]
                for col in etf_top20_table.columns
            }
        )
        with engine.begin() as conn:
            conn.execute(update_stmt)

    logger.info(f"[upload_etf_top20_to_mysql] OK, wrote {len(df)} rows")