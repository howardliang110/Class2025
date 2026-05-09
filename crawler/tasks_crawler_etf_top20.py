import time
from datetime import datetime

import pandas as pd
import requests
import yfinance as yf
from loguru import logger
from sqlalchemy import (
    BigInteger, Column, Date, Float, Integer, MetaData, String, Table, create_engine,
)
from sqlalchemy.dialects.mysql import insert

from crawler.config import MYSQL_ACCOUNT, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT
from crawler.worker import app


# 教學用: 只 print 不寫資料庫
@app.task()
def crawler_etf_top20_print(date: str = None):
    logger.info("[crawler_etf_top20_print] start")
    df = fetch_twse_etf_volume()
    if df.empty:
        logger.warning("無 ETF 資料")
        return
    df_top20 = df.sort_values("Trading_Volume", ascending=False).head(20).reset_index(drop=True)
    df_top20["rank"] = df_top20.index + 1
    print(df_top20)


# 正式版: 抓 → 排序 → 補 yfinance 漲跌 → 寫 MySQL
@app.task()
def crawler_etf_top20(date: str = None):
    logger.info("[crawler_etf_top20] start")
    df = fetch_twse_etf_volume()
    if df.empty:
        logger.warning("無 ETF 資料, 結束")
        return
    df_top20 = df.sort_values("Trading_Volume", ascending=False).head(20).reset_index(drop=True)
    df_top20["rank"] = df_top20.index + 1
    # API 已經有 Change 欄位 (= spread), 但仍用 yfinance 做交叉驗證 / 教學示範
    df_top20 = enrich_with_yfinance(df_top20)
    print(df_top20)
    upload_etf_top20_to_mysql(df_top20)


def fetch_twse_etf_volume() -> pd.DataFrame:
    """從 TWSE OpenAPI STOCK_DAY_ALL 抓最新一交易日資料, 篩選出 ETF

    Endpoint: https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL
    回傳: 全部上市標的 ~1357 筆 (含個股 + ETF)
    篩選: Code 以 '0' 開頭即為 ETF (例如 0050, 00940, 00400A)
    """
    url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        rows = resp.json()
    except Exception as e:
        logger.error(f"TWSE API 失敗: {e}")
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    logger.info(f"TWSE 全部標的: {len(df)} 筆")

    # ★ 篩選出 ETF: Code 以 '0' 開頭 (個股都是 1xxx ~ 9xxx)
    df = df[df["Code"].str.startswith("0")].reset_index(drop=True)
    logger.info(f"篩選出 ETF: {len(df)} 筆")

    # 欄位對應 (TWSE 官方欄位 → 我們的 schema)
    df = df.rename(columns={
        "Code": "stock_id",
        "Name": "stock_name",
        "TradeVolume": "Trading_Volume",
        "TradeValue": "Trading_money",
        "Transaction": "Trading_turnover",
        "OpeningPrice": "open",
        "HighestPrice": "max",
        "LowestPrice": "min",
        "ClosingPrice": "close",
        "Change": "spread_api",  # API 直接給的漲跌, 等等跟 yfinance 算的對照
    })

    # 補上 date — TWSE 的 Date 是民國年 (例如 1140509), 用今天日期當 fallback
    df["date"] = datetime.now().strftime("%Y-%m-%d")

    # 數字清洗
    int_cols = ["Trading_Volume", "Trading_money", "Trading_turnover"]
    float_cols = ["open", "max", "min", "close", "spread_api"]

    def to_int(s):
        return (
            s.astype(str).str.replace(",", "")
            .str.replace("--", "0").replace("", "0").astype("int64")
        )

    def to_float(s):
        return (
            s.astype(str).str.replace(",", "")
            .str.replace("--", "0").replace("", "0").astype(float)
        )

    for col in int_cols:
        df[col] = to_int(df[col])
    for col in float_cols:
        df[col] = to_float(df[col])

    df = df[[
        "date", "stock_id", "stock_name",
        "Trading_Volume", "Trading_money", "Trading_turnover",
        "open", "max", "min", "close", "spread_api",
    ]]
    return df


def enrich_with_yfinance(df: pd.DataFrame) -> pd.DataFrame:
    """用 yfinance 算當日漲跌 (= 當日 close - 前一交易日 close)

    note: API 已經有 spread_api, 這裡用 yfinance 做交叉驗證, 並以 yfinance 結果寫入 spread 欄位
    """
    spreads = []
    for stock_id in df["stock_id"]:
        try:
            ticker = yf.Ticker(f"{stock_id}.TW")
            hist = ticker.history(period="5d")
            if len(hist) < 2:
                # 抓不到 (新上市 ETF / yfinance 沒收錄), fallback 用 API 給的 spread_api
                spreads.append(None)
                continue
            prev_close = hist["Close"].iloc[-2]
            today_close = hist["Close"].iloc[-1]
            spreads.append(round(float(today_close - prev_close), 2))
            time.sleep(0.2)
        except Exception as e:
            logger.warning(f"yfinance 抓 {stock_id} 失敗: {e}")
            spreads.append(None)

    df["spread"] = spreads
    # 若 yfinance 抓不到, 用 API 的 spread_api 補
    df["spread"] = df["spread"].fillna(df["spread_api"])
    # 拿掉中介欄位
    df = df.drop(columns=["spread_api"])
    return df


def upload_etf_top20_to_mysql(df: pd.DataFrame):
    """寫入 MySQL, 主鍵 (date, stock_id), 重複會 update"""
    address = (
        f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/tibame"
    )
    engine = create_engine(address)

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
    metadata.create_all(engine)

    for _, row in df.iterrows():
        insert_stmt = insert(etf_top20_table).values(**row.to_dict())
        update_stmt = insert_stmt.on_duplicate_key_update(**{
            col.name: insert_stmt.inserted[col.name]
            for col in etf_top20_table.columns
        })
        with engine.begin() as conn:
            conn.execute(update_stmt)

    logger.info(f"[upload_etf_top20_to_mysql] OK, wrote {len(df)} rows")
