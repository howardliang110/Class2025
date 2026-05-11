"""
ETF 三大法人買超前 20 名爬蟲任務
===================================

每日抓取三大法人（外資、投信、自營商）對所有 ETF 的買賣超資料，
取淨買超前 20 名，並補充每檔 ETF 的當日 OHLCV、漲跌幅、5 日趨勢等資訊，
最後寫入 MySQL，供後續視覺化（Redash）和排程（Airflow）使用。

資料來源: FinMind API (https://finmindtrade.com/)
- TaiwanStockInstitutionalInvestorsBuySell: 三大法人買賣超
- TaiwanStockPrice: 日線 OHLCV
- TaiwanStockInfo: 股票基本資料（含中文名稱與類型）

執行流程:
    1. 取得指定日期所有 ETF 的三大法人買賣超
    2. 計算淨買超 (Buy - Sell) 並排序取前 20
    3. 為每檔 ETF 抓取近 N 日股價，計算當日漲幅和 5 日趨勢
    4. 合併資料並寫入 MySQL 表 EtfTop20BuyInstitutional
"""

import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from loguru import logger
from sqlalchemy import create_engine

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
FINMIND_API = "https://api.finmindtrade.com/api/v4/data"
MYSQL_DATABASE = "mydb"  # 與 mysql.yml 中 MYSQL_DATABASE 一致
TABLE_NAME = "EtfTop20BuyInstitutional"
TOP_N = 20  # 取前 N 名
TREND_DAYS = 5  # 趨勢計算天數


# ============================================================
# 共用工具函式
# ============================================================
def get_mysql_engine():
    """建立 MySQL SQLAlchemy 連線引擎"""
    address = (
        f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    )
    return create_engine(address)


def fetch_finmind(dataset: str, params: dict, retries: int = 3) -> pd.DataFrame:
    """
    呼叫 FinMind API 並回傳 DataFrame
    含重試機制, 避免短暫網路問題或 rate limit
    """
    query = {"dataset": dataset, **params}
    for attempt in range(retries):
        try:
            resp = requests.get(FINMIND_API, params=query, timeout=30)
            data = resp.json()
            if resp.status_code == 200 and data.get("status") == 200:
                return pd.DataFrame(data.get("data", []))
            logger.warning(
                f"[fetch_finmind] {dataset} 回傳異常: "
                f"status={data.get('status')}, msg={data.get('msg')}"
            )
        except Exception as e:
            logger.error(f"[fetch_finmind] {dataset} 嘗試 {attempt + 1} 失敗: {e}")
        time.sleep(2)
    # 全部失敗就回空 DataFrame, 由呼叫端決定要不要中止
    return pd.DataFrame()


def get_etf_list() -> pd.DataFrame:
    """
    取得所有 ETF 的 stock_id 與 stock_name
    台股 ETF 代號規則: 多為 00XX 開頭 (如 0050, 0056, 00878)
    使用 TaiwanStockInfo 並過濾 industry_category = 'ETF'
    """
    logger.info("[get_etf_list] 抓取 ETF 清單")
    df = fetch_finmind("TaiwanStockInfo", {})
    if df.empty:
        logger.error("[get_etf_list] TaiwanStockInfo 抓取失敗")
        return df
    # 篩選 ETF 類別
    etf_df = df[df["industry_category"] == "ETF"][
        ["stock_id", "stock_name"]
    ].drop_duplicates()
    logger.info(f"[get_etf_list] 共抓到 {len(etf_df)} 檔 ETF")
    return etf_df


# ============================================================
# 主要 Celery Task
# ============================================================
@app.task()
def crawler_etf_top20_institutional(target_date: str = None):
    """
    主任務: 抓取指定日期三大法人買超前 20 名 ETF

    Args:
        target_date: 目標日期, 格式 'YYYY-MM-DD'
                     未指定則用「昨天」(因為當日資料通常晚上才更新)

    執行步驟:
        1. 取得日期
        2. 抓 ETF 清單
        3. 抓三大法人買賣超
        4. 計算淨買超並取前 20
        5. 抓每檔 ETF 的 OHLCV 並計算技術指標
        6. 合併資料寫入 MySQL
    """
    # ---------- 1. 決定目標日期 ----------
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"========== ETF Top20 爬蟲開始: {target_date} ==========")

    # ---------- 2. 取得 ETF 清單 (用來過濾 + 補名稱) ----------
    etf_df = get_etf_list()
    if etf_df.empty:
        logger.error("ETF 清單為空, 任務中止")
        return
    etf_ids = set(etf_df["stock_id"].tolist())

    # ---------- 3. 抓三大法人買賣超 ----------
    logger.info(f"[step3] 抓取 {target_date} 三大法人買賣超")
    inst_df = fetch_finmind(
        "TaiwanStockInstitutionalInvestorsBuySell",
        {"start_date": target_date, "end_date": target_date},
    )
    if inst_df.empty:
        logger.error(f"{target_date} 無三大法人資料 (可能非交易日或資料尚未更新)")
        return

    # ---------- 4. 只保留 ETF, 加總三大法人, 取前 20 ----------
    inst_df = inst_df[inst_df["stock_id"].isin(etf_ids)]
    if inst_df.empty:
        logger.error("過濾後無 ETF 三大法人資料")
        return

    # 計算淨買超 (買 - 賣), 並加總三大法人
    inst_df["net_buy"] = inst_df["buy"] - inst_df["sell"]
    agg_df = (
        inst_df.groupby("stock_id", as_index=False)["net_buy"]
        .sum()
        .sort_values("net_buy", ascending=False)
        .head(TOP_N)
        .reset_index(drop=True)
    )
    agg_df["rank_num"] = agg_df.index + 1
    logger.info(f"[step4] 取出前 {TOP_N} 名 ETF")

    # ---------- 5. 抓每檔 ETF 的近期 OHLCV ----------
    # 為了算 5 日趨勢, 需要往前抓多一些 (避免遇到假日)
    # 抓 15 天確保至少有 5 個交易日
    price_start = (
        datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=15)
    ).strftime("%Y-%m-%d")

    enriched_rows = []
    for _, row in agg_df.iterrows():
        sid = row["stock_id"]
        logger.info(f"[step5] 抓取 {sid} 的 OHLCV")
        price_df = fetch_finmind(
            "TaiwanStockPrice",
            {"data_id": sid, "start_date": price_start, "end_date": target_date},
        )
        if price_df.empty:
            logger.warning(f"  {sid} 無價格資料, 跳過")
            continue
        # 依日期排序, 取最近 6 筆 (當日 + 前 5 個交易日)
        price_df = price_df.sort_values("date").reset_index(drop=True)
        # 過濾出 <= target_date 的資料
        price_df = price_df[price_df["date"] <= target_date].reset_index(drop=True)
        if len(price_df) == 0:
            continue

        # 當日資料 (最後一筆)
        today = price_df.iloc[-1]
        # 5 日前的收盤 (用來算 5 日趨勢)
        if len(price_df) >= TREND_DAYS + 1:
            five_day_ago_close = price_df.iloc[-(TREND_DAYS + 1)]["close"]
            five_day_trend = (
                (today["close"] - five_day_ago_close) / five_day_ago_close * 100
            )
        else:
            five_day_trend = None  # 資料不足

        # 當日漲幅: FinMind 自帶 spread (前一日收盤差), 但我們用收盤對前一日收盤計算更直觀
        if len(price_df) >= 2:
            prev_close = price_df.iloc[-2]["close"]
            daily_change = (today["close"] - prev_close) / prev_close * 100
        else:
            daily_change = None

        # 從 etf_df 補中文名稱
        name_lookup = etf_df.set_index("stock_id")["stock_name"].to_dict()
        stock_name = name_lookup.get(sid, "")

        enriched_rows.append(
            {
                "date": today["date"],
                "stock_id": sid,
                "stock_name": stock_name,
                "trading_volume": int(today.get("Trading_Volume", 0) or 0),
                "open_price": float(today["open"]),
                "close_price": float(today["close"]),
                "high_price": float(today["max"]),
                "low_price": float(today["min"]),
                "daily_change_pct": (
                    round(daily_change, 4) if daily_change is not None else None
                ),
                "five_day_trend_pct": (
                    round(five_day_trend, 4) if five_day_trend is not None else None
                ),
                "institutional_net_buy": int(row["net_buy"]),
                "rank_num": int(row["rank_num"]),
            }
        )
        # FinMind 免費版有 rate limit, 禮貌性 sleep
        time.sleep(0.3)

    if not enriched_rows:
        logger.error("最終無可寫入資料")
        return

    final_df = pd.DataFrame(enriched_rows)
    logger.info(f"[step6] 準備寫入 {len(final_df)} 筆資料")

    # ---------- 6. 寫入 MySQL ----------
    upload_etf_top20_to_mysql(final_df, target_date)
    logger.info(f"========== ETF Top20 爬蟲完成: {target_date} ==========")


def upload_etf_top20_to_mysql(df: pd.DataFrame, target_date: str):
    """
    寫入 MySQL: 先刪除當日舊資料避免重複, 再 append 新資料
    保留歷史, 但同一日期只會有一份最新資料 (idempotent)
    """
    engine = get_mysql_engine()

    # 先確保資料表存在 (第一次執行時自動建立)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
        `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
        `date` DATE NOT NULL,
        `stock_id` VARCHAR(10) NOT NULL,
        `stock_name` VARCHAR(50),
        `trading_volume` BIGINT,
        `open_price` FLOAT,
        `close_price` FLOAT,
        `high_price` FLOAT,
        `low_price` FLOAT,
        `daily_change_pct` FLOAT COMMENT '當日漲跌幅(%)',
        `five_day_trend_pct` FLOAT COMMENT '5日趨勢(%)',
        `institutional_net_buy` BIGINT COMMENT '三大法人淨買超(股)',
        `rank_num` INT COMMENT '當日名次',
        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY `uk_date_stock` (`date`, `stock_id`),
        KEY `idx_date` (`date`),
        KEY `idx_rank` (`date`, `rank_num`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    with engine.begin() as conn:
        from sqlalchemy import text

        conn.execute(text(create_sql))
        # 刪除當日舊資料 (idempotent)
        conn.execute(
            text(f"DELETE FROM `{TABLE_NAME}` WHERE `date` = :d"),
            {"d": target_date},
        )

    # append 新資料
    df.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False)
    logger.info(f"[upload] 成功寫入 {len(df)} 筆到 {TABLE_NAME}")


# ============================================================
# 測試版: 不寫 DB, 只印出, 用於 debug
# ============================================================
@app.task()
def crawler_etf_top20_institutional_print(target_date: str = None):
    """同上, 但只印出不寫 DB, 方便測試"""
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"[PRINT MODE] {target_date}")

    etf_df = get_etf_list()
    if etf_df.empty:
        return
    etf_ids = set(etf_df["stock_id"].tolist())

    inst_df = fetch_finmind(
        "TaiwanStockInstitutionalInvestorsBuySell",
        {"start_date": target_date, "end_date": target_date},
    )
    if inst_df.empty:
        logger.error(f"{target_date} 無三大法人資料")
        return

    inst_df = inst_df[inst_df["stock_id"].isin(etf_ids)]
    inst_df["net_buy"] = inst_df["buy"] - inst_df["sell"]
    agg_df = (
        inst_df.groupby("stock_id", as_index=False)["net_buy"]
        .sum()
        .sort_values("net_buy", ascending=False)
        .head(TOP_N)
    )
    name_lookup = etf_df.set_index("stock_id")["stock_name"].to_dict()
    agg_df["stock_name"] = agg_df["stock_id"].map(name_lookup)
    print(agg_df.to_string(index=False))
