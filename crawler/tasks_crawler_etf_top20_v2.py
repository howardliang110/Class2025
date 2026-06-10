"""
ETF 三大法人買超 Top 20 - v2 版本 (改進版)
=====================================

資料源: 證交所 T86 + yfinance

改進重點 (相對前一版):
1. yfinance 改用「批次下載」(一次抓 20 檔), 大幅降低被 rate limit 機率
2. yfinance 失敗時自動重試
3. OHLCV 抓不到的 ETF 仍保留 rank (價格欄位填 NULL), 不整檔丟棄
4. 加長請求間隔
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


# ============================================================
# 1. 抓三大法人 (證交所 T86)
# ============================================================
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
            resp = requests.get(
                TWSE_T86_URL, params=params, headers=headers, timeout=30
            )
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


def _clean_num(series: pd.Series) -> pd.Series:
    """T86 數字欄位 (含逗號) 轉 int64"""
    return (
        series.astype(str)
        .str.replace(",", "")
        .str.replace(" ", "")
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
        .astype("int64")
    )


def _find_col(df: pd.DataFrame, *keywords) -> str:
    """模糊比對找欄位 (避免空白/全形字差異)"""
    for col in df.columns:
        clean = col.replace(" ", "").replace("　", "")
        if all(k in clean for k in keywords):
            return col
    return None


def calculate_net_buy(df: pd.DataFrame) -> pd.DataFrame:
    """
    從 T86 原始 DataFrame 計算三大法人各別買賣量 + 三大法人合計買賣超 (排名用)

    輸出欄位:
        stock_id, stock_name,
        foreign_buy, foreign_sell, foreign_net,    (外資 = 外陸資 + 外資自營商)
        trust_buy, trust_sell, trust_net,          (投信)
        dealer_buy, dealer_sell, dealer_net,       (自營商 = 自行 + 避險)
        net_buy                                     (三大法人合計買賣超, 用於排序)
    """
    if df.empty:
        return df

    # 找股票代號、名稱欄位
    sid_col = _find_col(df, "代號") or _find_col(df, "代碼")
    name_col = _find_col(df, "名稱")
    if sid_col is None or name_col is None:
        logger.error(f"[calculate_net_buy] 找不到代號/名稱欄位: {list(df.columns)}")
        return pd.DataFrame()

    # 找三大法人合計買賣超 (排序用)
    total_col = _find_col(df, "三大法人", "買賣超")
    if total_col is None:
        logger.error(f"[calculate_net_buy] 找不到三大法人買賣超欄位: {list(df.columns)}")
        return pd.DataFrame()

    # 找各法人欄位
    # 外資 = 外陸資 (不含外資自營商) + 外資自營商
    fb1 = _find_col(df, "外陸資買進股數")
    fs1 = _find_col(df, "外陸資賣出股數")
    fn1 = _find_col(df, "外陸資買賣超股數")
    fb2 = _find_col(df, "外資自營商買進股數")
    fs2 = _find_col(df, "外資自營商賣出股數")
    fn2 = _find_col(df, "外資自營商買賣超股數")

    tb = _find_col(df, "投信買進股數")
    ts = _find_col(df, "投信賣出股數")
    tn = _find_col(df, "投信買賣超股數")

    # 自營商 = 自行 + 避險
    db1 = _find_col(df, "自營商買進股數", "自行")
    ds1 = _find_col(df, "自營商賣出股數", "自行")
    db2 = _find_col(df, "自營商買進股數", "避險")
    ds2 = _find_col(df, "自營商賣出股數", "避險")
    dn = _find_col(df, "自營商買賣超股數") and [c for c in df.columns if c.replace(" ", "").replace("　", "") == "自營商買賣超股數"]
    dn = dn[0] if dn else None

    out = pd.DataFrame()
    out["stock_id"] = df[sid_col].astype(str).str.strip()
    out["stock_name"] = df[name_col].astype(str).str.strip()

    # 外資 (合併外陸資 + 外資自營商)
    out["foreign_buy"] = _clean_num(df[fb1]) + (_clean_num(df[fb2]) if fb2 else 0)
    out["foreign_sell"] = _clean_num(df[fs1]) + (_clean_num(df[fs2]) if fs2 else 0)
    out["foreign_net"] = _clean_num(df[fn1]) + (_clean_num(df[fn2]) if fn2 else 0)

    # 投信
    out["trust_buy"] = _clean_num(df[tb]) if tb else 0
    out["trust_sell"] = _clean_num(df[ts]) if ts else 0
    out["trust_net"] = _clean_num(df[tn]) if tn else 0

    # 自營商 (合併自行 + 避險的買進/賣出)
    out["dealer_buy"] = (_clean_num(df[db1]) if db1 else 0) + (_clean_num(df[db2]) if db2 else 0)
    out["dealer_sell"] = (_clean_num(df[ds1]) if ds1 else 0) + (_clean_num(df[ds2]) if ds2 else 0)
    # 自營商買賣超直接用 T86 提供的合計
    out["dealer_net"] = _clean_num(df[dn]) if dn else 0

    # 三大法人合計 (排序用)
    out["net_buy"] = _clean_num(df[total_col])

    return out


# ============================================================
# 2. 抓 OHLCV (yfinance) - 改進版: 批次下載 + 重試
# ============================================================
def fetch_ohlcv_batch(
    stock_ids: list, target_date: str, lookback_days: int = 15, retries: int = 2
) -> dict:
    """
    一次批次下載多檔 ETF 的 OHLCV (大幅降低 rate limit 機率)
    回傳 dict: { stock_id: DataFrame }
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.error("[fetch_ohlcv_batch] yfinance 未安裝")
        return {}

    tickers = [f"{sid}.TW" for sid in stock_ids]
    end_dt = datetime.strptime(target_date, "%Y-%m-%d")
    end_str = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    start_str = (end_dt - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    result = {}
    for attempt in range(retries):
        try:
            # 一次下載所有 ticker
            data = yf.download(
                tickers,
                start=start_str,
                end=end_str,
                progress=False,
                auto_adjust=False,
                group_by="ticker",
                threads=True,
            )
            if data.empty:
                logger.warning(f"[fetch_ohlcv_batch] attempt {attempt+1} 回傳空, 重試")
                time.sleep(5)
                continue

            # 解析每檔 ticker 的資料
            for sid, ticker in zip(stock_ids, tickers):
                try:
                    if len(tickers) == 1:
                        # 單檔時欄位非 MultiIndex
                        df = data.copy()
                    else:
                        # 多檔時用 ticker 取出該檔
                        if ticker not in data.columns.get_level_values(0):
                            continue
                        df = data[ticker].copy()
                    df = df.dropna(how="all")
                    if not df.empty:
                        df = df.reset_index()
                        result[sid] = df
                except Exception:
                    continue

            if result:
                logger.info(
                    f"[fetch_ohlcv_batch] 批次下載成功 {len(result)}/{len(stock_ids)} 檔"
                )
                return result

        except Exception as e:
            logger.warning(f"[fetch_ohlcv_batch] attempt {attempt+1} 失敗: {e}")
            time.sleep(5)

    return result


def compute_metrics(price_df: pd.DataFrame, target_date: str):
    """從單檔 OHLCV 算出 open/close/volume/value/trend"""
    if price_df is None or price_df.empty:
        return None

    price_df = price_df.copy()
    price_df["Date"] = pd.to_datetime(price_df["Date"])
    target_dt = pd.to_datetime(target_date)
    price_df = price_df[price_df["Date"] <= target_dt].sort_values("Date")
    if len(price_df) == 0:
        return None

    today = price_df.iloc[-1]

    if len(price_df) >= TREND_DAYS + 1:
        past_close = price_df.iloc[-(TREND_DAYS + 1)]["Close"]
        five_day_trend = (today["Close"] - past_close) / past_close * 100
    else:
        five_day_trend = None

    volume_shares = int(today["Volume"]) if pd.notna(today["Volume"]) else 0
    trading_value = (
        int(volume_shares * float(today["Close"]))
        if pd.notna(today["Close"])
        else None
    )

    return {
        "open_price": round(float(today["Open"]), 4) if pd.notna(today["Open"]) else None,
        "close_price": round(float(today["Close"]), 4) if pd.notna(today["Close"]) else None,
        "trading_volume_shares": volume_shares,
        "trading_value": trading_value,
        "five_day_trend_pct": (
            round(five_day_trend, 4) if five_day_trend is not None else None
        ),
    }


# ============================================================
# 3. 主任務 (改進版)
# ============================================================
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

    # 改進: 批次下載所有 20 檔的 OHLCV (一次請求, 降低 rate limit)
    stock_ids = top_df["stock_id"].tolist()
    ohlcv_map = fetch_ohlcv_batch(stock_ids, target_date, lookback_days=15)

    enriched = []
    for _, row in top_df.iterrows():
        sid = row["stock_id"]
        metrics = compute_metrics(ohlcv_map.get(sid), target_date)

        if metrics is None:
            # 改進: 即使 OHLCV 抓不到, 仍保留 rank, 價格欄位 NULL
            logger.warning(f"  {sid} 無 OHLCV, 保留 rank 但價格為 NULL")
            metrics = {
                "open_price": None,
                "close_price": None,
                "trading_volume_shares": None,
                "trading_value": None,
                "five_day_trend_pct": None,
            }

        enriched.append(
            {
                "date": target_date,
                "stock_id": sid,
                "stock_name": row["stock_name"],
                "rank_num": int(row["rank_num"]),
                "foreign_buy": int(row.get("foreign_buy", 0)),
                "foreign_sell": int(row.get("foreign_sell", 0)),
                "foreign_net": int(row.get("foreign_net", 0)),
                "trust_buy": int(row.get("trust_buy", 0)),
                "trust_sell": int(row.get("trust_sell", 0)),
                "trust_net": int(row.get("trust_net", 0)),
                "dealer_buy": int(row.get("dealer_buy", 0)),
                "dealer_sell": int(row.get("dealer_sell", 0)),
                "dealer_net": int(row.get("dealer_net", 0)),
                **metrics,
            }
        )

    if not enriched:
        logger.error("最終無可寫入資料")
        return

    final_df = pd.DataFrame(enriched)
    upload_to_mysql_v2(final_df, target_date)
    logger.info(f"========== ETF Top20 v2 完成: {target_date} ({len(final_df)} 筆) ==========")


def upload_to_mysql_v2(df: pd.DataFrame, target_date: str):
    engine = get_mysql_engine()
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
        `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
        `date` DATE NOT NULL COMMENT '日期',
        `stock_id` VARCHAR(10) NOT NULL COMMENT '股票代號',
        `stock_name` VARCHAR(50) COMMENT '股票名稱',
        `open_price` FLOAT COMMENT '開盤價',
        `close_price` FLOAT COMMENT '收盤價',
        `trading_volume_shares` BIGINT COMMENT '成交股數',
        `trading_value` BIGINT COMMENT '成交金額(元,近似)',
        `five_day_trend_pct` FLOAT COMMENT '5日趨勢(%)',
        `rank_num` INT COMMENT '三大法人淨買超名次',
        `foreign_buy` BIGINT COMMENT '外資買進股數',
        `foreign_sell` BIGINT COMMENT '外資賣出股數',
        `foreign_net` BIGINT COMMENT '外資買賣超股數',
        `trust_buy` BIGINT COMMENT '投信買進股數',
        `trust_sell` BIGINT COMMENT '投信賣出股數',
        `trust_net` BIGINT COMMENT '投信買賣超股數',
        `dealer_buy` BIGINT COMMENT '自營商買進股數',
        `dealer_sell` BIGINT COMMENT '自營商賣出股數',
        `dealer_net` BIGINT COMMENT '自營商買賣超股數',
        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY `uk_date_stock` (`date`, `stock_id`),
        KEY `idx_date` (`date`),
        KEY `idx_rank` (`date`, `rank_num`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ETF 三大法人買超前20名每日快照';
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))
        conn.execute(
            text(f"DELETE FROM `{TABLE_NAME}` WHERE `date` = :d"),
            {"d": target_date},
        )

    df.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False)
    logger.info(f"[upload_v2] 寫入 {len(df)} 筆到 {TABLE_NAME}")


# ============================================================
# Print 模式
# ============================================================
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