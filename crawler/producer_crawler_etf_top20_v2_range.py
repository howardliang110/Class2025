"""
ETF Top 20 v2 - 區間派送
========================
一次派送一段日期範圍的所有交易日任務到 Celery worker。

用法:
    .venv/bin/python -m crawler.producer_crawler_etf_top20_v2_range 2025-05-01 2025-05-13

特點:
- 自動跳過週六、週日 (但無法判斷國定假日, 會派送但任務會回報無資料)
- 派送速度很快, 但 worker 處理需要時間 (每天約 20-30 秒)
- 證交所有 rate limit, 建議單次不要超過 1 個月
"""

import sys
import time
from datetime import datetime, timedelta

from loguru import logger

from crawler.tasks_crawler_etf_top20_v2 import crawler_etf_top20_v2


def daterange(start: str, end: str):
    """產生 start 到 end (含) 的所有工作日 (跳過六日)"""
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    current = start_dt
    while current <= end_dt:
        if current.weekday() < 5:  # 0=Mon, 4=Fri
            yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def main():
    if len(sys.argv) < 3:
        logger.error("用法: producer_crawler_etf_top20_v2_range <start> <end>")
        logger.error("例: producer_crawler_etf_top20_v2_range 2025-05-01 2025-05-13")
        sys.exit(1)

    start, end = sys.argv[1], sys.argv[2]
    try:
        datetime.strptime(start, "%Y-%m-%d")
        datetime.strptime(end, "%Y-%m-%d")
    except ValueError:
        logger.error("日期格式錯誤, 應為 YYYY-MM-DD")
        sys.exit(1)

    dates = list(daterange(start, end))
    logger.info(f"派送 {len(dates)} 個工作日的任務: {start} ~ {end}")

    for d in dates:
        result = crawler_etf_top20_v2.apply_async(
            kwargs={"target_date": d},
            queue="twse",
        )
        logger.info(f"  {d} 任務已派送, task_id = {result.id}")
        # 派送間略停一下, 避免 Celery 端壓力過大
        time.sleep(0.1)

    logger.info(
        f"全部 {len(dates)} 個任務已派送, worker 會依序處理 (預估 {len(dates) * 25} 秒)"
    )


if __name__ == "__main__":
    main()