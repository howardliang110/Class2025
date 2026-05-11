"""
ETF Top 20 三大法人爬蟲 - Producer
================================

手動觸發 ETF 三大法人前 20 名爬蟲任務的發送端。
把任務送到 RabbitMQ 的 "twse" queue, 由 worker 領取執行。

用法:
    # 抓昨天的資料 (預設)
    uv run python -m crawler.producer_crawler_etf_top20

    # 抓指定日期
    uv run python -m crawler.producer_crawler_etf_top20 2025-05-10
"""

import sys
from datetime import datetime, timedelta

from loguru import logger

from crawler.tasks_crawler_etf_top20 import crawler_etf_top20_institutional


def main():
    # 從命令列參數取得日期, 沒給就用昨天
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        # 驗證日期格式
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            logger.error(f"日期格式錯誤: {target_date}, 應為 YYYY-MM-DD")
            sys.exit(1)
    else:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info(f"派送 ETF Top20 三大法人爬蟲任務: {target_date}")

    # 非同步派送任務到 twse queue
    # apply_async 會立即回傳, 實際執行交給 worker
    result = crawler_etf_top20_institutional.apply_async(
        kwargs={"target_date": target_date},
        queue="twse",
    )
    logger.info(f"任務已派送, task_id = {result.id}")


if __name__ == "__main__":
    main()
