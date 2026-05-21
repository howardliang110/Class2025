"""
ETF Top 20 v2 - Producer (Print 模式, 不寫 DB)
==============================================
用法:
    uv run python -m crawler.producer_crawler_etf_top20_v2_print
"""

import sys
from datetime import datetime, timedelta

from loguru import logger

from crawler.tasks_crawler_etf_top20_v2 import crawler_etf_top20_v2_print


def main():
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            logger.error(f"日期格式錯誤: {target_date}")
            sys.exit(1)
    else:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info(f"派送 ETF Top20 v2 [PRINT] 任務: {target_date}")
    result = crawler_etf_top20_v2_print.apply_async(
        kwargs={"target_date": target_date},
        queue="twse",
    )
    logger.info(f"任務已派送, task_id = {result.id}")


if __name__ == "__main__":
    main()