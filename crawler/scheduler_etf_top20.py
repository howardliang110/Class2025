"""
ETF Top 20 三大法人爬蟲 - Scheduler
====================================

定時派送爬蟲任務到 Celery worker, 每日自動更新 ETF 前 20 名買超資料。

排程設計:
    - 每日 18:00 (Asia/Taipei) 抓取「今日」三大法人資料
      (FinMind 通常下午 5:30 後更新, 留半小時 buffer)
    - 假日 (週六日) 不執行, 因為無交易

執行方式 (在 docker / 本機):
    uv run python -m crawler.scheduler_etf_top20

下游使用者:
    - 視覺化 (Redash): 連到 MySQL 讀 EtfTop20BuyInstitutional 表
    - Airflow: 也可以改用 Airflow DAG 取代本檔 scheduler
"""

import time
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger

from crawler.tasks_crawler_etf_top20 import crawler_etf_top20_institutional


def send_etf_top20_task():
    """派送今日 ETF Top 20 任務到 Celery"""
    today = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"[scheduler] 派送 ETF Top20 任務: {today}")
    crawler_etf_top20_institutional.apply_async(
        kwargs={"target_date": today},
        queue="twse",
    )


def main():
    scheduler = BackgroundScheduler(timezone="Asia/Taipei")

    # 平日 18:00 執行 (週一 ~ 週五)
    # cron 寫法: day_of_week='mon-fri'
    scheduler.add_job(
        id="etf_top20_daily",
        func=send_etf_top20_task,
        trigger="cron",
        day_of_week="mon-fri",
        hour="18",
        minute="0",
        second="0",
        coalesce=True,  # 錯過排程只補執行一次
    )
    logger.info("ETF Top20 scheduler 啟動, 每個交易日 18:00 自動執行")
    scheduler.start()


if __name__ == "__main__":
    main()
    # 主程式持續執行, 否則排程器會跟著結束
    while True:
        time.sleep(600)
