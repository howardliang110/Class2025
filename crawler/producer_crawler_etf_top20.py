from datetime import datetime, timedelta

from crawler.tasks_crawler_etf_top20 import crawler_etf_top20

# 一次發送最近 N 個交易日的爬蟲任務
# RabbitMQ 會把任務排進 queue, 由 worker 平行處理
# 假日 (六日 / 國定假日) 會被 task 內部檢查到並 skip, 不會出錯

DAYS = 5  # 抓最近 5 天

today = datetime.now()
for i in range(DAYS):
    date = (today - timedelta(days=i)).strftime("%Y%m%d")
    print(date)
    crawler_etf_top20.apply_async(
        kwargs={"date": date},
        queue="twse",
    )