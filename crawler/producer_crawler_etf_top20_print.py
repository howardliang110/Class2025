# 教學用版本: 使用 crawler_etf_top20_print (只抓取並印出, 不寫入資料庫)
# 適合初學者第一次派送任務時使用, 驗證 producer/worker 流程是否通暢
# 對應的 consumer 就是 tasks_crawler_etf_top20.py 裡註冊的 crawler_etf_top20_print task
from datetime import datetime, timedelta

from crawler.tasks_crawler_etf_top20 import crawler_etf_top20_print

# 發送最近 5 天 (六日會被 task 內部判定為無資料, 自動 skip)
today = datetime.now()
for i in range(5):
    date = (today - timedelta(days=i)).strftime("%Y%m%d")
    print(date)
    # .delay() 是 Celery 的非同步派送捷徑, 呼叫完會立刻回傳, 不等 task 執行完
    crawler_etf_top20_print.delay(date=date)