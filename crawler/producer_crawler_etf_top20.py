from crawler.tasks_crawler_etf_top20 import crawler_etf_top20

# TWSE OpenAPI 永遠回最新一交易日, 發一個任務即可
crawler_etf_top20.apply_async(queue="twse")
print("已發送 ETF top 20 爬蟲任務")
