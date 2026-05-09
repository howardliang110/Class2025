from crawler.tasks_crawler_finmind import crawler_finmind

for stock_id in ["2330", "0050", "2317", "0056", "00713"]:
    print(stock_id)
    crawler_finmind.apply_async(
        kwargs={"stock_id": stock_id},
        queue="twse",
    )
