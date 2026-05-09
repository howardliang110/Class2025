import pandas as pd
import requests
from sqlalchemy import create_engine

from crawler.config import MYSQL_ACCOUNT, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT
from crawler.worker import app


@app.task()
def crawler_finmind_print(stock_id):
    url = "https://api.finmindtrade.com/api/v4/data"
    parameter = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock_id,
        "start_date": "2024-01-01",
        "end_date": "2025-06-17",
    }
    resp = requests.get(url, params=parameter)
    data = resp.json()
    if resp.status_code == 200:
        df = pd.DataFrame(data["data"])
        print(df)
    else:
        print(data["msg"])


def upload_data_to_mysql(df: pd.DataFrame):
    address = f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/tibame"
    print(f"[upload_data_to_mysql] connecting to {MYSQL_HOST}:{MYSQL_PORT}/tibame")
    engine = create_engine(address)
    df.to_sql(
        "TaiwanStockPrice",
        con=engine,
        if_exists="append",
        index=False,
    )
    print(f"[upload_data_to_mysql] OK, wrote {len(df)} rows")


@app.task()
def crawler_finmind(stock_id):
    url = "https://api.finmindtrade.com/api/v4/data"
    parameter = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock_id,
        "start_date": "2024-01-01",
        "end_date": "2025-06-17",
    }
    resp = requests.get(url, params=parameter)
    data = resp.json()
    if resp.status_code == 200:
        df = pd.DataFrame(data["data"])
        print(df)
        upload_data_to_mysql(df)
    else:
        print(data["msg"])
