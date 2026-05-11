"""
Airflow DAG: ETF Top 20 三大法人爬蟲
======================================
給「Airflow 負責人」的範例 DAG。

部署方式有兩種:
方式 A (推薦, 解耦): 用 CeleryExecutor + 既有 Celery worker
    - Airflow 只負責「派送任務」, 真正的爬蟲還是跑在現有 worker container
    - 這個 DAG 就是把原本的 producer 改寫成 Airflow 任務

方式 B: 直接在 Airflow 內呼叫 task function
    - 不經過 RabbitMQ, 由 Airflow 自己的 worker 執行
    - 適合不想維護 Celery 架構的小團隊

本範例使用方式 A, 保留現有 Celery 架構, Airflow 只取代 scheduler_etf_top20.py。

放置位置:
    把這個檔案放到 Airflow 的 dags/ 目錄, Airflow 會自動偵測。
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# ============================================================
# 任務函式
# ============================================================
def trigger_etf_top20_task(**context):
    """
    從 Airflow context 取得執行日期 (ds), 派送任務到 Celery worker。
    Airflow 的 ds 是排程當下的 logical date, 格式 'YYYY-MM-DD'
    """
    # 從 context 拿邏輯日期 (Airflow 提供)
    target_date = context["ds"]

    # 動態 import 避免 DAG 解析時連線 RabbitMQ
    from crawler.tasks_crawler_etf_top20 import crawler_etf_top20_institutional

    result = crawler_etf_top20_institutional.apply_async(
        kwargs={"target_date": target_date},
        queue="twse",
    )
    print(f"Celery task dispatched: {result.id}, target_date={target_date}")
    return result.id


# ============================================================
# DAG 定義
# ============================================================
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="etf_top20_institutional_crawler",
    description="每日抓取三大法人買超前 20 名 ETF",
    default_args=default_args,
    # 平日 (週一到週五) 下午 18:00 (台北時間, UTC+8 = UTC 10:00)
    # Airflow 預設 UTC, 也可在 airflow.cfg 改時區
    schedule_interval="0 10 * * 1-5",
    start_date=datetime(2025, 1, 1),
    catchup=False,  # 不補跑歷史日期
    tags=["crawler", "etf", "institutional"],
) as dag:
    dispatch_task = PythonOperator(
        task_id="dispatch_etf_top20_to_celery",
        python_callable=trigger_etf_top20_task,
        provide_context=True,
    )

    dispatch_task
