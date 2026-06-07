"""ETF Top 20 三大法人爬蟲 - 每日自動執行"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "howard",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etf_top20_crawler",
    default_args=default_args,
    description="每日爬取 ETF 三大法人 Top 20",
    schedule_interval="0 18 * * 1-5",  # 平日 18:00
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["etf", "crawler"],
) as dag:

    crawl_etf = DockerOperator(
        task_id="crawl_etf_top20",
        image="howardlch/etf_crawler:1.1",
        command="uv run python -m crawler.producer_crawler_etf_top20_v2",
        network_mode="my_attachable_network",
        environment={
            "RABBITMQ_HOST": "rabbitmq",
            "RABBITMQ_PORT": "5672",
            "WORKER_ACCOUNT": "worker",
            "WORKER_PASSWORD": "worker",
            "MYSQL_HOST": "mysql",
            "MYSQL_PORT": "3306",
            "MYSQL_ACCOUNT": "root",
            "MYSQL_PASSWORD": "<YOUR_MYSQL_PASSWORD>",
        },
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )