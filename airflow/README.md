# Airflow 排程模組

> 本目錄是 ETF Top 20 爬蟲的 **Airflow 自動排程模組**, 對應老師架構圖的 🟡 **排程** 區塊。
>
> 完整原始倉庫: https://github.com/howardliang110/dataflow

## 🎯 作用

- 每天平日 18:00 自動觸發 ETF 爬蟲
- 用 DockerOperator 呼叫 ETF Worker container (見 `../docker-compose-etf-worker-swarm.yml`)
- 完整對應老師「Airflow 排程」教學

## 📁 重要檔案

| 檔案 | 用途 |
|------|------|
| `docker-compose-airflow.yml` | Airflow Stack 部署 (webserver/scheduler/worker/redis) |
| `dataflow/dags/etf_crawler_dag.py` | ETF Top 20 主 DAG (DockerOperator 版) |
| `dataflow/dags/etf_top20_dag.py` | ETF Top 20 替代 DAG (PythonOperator 版) |
| `with.env.Dockerfile` | Build 自建 Airflow image |
| `SETUP.md` | 完整部署指南 |
| `local.ini.example` | 環境設定範本 (含密碼預留位置) |

## 🚀 快速啟動

請參考本目錄的 [SETUP.md](SETUP.md), 內含 6 步完整部署流程:

1. 複製 `local.ini.example` → `local.ini`, 填入你的密碼
2. 改 `airflow.cfg` 把 `<YOUR_MYSQL_PASSWORD>` 換成真密碼
3. 改 DAG 內的密碼 (`dataflow/dags/etf_crawler_dag.py`)
4. 跑 `ENV=DOCKER python3 genenv.py` 產 .env
5. Build image: `docker build -f with.env.Dockerfile -t <your_user>/tibame_dataflow:0.0.1 .`
6. 部署: `DOCKER_IMAGE_VERSION=0.0.1 docker stack deploy --resolve-image=never -c docker-compose-airflow.yml airflow`

## 🔗 配套需求

跑 Airflow 之前, 上一層的 ETF 爬蟲環境**必須先部署**:

- MySQL stack (見 `../mysql.yml`)
- RabbitMQ stack (見 `../rabbitmq-network.yml`)
- ETF Worker stack (見 `../docker-compose-etf-worker-swarm.yml`)
- 必須有 `my_attachable_network` (跨 stack 通訊用)

詳見 `../README.md` 與 `../docs/DATAFLOW.md`.
