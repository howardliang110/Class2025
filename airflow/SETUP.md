# 接手者快速啟動指南

本專案為 ETF Top 20 爬蟲的 Airflow 排程模組,
搭配 [Class2025 ETF 爬蟲專案](https://github.com/howardliang110/Class2025) 使用。

## 📋 你需要修改的檔案 (clone 後)

clone 下來**直接跑會失敗**, 因為密碼類設定不放進 git。
請依下面 4 步設定:

### 步驟 1: 建立 local.ini

```bash
cp local.ini.example local.ini
```

開啟 `local.ini`, 把所有 `<YOUR_MYSQL_PASSWORD>` 改成你的 MySQL root 密碼。

### 步驟 2: 修改 airflow.cfg

打開 `airflow.cfg`, 找到 `sql_alchemy_conn`, 把 `<YOUR_MYSQL_PASSWORD>` 改成你的 MySQL root 密碼:

```ini
sql_alchemy_conn = mysql+pymysql://root:<YOUR_MYSQL_PASSWORD>@mysql/airflow
```

### 步驟 3: 修改 DAG 的密碼

打開 `dataflow/dags/etf_crawler_dag.py`, 把 `"MYSQL_PASSWORD": "<YOUR_MYSQL_PASSWORD>"` 改成你的密碼。

### 步驟 4: 產出 .env

```bash
ENV=DOCKER python3 genenv.py
```

## 🚀 部署流程

設定完成後, 照下列步驟部署:

### 前置作業 (做一次)

```bash
# 1. 建 attachable network
docker network create --scope=swarm --driver=overlay --attachable my_attachable_network

# 2. 給 swarm node 加 labels
docker node update --label-add airflow=true $(docker node ls -q)
docker node update --label-add worker=true $(docker node ls -q)
```

### 部署

```bash
# Build 你自己的 Airflow image (含你的密碼設定)
docker build -f with.env.Dockerfile -t <YOUR_DOCKER_USERNAME>/tibame_dataflow:0.0.1 .

# 改 docker-compose-airflow.yml 用你的 image
# 把 howardlch/tibame_dataflow 改成 <YOUR_DOCKER_USERNAME>/tibame_dataflow

# 在 MySQL 建 airflow database
docker exec -it <MYSQL_CONTAINER> mysql -uroot -p<YOUR_PASSWORD> \
  -e "CREATE DATABASE IF NOT EXISTS airflow CHARACTER SET utf8mb4;"

# 部署 Airflow stack
DOCKER_IMAGE_VERSION=0.0.1 docker stack deploy \
  --resolve-image=never \
  -c docker-compose-airflow.yml \
  airflow
```

打開 `http://127.0.0.1:5000`, 用 admin/admin 登入。

## 🔗 配套專案需求

本專案要跟 [Class2025 ETF 爬蟲](https://github.com/howardliang110/Class2025) 一起運作:

- 需要 ETF 爬蟲的 worker image (`howardlch/etf_crawler:1.1`) 已部署
- 需要 MySQL stack 已部署於 `my_attachable_network`
- 需要 RabbitMQ stack 已部署於 `my_attachable_network`

請先按照 Class2025 repo 的 README 部署完爬蟲環境, 再來部署 Airflow。

## ⚠️ 重要提醒

- **絕對不要 commit local.ini, airflow.cfg, .env**, 它們含真實密碼
- 已在 `.gitignore` 排除, 但每次 commit 前再次確認
- 密碼類設定建議統一使用同一組 (例如本範例的 mysql 密碼)
