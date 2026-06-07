# 使用 Ubuntu 20.04 作為基礎映像檔
FROM linsamtw/tibame_dataflow:0.0.7

# 將當前目錄（與 Dockerfile 同層）所有內容複製到容器的 /dataflow 資料夾
COPY ./dataflow /dataflow/dataflow
COPY ./genenv.py /dataflow
COPY ./pyproject.toml /dataflow
COPY ./uv.lock /dataflow
COPY ./README.md /dataflow
COPY ./local.ini /dataflow
COPY ./airflow-gce.cfg /dataflow/airflow.cfg

# # 設定容器的工作目錄為 /dataflow，後續的指令都在這個目錄下執行
WORKDIR /dataflow/

# # 啟動容器後，預設執行 bash（開啟終端）
CMD ["/bin/bash"]
