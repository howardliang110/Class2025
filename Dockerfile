# ETF Crawler 
FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

RUN uv python install 3.11

RUN mkdir /crawler

# 先複製依賴定義檔, 利用 Docker layer cache
COPY pyproject.toml uv.lock README.md /crawler/

WORKDIR /crawler/

# 安裝依賴 (含 yfinance, 已在 pyproject.toml)
RUN uv sync --frozen

# 複製程式碼
COPY crawler /crawler/crawler

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

CMD ["/bin/bash"]