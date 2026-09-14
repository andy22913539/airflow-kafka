FROM apache/airflow:2.9.2

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    beautifulsoup4 \
    kafka-python-ng \
    pymongo \
    pymysql \
    requests[socks]
