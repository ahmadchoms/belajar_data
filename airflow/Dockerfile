FROM apache/airflow:2.7.1

USER root

# Install Java (OpenJDK 17) & Procps (biar command ps jalan)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME biar Airflow tau
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Install Provider Spark
RUN pip install --no-cache-dir \
    "apache-airflow-providers-openlineage>=1.8.0" \
    apache-airflow-providers-apache-spark \
    pyspark \
    pandas \
    psycopg2-binary \
    dbt-postgres