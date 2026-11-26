# Image Airflow du TP5 : Airflow 3.3.2, plus Spark (pour spark-submit) et dbt
FROM apache/airflow:3.3.2

# 1. Java et Spark, recopiés depuis l'image tp3-spark du TP3 (rien à télécharger)
COPY --from=tp3-spark:3.5.9 /opt/java/openjdk /opt/java/openjdk
COPY --from=tp3-spark:3.5.9 --chown=airflow:root /opt/spark /opt/spark
ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH="/opt/java/openjdk/bin:/opt/spark/bin:${PATH}:/opt/airflow/dbt-venv/bin"

# 2. Les providers Spark et Trino, et Cosmos (dbt dans Airflow)
RUN pip install --timeout 90 --retries 12 --resume-retries 20 "apache-airflow==3.3.2" \
    apache-airflow-providers-apache-spark==6.3.5 apache-airflow-providers-trino==7.0.0 \
    astronomer-cosmos==1.16.0 keyring

# 3. dbt dans son propre environnement Python, séparé de celui d'Airflow
RUN python -m venv /opt/airflow/dbt-venv \
 && /opt/airflow/dbt-venv/bin/pip install --timeout 90 --retries 12 --resume-retries 20 \
    dbt-core==1.12.7 dbt-trino==1.12.0 keyring
