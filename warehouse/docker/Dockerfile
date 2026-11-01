FROM apache/airflow:2.11.2-python3.11

ARG AIRFLOW_VERSION=2.11.2
ARG PYTHON_VERSION=3.11

# Certificado de AWS para conexiones TLS a RDS y DocumentDB (sirve para ambos).
USER root
RUN mkdir -p /opt/certs \
    && curl -fsSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -o /opt/certs/global-bundle.pem \
    && chmod 644 /opt/certs/global-bundle.pem
USER airflow

# Dependencias del pipeline (respetando los constraints de Airflow).
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    "apache-airflow==${AIRFLOW_VERSION}" \
    Faker PyMySQL pymongo pyarrow boto3 psycopg2-binary

# dbt en un entorno aislado (sus dependencias chocan con las de Airflow).
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core~=1.9.4" "dbt-redshift~=1.9.0" \
    && ln -sf /home/airflow/dbt-venv/bin/dbt /home/airflow/.local/bin/dbt
