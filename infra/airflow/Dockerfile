FROM apache/airflow:2.9.3-python3.11

# Installed against the Airflow constraint set so Airflow's own dependencies stay pinned.
ARG AIRFLOW_CONSTRAINTS=https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt
RUN pip install --no-cache-dir --constraint "${AIRFLOW_CONSTRAINTS}" \
      "pyiceberg[s3fs,pyarrow]==0.8.1" \
      "duckdb==1.1.3" \
      "clickhouse-connect==0.8.11"
