FROM apache/airflow:2.9.3-python3.11

# Reconciliation runs in its own virtualenv: Airflow's constraints pin pyiceberg 0.6, which
# lacks the REST catalog features used here. DAG tasks run its CLI (python -m flux_recon.cli)
# as a subprocess, so Airflow's interpreter never imports these packages.
USER root
RUN python -m venv /opt/recon-venv && chown -R airflow:root /opt/recon-venv
USER airflow
RUN /opt/recon-venv/bin/pip install --no-cache-dir \
      "pyiceberg[s3fs,pyarrow]==0.8.1" \
      "duckdb==1.1.3" \
      "clickhouse-connect==0.8.11"
