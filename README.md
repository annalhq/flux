# Flux

Real-time crypto analytics platform: exchange trades → Avro/Kafka → Flink SQL → ClickHouse + Iceberg,
with batch-vs-stream reconciliation in Airflow. The rebuild plan lives in `docs/rebuild/`.

## Quickstart

```bash
make env        # creates .env from .env.example
make up         # all services, health-gated
make test lint  # unit + contract tests, ruff
```

| UI | URL |
| --- | --- |
| Grafana | http://localhost:3000 |
| Flink | http://localhost:8080 |
| Kafka UI | http://localhost:8090 |
| Airflow | http://localhost:8082 |
| Prometheus | http://localhost:9090 |
| MinIO console | http://localhost:9001 |
| Nessie | http://localhost:19120 |
