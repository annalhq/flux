"""Prometheus metrics exposed on PRODUCER_METRICS_PORT."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

MESSAGES = Counter("flux_producer_messages", "Trades acknowledged by Kafka", ["exchange", "asset"])
DELIVERY_ERRORS = Counter("flux_producer_delivery_errors", "Trades Kafka failed to acknowledge", ["exchange"])
DLQ = Counter("flux_producer_dlq", "Messages routed to the DLQ", ["exchange", "stage"])
WS_RECONNECTS = Counter("flux_producer_ws_reconnects", "WebSocket reconnect attempts", ["exchange"])
EVENT_LAG = Histogram(
    "flux_producer_event_lag_seconds",
    "Exchange event time to producer ingestion",
    ["exchange"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)
LAST_EVENT = Gauge(
    "flux_producer_last_event_timestamp_seconds",
    "Wall-clock time of the last trade acknowledged by Kafka",
    ["exchange"],
)


def serve(port: int) -> None:
    start_http_server(port)
