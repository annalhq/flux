# Flink with the Kafka, Avro (Schema Registry), Iceberg and S3 connectors. Jobs are pure SQL.
FROM flink:1.19.1-scala_2.12-java11

ARG MVN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MVN}/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar /opt/flink/lib/
ADD --chmod=644 ${MVN}/org/apache/flink/flink-sql-avro-confluent-registry/1.19.1/flink-sql-avro-confluent-registry-1.19.1.jar /opt/flink/lib/
ADD --chmod=644 ${MVN}/org/apache/iceberg/iceberg-flink-runtime-1.19/1.7.1/iceberg-flink-runtime-1.19-1.7.1.jar /opt/flink/lib/
ADD --chmod=644 ${MVN}/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar /opt/flink/lib/
# Hadoop classes required by the Iceberg Flink runtime.
ADD --chmod=644 ${MVN}/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar /opt/flink/lib/

RUN set -eux; \
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/; \
    if ! ls -R /opt/flink/plugins | grep -q 'flink-metrics-prometheus'; then \
      mkdir -p /opt/flink/plugins/metrics-prometheus; \
      cp /opt/flink/opt/flink-metrics-prometheus-*.jar /opt/flink/plugins/metrics-prometheus/; \
    fi; \
    chown -R flink:flink /opt/flink/plugins
