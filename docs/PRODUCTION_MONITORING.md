# Production Monitoring for Kafka Manager and Kafka Brokers

This document describes recommended production monitoring architecture, security considerations, and examples for running the Prometheus JMX exporter inside Kafka broker containers. It also includes a sample Prometheus scrape configuration and operational recommendations.

## Goal
- Provide reliable, secure, and scalable metrics collection for Kafka brokers and the Kafka Manager service.
- Avoid fragile RMI/JMX network issues in containerized deployments by preferring HTTP-based exporters.
- Give clear production-ready guidance (auth, TLS, firewalling, cardinality) and example configs.

## Recommended Architecture (summary)

1. Use Prometheus as the central metrics collector and datastore for short/medium-term metrics.
2. Expose broker JMX metrics via the Prometheus JMX exporter (Java agent) or Jolokia (HTTP bridge) running inside each broker container.
3. Scrape the exporters from Prometheus with controlled scrape intervals and relabeling to limit cardinality.
4. Use Alertmanager (or a managed equivalent) for alerting and route alerts to on-call systems.
5. Let Kafka Manager integrate with Prometheus via the Prometheus HTTP API for dashboarding and ad-hoc queries. Do not rely on the Cluster Manager to be the primary scraper in production.

Rationale: Prometheus and exporter agents are purpose-built for scraping, filtering, and retention. They avoid the fragility of remote RMI/JMX connections across container boundaries and simplify security (single HTTP port per exporter).

---

## Production checklist (must-haves)

- Use the JMX exporter agent or Jolokia inside broker containers rather than exposing raw RMI/JMX to your network.
- Secure exporter HTTP endpoints with network-level controls (VPC, firewall rules) and TLS or an authenticated reverse proxy when reachable across trust boundaries.
- Limit metric label cardinality (topic labels can explode). Apply relabeling in Prometheus or use exporter configuration to drop high-cardinality metrics.
- Configure retention and remote-write to scalable long-term storage if you need historical analysis beyond Prometheus local retention.
- Create alerts for broker health (under-replicated partitions, offline partitions, controller quorum changes), I/O saturation, disk usage, and request handler saturation.
- Test and verify exporter startup and scraping on staging before production rollouts.

---

## Example: Run Prometheus JMX Exporter as a Java agent in Docker

This example shows the components you need inside your broker container. It uses the `jmx_prometheus_javaagent` jar and a simple `config.yml` for the exporter.

1) Copy the JMX exporter jar and a config file into the image or mount them as volumes.

Docker Compose snippet (add to each `brokerN` service):

```yaml
services:
  broker1:
    image: apache/kafka:4.2.0
    ports:
      - "19092:9094"   # kafka client
      - "9404:9404"    # prometheus jmx exporter HTTP port
    volumes:
      - ./monitoring/jmx_exporter/jmx_prometheus_javaagent-0.16.1.jar:/opt/jmx_exporter/jmx_prometheus_javaagent.jar:ro
      - ./monitoring/jmx_exporter/broker-9404.yml:/opt/jmx_exporter/config.yml:ro
    environment:
      # Typical Kafka images accept KAFKA_OPTS or JAVA_TOOL_OPTIONS; adapt to your image
      KAFKA_OPTS: >-
        -javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar=9404:/opt/jmx_exporter/config.yml
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://broker1:9092,EXTERNAL://${KAFKA_HOST_IP:-10.71.135.15}:19092
```

Notes:
- Expose a single HTTP port (9404) for Prometheus scraping. Protect this port with host/firewall rules.
- Place the exporter jar and config under `./monitoring/jmx_exporter/` in your repo or image build context.

2) Example minimal `broker-9404.yml` exporter config

```yaml
rules:
  - pattern: 'kafka.server<type=(.+), name=(.+)><>Value'
    name: kafka_server_$1_$2
    type: GAUGE
  - pattern: 'kafka.server<type=(.+), name=(.+), topic=(.+)><>OneMinuteRate'
    name: kafka_topic_$2_one_minute
    labels:
      topic: '$3'
    type: GAUGE
```

Caveat: exporter config should be tuned to select the metrics you need and to avoid metrics that create explosive cardinality (for example, metrics labeled by topic should be sampled selectively or relabeled).

---

## Sample Prometheus `scrape_configs` (prometheus.yml)

Add the scrape job to your Prometheus configuration. Use service discovery or static targets depending on your deployment.

```yaml
scrape_configs:
  - job_name: 'kafka-jmx'
    scrape_interval: 15s
    metrics_path: '/metrics'
    scheme: http
    static_configs:
      - targets: ['10.71.135.15:9404', '10.71.135.16:9404', '10.71.135.17:9404']
        labels:
          cluster: 'prod-kafka'
    relabel_configs:
      # Drop metrics with extremely high cardinality if needed (example)
      - source_labels: ['__name__']
        regex: 'kafka_topic_.*'
        action: keep
```

Example with docker-compose host-mapped ports (if you mapped 9404 per-broker to different host ports):

```yaml
scrape_configs:
  - job_name: 'kafka-jmx-hostmapped'
    static_configs:
      - targets: ['10.71.135.15:9404','10.71.135.15:9405','10.71.135.15:9406']
        labels:
          cluster: 'external-qa'
```

---

## Alerts (short examples and ideas)

- Broker down / unreachable (no scrape): alert when `up{job="kafka-jmx"} == 0` for 2 minutes.
- Under-replicated partitions: alert when `kafka_server_replicamanager_under_replicated_partitions > 0`.
- Broker disk usage: alert when node disk usage > 80%.
- Request handler saturation: alert if `kafka_manager_broker_jmx_request_handler_idle_percent` falls below a threshold.

Tune thresholds to your workload.

---

## Metric cardinality & relabeling guidance

- Avoid scraping or retaining metrics with unbounded label dimensions (e.g., per-client-id with many unique values).
- Use exporter-side filters or Prometheus `relabel_configs` to drop high-cardinality metrics or rename labels.
- Use aggregation rules in Prometheus (recording rules) to precompute expensive queries.

---

## Integration with Kafka Manager

- Recommended: keep Kafka Manager as a consumer of metrics (query Prometheus via the HTTP API for dashboards and cross-correlation) rather than making it responsible for scraping metrics.
- Kafka Manager can present combined views by joining its metadata (topics, brokers, partitions) with metric time-series queried from Prometheus.
- In environments without Prometheus, a limited built-in JMX collector (like `BrokerJmxMetricsCollectorService`) is useful for local/dev uses — but it is not a replacement for Prometheus in production.

---

## Troubleshooting tips

- If metrics are missing:
  1. Confirm the exporter process is running inside the broker container and bound to the expected port.
  2. From the Prometheus server host, `curl http://<broker-host>:9404/metrics` and verify response.
  3. Check Prometheus scrape logs and `up` metric for the job.
  4. Look at broker logs for exporter or JVM startup errors.

- If you must use RMI/JMX (legacy): ensure `-Dcom.sun.management.jmxremote.rmi.port` is set and `-Djava.rmi.server.hostname` points to an address reachable by the client, and map host ports accordingly (see README notes).

---

## References & further reading

- Prometheus JMX exporter: https://github.com/prometheus/jmx_exporter
- Jolokia: https://jolokia.org/
- Kafka monitoring best practices: official Confluent/Kafka monitoring guides


Document created on: 2026-08-24
