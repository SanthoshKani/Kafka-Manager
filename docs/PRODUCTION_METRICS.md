# Production Metrics Plan

This document describes the Kafka metrics that are exposed today, the Admin-derived structural metrics added in this change, and the additional broker metrics that should be collected in production for a Kafka 4.3.x cluster.

## What is exposed today

### Application metrics
- Spring Boot Actuator is enabled.
- Prometheus export is enabled at `/management/prometheus`.
- Internal runtime metrics already visible in the app include:
  - Admin request timing and error counts from `KafkaAdminExecutionService`
  - Async executor gauges from `AsyncConfiguration`

### Admin-derived structural metrics exposed by the app
These are computed from Kafka metadata using AdminClient and exposed as Micrometer gauges plus a REST snapshot endpoint:
- `kafka.manager.structural.offline.partitions`
- `kafka.manager.structural.under.replicated.partitions`
- `kafka.manager.structural.partition.count`
- `kafka.manager.structural.active.controller.count`
- `kafka.manager.structural.leader.count` (tagged by `brokerId`)

REST snapshot endpoint:
- `GET /api/v1/metrics/structural`

## Broker-side metrics exposed by the app
The application now includes a broker JMX collector that reads Kafka broker JMX and publishes the latest snapshot to Micrometer gauges and a REST endpoint.

REST snapshot endpoint:
- `GET /api/v1/metrics/broker-jmx`

Current broker-side JMX metrics collected by the app:

### Request process rate
- Produce requests per minute
- Fetch consumer requests per minute
- Fetch follower requests per minute

### Time taken for requests
- Total produce request time per minute
- Total fetch consumer request time per minute
- Total fetch follower request time per minute

### Network processor and request handler
- Network processor average idle percent
- Request handler average idle percent

### Topic / broker metrics
- Bytes in per minute
- Bytes out per minute
- Failed fetch requests per minute
- Failed produce requests per minute
- Messages in per minute
- Bytes rejected per minute
- Log flush rate

## Metrics that still need broker-side collection
The following are still best collected from Kafka broker JMX or a JMX exporter if you want to centralize them outside the application:

### Controller metrics
- Leader election rate
- Unclean leader election rate
- Offline partitions count

### Replication manager metrics
- ISR expands per minute
- ISR shrinks per minute
- Leader count per broker
- Partition count
- Under-replicated partitions

## Runtime aggregation and diagnostics endpoints (new)
The application exposes read-only REST endpoints that report runtime metric availability, derived rates, and diagnostic metadata. These endpoints read only from in-memory sample and admin stores — they do NOT perform synchronous scrapes or AdminClient calls during user requests.

Key characteristics:
- Read-only: data is produced from cached sample stores and admin snapshots only.
- No synchronous scraping: requests must never trigger a network scrape to a broker or controller.
- Supported windows: initially only `1m` (one minute) is supported; unsupported windows are rejected with the repository-standard validation response.
- Rate derivation: monotonic counters are converted to per-minute rates using two samples (earliest at-or-before window start and latest); decreases are treated as counter resets.
- Gauge aggregation: time-weighted average over the window (RollingGaugeAggregator) when adequate coverage exists.
- Availability & status: each metric includes source backend, collection timestamps, freshness, and an availability status (e.g. OK, INSUFFICIENT_DATA, COUNTER_RESET, UNAVAILABLE).
- Security & gating: diagnostics are feature-gated and the diagnostics endpoint is admin-only by repository convention.
- No sensitive data: diagnostics return metric names and label key names only; they do NOT include label values, headers, credentials, or arbitrary scrape payloads.

Primary endpoints:
- `GET /api/v1/clusters/{clusterId}/brokers/{brokerId}/metrics?window=1m`
  - Broker-level view: derived per-minute rates, gauge averages, idle percents, request/traffic rates, source backend and timestamps, and admin structural values for comparison (e.g. leader count, under-replicated partitions).
- `GET /api/v1/clusters/{clusterId}/topics/{topic}/metrics?window=1m[&perBroker=true]`
  - Topic-level aggregates across brokers. Optional per-broker breakdown (`perBroker=true`). Returns common topic metrics such as bytes in/out per minute, messages in per minute, failed produce/fetch per minute when available. Unavailable metrics are explicitly represented.
- `GET /api/v1/clusters/{clusterId}/metrics?window=1m`
  - Cluster-level summary combining Admin-derived structural metrics and aggregated runtime availability. Preserves independent timestamps for Admin and runtime data and reports expected/fresh/stale/unavailable broker counts.
- `GET /api/v1/clusters/{clusterId}/brokers/{brokerId}/metrics/diagnostics`
  - Admin-only diagnostics (feature disabled by default). Returns the recognized exporter metric source names mapped to canonical metric names and the set of label keys that the system expects for each metric, plus an Admin snapshot for structural comparison. Response size is limited (configurable via `app.metrics.prometheus-scrape.diagnostics-max-items`, default 500).

Configuration and properties:
- `app.metrics.prometheus-scrape.diagnostics-enabled` (default: false) — feature gate for diagnostics endpoint.
- `app.metrics.prometheus-scrape.diagnostics-max-items` (default: 500) — limits items returned by diagnostics to avoid excessively large responses.

Mapping and validation notes:
- Metric canonicalization is performed by `MetricMapper` which includes metadata for label key names. Diagnostics surface those canonical names and label keys so operators can validate exporter outputs without exposing values.
- Admin-derived structural metrics (from `AdminClient`) are considered the structural source of truth. Runtime broker or controller gauges are exposed for comparison but are not used to overwrite Admin-derived values.
- Leader-election and other counters are treated as monotonic counters; decreases are reported as counter resets and marked accordingly in availability status.

Testing and safety:
- Endpoints are covered by unit tests that verify authorization, feature gating, counter-reset handling, controller transitions, unsupported window validation, topic aggregation (including special characters), and partial/unavailable broker scenarios.
- The diagnostics endpoint only returns metric names + label keys (no label values or scrape contents).


## How the metrics should be collected in production

### 1. Admin-derived structural metrics
These metrics are computed in the application by polling metadata and should be treated as a snapshot:
- Poll interval: configurable via `app.metrics.admin-derived.poll-interval`
- Source of truth:
  - `Admin.listTopics(...).names()`
  - `Admin.describeTopics(...)`
  - `Admin.describeCluster().controller()`

### 2. Broker counters and gauges
The app already polls Kafka broker JMX directly for the metrics listed above and exports them via Micrometer.

If you want a separate observability pipeline, use Kafka JMX through a JMX exporter or equivalent collector.

Recommended collection model:
- Export broker JMX metrics on each controller and broker.
- Scrape them centrally with Prometheus.
- Convert monotonic counters to per-minute rates in Prometheus or in a metrics pipeline.

### 3. Rate computation
For any counter-like metric:
- Capture two samples `v1` and `v2`
- Capture timestamps `t1` and `t2`
- Compute per-minute rate as:

```text
rate_per_min = (v2 - v1) / (t2 - t1) * 60
```

## Compose changes in this repository
The compose file now exposes JMX ports for controllers and brokers so the above broker-side metrics can be collected in local development:
- controllers: `19101`, `19102`, `19103`
- brokers: `19111`, `19112`, `19113`

The app also gets:
- `APP_METRICS_ADMIN_DERIVED_POLL_INTERVAL=60s`
- `APP_METRICS_BROKER_JMX_ENABLED=true` in local profile via `application-local.yml`

## Recommended production dashboards and alerts

### Cluster health alerts
- Offline partitions > 0
- Under-replicated partitions > 0
- Active controller count = 0
- Leader count skew across brokers

### Performance alerts
- Produce / fetch request rates spike
- Bytes in/out spikes without matching consumer throughput
- Failed produce/fetch requests increase
- Request handler idle percent drops below threshold
- Network processor idle percent drops below threshold

## Implementation notes
- Structural metrics are intentionally exposed as a snapshot, not as raw live broker counters.
- Broker-side performance metrics are still pending collection via JMX exporter / Prometheus.
- Keep internal JMX and admin credentials out of source control.

