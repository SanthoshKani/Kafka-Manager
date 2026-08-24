# Kafka Manager

KRaft-native Kafka cluster manager built with Spring Boot, Spring MVC, Spring Security and the Kafka AdminClient.

## Stack

- Java 25
- Spring Boot 4.1.0
- Spring Web MVC
- Single Kafka cluster configured through `BOOTSTRAP_SERVERS_CONFIG`
- Kafka AdminClient
- OAuth2 Resource Server / Basic Auth
- Micrometer + Prometheus
- springdoc-openapi
- Docker Compose
- Bucket4j (rate limiting)
- Resilience4j (circuit breaker)

## What it exposes

### Kafka Admin Surfaces

**Topics**
- List, describe, create, delete
- Increase partitions
- Update configs (incremental alter)
- List offsets, delete records

**Brokers**
- List, describe configs, alter configs

**Consumer Groups**
- List, describe, delete
- Alter offsets, remove members

**Cluster Actions**
- Preferred/unclean leader election
- Partition reassignment
- Log dir describe/alter

**Metadata Quorum**
- Voter/observer status

**Client Metrics Resources**
- List client metrics resources

**ACLs**
- Describe, create, delete

**SCRAM Credentials**
- Describe, upsert, delete

**Delegation Tokens**
- List, create, renew, expire

### Operations (Async)
- Persisted request/response records with events
- Retry/cancel for submitted operations
- Polling-based status tracking

## Run Locally

```bash
$env:SPRING_PROFILES_ACTIVE = 'local'; docker compose up --build
```

App: `http://localhost:8080`

Actuator: `http://localhost:8080/management`

OpenAPI: `http://localhost:8080/api-docs` (or `openapi.yaml` in repo root)

## Profiles

  - `local`: Open HTTP profile for IDE and docker-compose development. Connects to Kafka over PLAINTEXT only and does not configure certs, keystores, or truststores.
  - `prod`: Secure profile with Basic Auth / OAuth2 Resource Server, SSL or mTLS Kafka connectivity, and rate limiting enabled. This is the default fallback when no profile is set.
  - `it`: Compose-backed integration profile for live Kafka validation

## Configuration

Key environment variables (see `application-local.yml` and `application-prod.yml` for full list):

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_PROFILES_ACTIVE` | `local` | Selects the runtime profile (`local` or `prod`) |
| `KAFKA_MANAGER_BASIC_AUTH_USERNAME` | `admin` | Production Basic Auth username |
| `KAFKA_MANAGER_BASIC_AUTH_PASSWORD` | `admin` | Production Basic Auth password |
| `KAFKA_MANAGER_OAUTH2_ISSUER_URI` | - | OIDC issuer for OAuth2 Resource Server |
| `KAFKA_MANAGER_OAUTH2_JWK_SET_URI` | - | JWK set URI for OAuth2 Resource Server |
| `BOOTSTRAP_SERVERS_CONFIG` | `localhost:19092,localhost:29092,localhost:39092` (local) | Kafka bootstrap servers for the singleton AdminClient |
| `KAFKA_ADMIN_SECURITY_PROTOCOL` | `PLAINTEXT` in local, `SSL` in prod | Kafka client protocol |
| `javax.net.ssl.keyStore`, `javax.net.ssl.trustStore` | - | Production Kafka client keystore/truststore file paths |
| `javax.net.ssl.keyStorePassword`, `javax.net.ssl.trustStorePassword` | - | Production Kafka client keystore/truststore passwords |

### Rate Limiting
- Enabled by default (`app.rate-limit.enabled=true`)
- Capacity: 300 requests per refill period
- Refill period: 1 minute
- Key header: `X-Client-Id` (falls back to remote address)

### Circuit Breaker (Resilience4j)
- Applied to Kafka AdminClient calls
- Sliding window: 10 calls
- Failure threshold: 50%
- Wait in open state: 30s
- Records exceptions: AuthenticationException, AuthorizationException, TimeoutException, IOException

## Security

- **Local profile**: No HTTP authentication, no rate limiting, no bearer security scheme in OpenAPI, and no Kafka SSL material.
- **Production profile**: HTTP Basic Auth and OAuth2 Resource Server are enabled, actuator endpoints remain secured, and Kafka AdminClient SSL/mTLS settings are read from production profile properties or JVM SSL system properties.
- **Rate limiting**: Bucket4j token bucket (300 req/min per `X-Client-Id` header, falls back to remote address) in production only.

Supported Kafka client security:

- Local profile uses PLAINTEXT only.
- Production profile supports SSL and mTLS via JVM keystore/truststore settings.

## API Endpoints (v1)

Base path: `/api/v1`

### Clusters
```
The app no longer exposes cluster registry CRUD endpoints.
Cluster-scoped endpoints still take a `clusterId` path variable for compatibility with existing resource routes.
```

### Topics
```
GET    /clusters/{clusterId}/topics                                    # List topics
POST   /clusters/{clusterId}/topics                                    # Create topic
GET    /clusters/{clusterId}/topics/{topicName}                        # Describe topic
DELETE /clusters/{clusterId}/topics/{topicName}                        # Delete topic
POST   /clusters/{clusterId}/topics/{topicName}/partitions             # Increase partitions
GET    /clusters/{clusterId}/topics/{topicName}/configs                # Describe configs
PATCH  /clusters/{clusterId}/topics/{topicName}/configs                # Alter configs (incremental)
GET    /clusters/{clusterId}/topics/{topicName}/offsets                # List offsets
POST   /clusters/{clusterId}/topics/{topicName}/records/delete         # Delete records
```

### Brokers
```
GET    /clusters/{clusterId}/brokers                          # List brokers
GET    /clusters/{clusterId}/brokers/{brokerId}               # Describe broker
GET    /clusters/{clusterId}/brokers/{brokerId}/configs       # Describe broker configs
PATCH  /clusters/{clusterId}/brokers/{brokerId}/configs       # Alter broker configs
```

### Consumer Groups
```
GET    /clusters/{clusterId}/consumer-groups                              # List groups
GET    /clusters/{clusterId}/consumer-groups/{groupId}                    # Describe group
DELETE /clusters/{clusterId}/consumer-groups/{groupId}                    # Delete group
POST   /clusters/{clusterId}/consumer-groups/{groupId}/offsets            # Alter offsets
POST   /clusters/{clusterId}/consumer-groups/{groupId}/members/remove     # Remove members
```

### Cluster Actions
```
POST   /clusters/{clusterId}/leader-election/preferred    # Preferred leader election
POST   /clusters/{clusterId}/leader-election/unclean      # Unclean leader election
POST   /clusters/{clusterId}/partition-reassignment       # Partition reassignment
GET    /clusters/{clusterId}/log-dirs                     # Describe log dirs
PATCH  /clusters/{clusterId}/log-dirs                     # Alter log dirs (replica moves)
```

### Metadata Quorum
```
GET    /clusters/{clusterId}/metadata-quorum                # Quorum status
```

### Client Metrics

Note: Cluster Manager no longer exposes built-in runtime/client metrics endpoints for production use. We recommend using Prometheus with the JMX exporter or Jolokia for broker and client metrics. See `docs/PRODUCTION_MONITORING.md` for guidance and sample scrape configuration.

### ACLs
```
GET    /clusters/{clusterId}/acls                           # List ACLs (with filters)
POST   /clusters/{clusterId}/acls                           # Create ACLs
DELETE /clusters/{clusterId}/acls                           # Delete ACLs
```

### SCRAM Credentials
```
GET    /clusters/{clusterId}/scram                          # List SCRAM users
POST   /clusters/{clusterId}/scram                          # Upsert SCRAM credential
DELETE /clusters/{clusterId}/scram                          # Delete SCRAM credential
```

### Delegation Tokens
```
GET    /clusters/{clusterId}/delegation-tokens              # List tokens
POST   /clusters/{clusterId}/delegation-tokens              # Create token
POST   /clusters/{clusterId}/delegation-tokens/renew        # Renew token
POST   /clusters/{clusterId}/delegation-tokens/expire       # Expire token
```

### Operations (Async)
```
POST   /operations                                          # Submit operation
GET    /operations                                          # List operations (with filters)
GET    /operations/{id}                                     # Get operation detail
GET    /operations/{id}/events                              # Get operation events (polling)
POST   /operations/{id}/retry                               # Retry failed operation
POST   /operations/{id}/cancel                              # Cancel pending operation
```

## Tests

```bash
# Unit + slice tests
./gradlew test

# Full verification
./gradlew check
```

## Notes

- Set `BOOTSTRAP_SERVERS_CONFIG` before starting the app; the Kafka Manager now connects to one shared Kafka cluster through a singleton `AdminClient`.
- Kafka admin calls are bounded, logged, translated to RFC 9457 problem details, and recorded as persisted operations for mutating flows.
- `openapi.yaml` is the exported contract snapshot for the REST API.
- HikariCP pool tuned for production (leak detection, validation, max lifetime).
- Structured logging with correlation IDs via `CorrelationIdFilter`.

## Broker JMX (Docker) — setup & security

- When running brokers in Docker and exposing JMX over the host, set a stable host address that the broker will advertise for RMI (the collector must be able to reach this address). Example environment variables for each broker/container:

```yaml
KAFKA_HOST_IP: 10.71.135.15            # host IP reachable by kafka-manager
KAFKA_JMX_PORT: 9101                   # container JMX port (fixed)
KAFKA_JMX_HOSTNAME: ${KAFKA_HOST_IP}   # ensure the RMI stub advertises a reachable address
KAFKA_JMX_OPTS: >-
  -Dcom.sun.management.jmxremote=true
  -Dcom.sun.management.jmxremote.authenticate=false   # local/dev only; enable auth in prod
  -Dcom.sun.management.jmxremote.ssl=false            # local/dev only; enable SSL in prod
  -Dcom.sun.management.jmxremote.port=9101
  -Dcom.sun.management.jmxremote.rmi.port=9101
  -Djava.rmi.server.hostname=${KAFKA_HOST_IP}
```

- Map a host port to the container JMX port (example in docker-compose: `19111:9101`). Configure `application-local.yml` to use the host IP and mapped port (for example `10.71.135.15:19111`) so the `BrokerJmxMetricsCollectorService` connects to the correct TCP endpoint.

- Security caveats:
  - The `-Dcom.sun.management.jmxremote.authenticate=false` and `-Dcom.sun.management.jmxremote.ssl=false` options are only acceptable for isolated development environments. Do NOT use these settings in production as they expose an unauthenticated RMI interface.
  - For production, enable JMX authentication and TLS (or use a Prometheus JMX exporter or Jolokia inside the container to expose metrics over an authenticated/HTTP endpoint). A safer pattern is to run the Prometheus JMX exporter as a Java agent inside the broker and scrape over HTTP (single TCP port) rather than relying on RMI.
  - Ensure firewall rules only allow trusted hosts to reach the JMX ports, and prefer network-level restrictions (VPC/subnet, host firewall) in addition to JMX authentication.

- If you encounter "no metrics" or connection failures from the application, check:
  1. The host:port is reachable from the kafka-manager host (telnet or curl is sufficient for a TCP check).
  2. `java.rmi.server.hostname` inside the broker is set to the host address the collector uses to connect.
  3. The container uses a fixed `com.sun.management.jmxremote.rmi.port` to avoid ephemeral ports.
  4. Broker logs for JMX binding errors and kafka-manager logs for connector exceptions.

