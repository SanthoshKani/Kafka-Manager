# Kafka Manager

KRaft-native Kafka cluster manager built with Spring Boot, Spring MVC, Spring Security and the Kafka AdminClient.

## Stack

- Java 25
- Spring Boot 4.1.0
- Spring Web MVC
-- In-memory cluster registry: the cluster registry loads cluster definitions from environment if provided
- Kafka AdminClient
- OAuth2 Resource Server / Basic Auth
- Micrometer + Prometheus
- springdoc-openapi
- Docker Compose
- Bucket4j (rate limiting)
- Resilience4j (circuit breaker)

## What it exposes

### Cluster Registry
- Register, update, enable, disable, validate, delete clusters
- Capability report per cluster

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
docker compose up --build
```

App: `http://localhost:8080`

Actuator: `http://localhost:8080/management`

OpenAPI: `http://localhost:8080/api-docs` (or `openapi.yaml` in repo root)

## Profiles

 - `default`: Application runtime. The cluster registry runs in-memory and loads cluster definitions from environment (see below).
 - `it`: Compose-backed integration profile for live Kafka validation

## Configuration

Key environment variables (see `application.yaml` for full list):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_MANAGER_CLUSTERS` | - | Optional JSON array of cluster definitions loaded at startup into the in-memory registry. Example: [{"displayName":"prod","bootstrapServers":"kafka:9092","securityProtocol":"PLAINTEXT","enabled":true}]
| `KAFKA_MANAGER_CLUSTERS` | - | Optional JSON array of cluster definitions loaded at startup into the in-memory registry. Example: [{"displayName":"prod","bootstrapServers":"kafka:9092","securityProtocol":"PLAINTEXT","enabled":true}]
| `KAFKA_MANAGER_MASTER_KEY_BASE64` | (dev default) | AES master key for encrypting cluster secrets |
| `KAFKA_MANAGER_BASIC_AUTH_USERNAME` | `admin` | Basic Auth username |
| `KAFKA_MANAGER_BASIC_AUTH_PASSWORD` | `admin` | Basic Auth password |
| `KAFKA_MANAGER_OAUTH2_ISSUER_URI` | - | OIDC issuer for OAuth2 Resource Server |
| `KAFKA_MANAGER_OAUTH2_JWK_SET_URI` | - | JWK set URI for OAuth2 Resource Server |
| `KAFKA_MANAGER_ADMIN_CACHE_SIZE` | `64` | AdminClient cache size |
| `KAFKA_MANAGER_DEFAULT_REQUEST_TIMEOUT` | `5s` | AdminClient request timeout |
| `KAFKA_MANAGER_DEFAULT_OPERATION_TIMEOUT` | `30s` | AdminClient operation timeout |

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

- **Basic Authentication**: HTTP Basic Auth for username/password authentication (configured via `KAFKA_MANAGER_BASIC_AUTH_USERNAME` and `KAFKA_MANAGER_BASIC_AUTH_PASSWORD`)
- **OAuth2 Resource Server**: Validates JWT Bearer tokens via issuer-uri or JWK set URI (configured via `KAFKA_MANAGER_OAUTH2_ISSUER_URI` or `KAFKA_MANAGER_OAUTH2_JWK_SET_URI`)
- **Cluster secrets encrypted**: Bootstrap servers, SASL credentials, TLS config encrypted at rest with AES-GCM (master key from `KAFKA_MANAGER_MASTER_KEY_BASE64`)
- **Actuator endpoints**: Secured, read-only access for health/info/prometheus
- **CORS**: Disabled by default; configure via `SecurityConfig` if needed
- **Rate limiting**: Bucket4j token bucket (300 req/min per `X-Client-Id` header, falls back to remote address)

Supported Kafka client security:

- AdminClient supports PLAINTEXT, SSL, SASL_PLAINTEXT and SASL_SSL connection modes.
- SASL mechanisms supported: `SCRAM-SHA-256`, `SCRAM-SHA-512`, and `PLAIN` (configure via cluster registration fields).
- TLS: provide truststore/keystore as a file path or as base64-encoded keystore bytes; temporary files are created securely when needed.
- All security-sensitive values (passwords, keystore bytes) should be supplied via the secret fields and are stored encrypted.

## API Endpoints (v1)

Base path: `/api/v1`

### Clusters
```
GET    /clusters                    # List clusters
POST   /clusters                    # Register cluster
GET    /clusters/{id}               # Get cluster detail
PATCH  /clusters/{id}               # Update cluster
DELETE /clusters/{id}               # Delete cluster
POST   /clusters/{id}/validate      # Validate cluster connectivity
GET    /clusters/{id}/capability    # Capability report
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
```
GET    /clusters/{clusterId}/client-metrics                 # List client metrics resources
```

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

# Integration tests (requires Docker)
./gradlew integrationTest

# Full verification (includes integration tests + coverage)
./gradlew check
```

## Notes

- `AdminClient` instances are cached per cluster and invalidated on registry changes.
- Kafka admin calls are bounded, logged, translated to RFC 9457 problem details, and recorded as persisted operations for mutating flows.
- The compose-backed integration suite validates the live REST-to-Kafka path for the supported cluster surfaces.
- `openapi.yaml` is the exported contract snapshot for the REST API.
- `openapi.yaml` is the exported contract snapshot for the REST API.
- When running with the in-memory registry, no database or migrations are required for cluster registry functionality; all data is kept in-memory and cleared on restart.
- HikariCP pool tuned for production (leak detection, validation, max lifetime).
- Structured logging with correlation IDs via `CorrelationIdFilter`.
