# Kafka Manager - Developer Guide

## Overview

Kafka Manager is a Spring Boot application for managing Apache Kafka clusters. It provides a REST API for cluster administration, topic management, broker operations, consumer group management, ACLs, SCRAM credentials, delegation tokens, and more.

## Technology Stack

- **Java 25**
- **Spring Boot 4.1.0**
-- **In-memory storage** for clusters, secrets and operations
- **Spring Security** (Basic Auth / OAuth2 Resource Server)
- **Spring Web MVC** (Servlet-based REST API)
- **Caffeine** for AdminClient caching
- **Bucket4j** for rate limiting
- **Resilience4j** for circuit breaker
- **OpenAPI/Swagger** for API documentation
- **Gradle** for build

## Project Structure

```
src/main/java/com/opentext/security/analytics/messagehub/kafkamanager/
├── KafkaManagerApplication.java          # Application entry point
├── config/                               # Spring configuration classes
│   ├── AsyncConfiguration.java           # Async executor with metrics
│   ├── CorrelationIdFilter.java          # Request correlation ID tracking
│   ├── JacksonConfig.java                # ObjectMapper configuration
│   ├── KafkaManagerProperties.java       # Consolidated configuration properties
│   ├── OpenApiConfig.java                # OpenAPI/Swagger configuration
│   ├── RateLimitingConfig.java           # Bucket4j rate limiting filter
│   └── SecurityConfig.java               # Spring Security filter chain
├── common/                               # Shared utilities and exceptions
│   ├── ApiAccessDeniedHandler.java
│   ├── ApiAuthenticationEntryPoint.java
│   ├── ApiErrorCode.java
│   ├── ApiException.java
│   ├── ApiProblemAdvice.java             # RFC 9457 Problem Details
│   ├── ConflictException.java
│   ├── InvalidOperationException.java
│   ├── JsonSupport.java
│   ├── KafkaAdminException.java
│   ├── ProblemResponseWriter.java
│   ├── ResourceNotFoundException.java
│   └── TopicPartitionRequest.java
├── kafkaadmin/                           # Kafka AdminClient abstraction
│   ├── KafkaAdminExecutionService.java   # Circuit breaker wrapper
│   └── KafkaEndpointSupport.java         # Endpoint normalization helpers
├── topics/                               # Topic management
│   ├── api/
│   │   ├── TopicController.java
│   │   ├── TopicCreateRequest.java
│   │   ├── TopicDetailResponse.java
│   │   ├── TopicSummaryResponse.java
│   │   ├── TopicConfigMutationRequest.java
│   │   ├── TopicPartitionExpansionRequest.java
│   │   └── ... (other DTOs)
│   └── service/
│       ├── TopicService.java             # Topic CRUD & queries
│       └── TopicMutationService.java     # Config mutations, partition expansion
├── brokers/                              # Broker management
│   ├── api/
│   │   ├── BrokerController.java
│   │   ├── BrokerSummaryResponse.java
│   │   └── BrokerConfigMutationRequest.java
│   └── service/
│       └── BrokerService.java
├── consumergroups/                       # Consumer group management
│   ├── api/
│   │   ├── ConsumerGroupController.java
│   │   ├── ConsumerGroupDetailResponse.java
│   │   ├── ConsumerGroupSummaryResponse.java
│   │   ├── ConsumerGroupOffsetUpdateRequest.java
│   │   └── ... (other DTOs)
│   └── service/
│       └── ConsumerGroupService.java
├── acls/                                 # ACL management
│   ├── api/
│   └── service/
├── scram/                                # SCRAM credential management
│   ├── api/
│   └── service/
├── delegationtokens/                     # Delegation token management
│   ├── api/
│   └── service/
├── metadataquorum/                       # KRaft metadata quorum
│   ├── api/
│   └── service/
├── clientmetrics/                        # Client metrics
│   ├── api/
│   └── service/
└── operations/                           # Long-running operations tracking
    ├── api/
    └── service/
```

## Code Flow - Key Request Paths

### 1. Profile-Scoped Startup Flow

```
Application startup
    → Select `local` or `prod` profile
    → Load profile-specific HTTP security, OpenAPI, and Kafka AdminClient config
    → Create the singleton AdminClient with PLAINTEXT (local) or SSL/mTLS (prod)
    → Expose unsecured endpoints in local and secured endpoints in prod
```

**Key Classes:**
- `LocalSecurityConfig` - Permissive HTTP filter chain for IDE and compose runs
- `SecurityConfig` - Production HTTP security chain with Basic Auth / OAuth2 Resource Server
- `LocalKafkaAdminClientPropertiesFactory` - PLAINTEXT-only AdminClient properties
- `ProdKafkaAdminClientPropertiesFactory` - SSL/mTLS AdminClient properties

### 2. AdminClient Creation & SSL/TLS Flow

```
Any Kafka Admin Operation
    → Use the Spring-managed singleton AdminClient
    → Apply bootstrap servers + SSL/TLS settings from app configuration
    → Execute operation through KafkaAdminExecutionService
```

**Key Classes:**
- `KafkaAdminClientConfiguration` - Spring bean that builds/configures AdminClient handles
- `KafkaClientPropertyPolicyService` - Validates allowed properties

### 3. Topic Management Flow

```
POST /api/v1/clusters/{clusterId}/topics
    → TopicController.createTopic()
    → TopicService.createTopic()
    → AdminClientRegistry.getAdminClient(clusterId)
    → AdminClient.createTopics()
    → Returns TopicDetailResponse
```

**Key Classes:**
- `TopicController` - REST endpoints
- `TopicService` - Business logic
- `TopicMutationService` - Config mutations, partition expansion

### 4. Consumer Group Management Flow

```
GET /api/v1/clusters/{clusterId}/consumer-groups
    → ConsumerGroupController.listConsumerGroups()
    → ConsumerGroupService.listConsumerGroups()
    → AdminClientRegistry.getAdminClient(clusterId)
    → AdminClient.listConsumerGroups()
    → Returns List<ConsumerGroupSummaryResponse>
```

### 5. Security & Rate Limiting Flow

```
Incoming Request
    → CorrelationIdFilter (adds X-Correlation-Id to MDC)
    → SecurityConfig filter chain
        → Basic Auth / OAuth2 Resource Server validation
        → Security headers (HSTS, CSP, etc.)
    → RateLimitingConfig (Bucket4j filter)
        → Token bucket per keyHeader (default: X-Client-Id)
    → Controller
```

## Configuration

### Application Properties (`application.yaml` / `application-local.yml`)

```yaml
app:
  serviceName: "kafka-manager"
  security:
    basicAuth:
      username: "admin"
      password: "admin"
    oauth2ResourceServer:
      issuerUri: "https://auth.example.com"
      jwkSetUri: "https://auth.example.com/.well-known/jwks.json"
  admin:
    cacheSize: 100
    defaultRequestTimeout: 30s
    defaultOperationTimeout: 60s
    connectionValidationTimeout: 10s
  operations:
    pollInterval: 5s
    leaseDuration: 30s
  clusterRegistry:
    maxPageSize: 100
    maxClientProperties: 50
  rateLimit:
    enabled: true
    capacity: 1000
    refillPeriod: 1m
    keyHeader: "X-Client-Id"

# Local profile: no HTTP auth, plaintext Kafka only.
# Prod profile: Basic Auth / OAuth2 Resource Server plus SSL or mTLS Kafka settings.
```

### Metrics

The built-in metrics subsystem has been removed from this repository. Production monitoring should be performed using external tooling (for example: broker JMX exporters scraped by Prometheus, centralized exporters, or vendor-managed observability agents). See `docs/PRODUCTION_MONITORING.md` for recommended production monitoring practices and dashboards.

### Data Persistence

This project uses in-memory repositories for development and testing. The in-memory stores are implemented in the `operations.service` package and provide thread-safe ConcurrentHashMap-backed storage. Data is not persisted across restarts. If you need durability, swap in a persistent implementation behind the `OperationStore` abstractions.

## Testing

### Unit Tests
```bash
./gradlew test
```
Located in `src/test/java/...`

### Integration Tests
```bash
./gradlew integrationTest
```
Requires:
- Kafka cluster (via docker-compose)
Located in `src/integrationTest/java/...`

### Test Profiles
- `test` - Unit tests (in-memory stores)
- `it` - Integration tests with Testcontainers

## Key Design Patterns

### 1. AdminClient Caching with Fingerprint Invalidation
`AdminClientRegistry` uses Caffeine cache keyed by clusterId. The cache key includes a fingerprint hash of all connection properties. When cluster config changes, the fingerprint changes, automatically invalidating the cache.

### 2. Profile-Based Security
Local profile requests are permitted without authentication for easy IDE and docker-compose development. Production profile uses Basic Auth and OAuth2 Resource Server, while Kafka AdminClient SSL/mTLS settings are loaded only from production properties.

### 3. Circuit Breaker
`KafkaAdminExecutionService` wraps AdminClient calls with Resilience4j circuit breaker (sliding window, failure threshold, wait duration).

### 4. RFC 9457 Problem Details
All errors returned as `application/problem+json` via `ApiProblemAdvice` and `ProblemResponseWriter`.

### 5. Correlation ID Tracking
`CorrelationIdFilter` extracts/generates `X-Correlation-Id`, adds to MDC for logging, and includes in response headers.

## Adding New Features

### New REST Endpoint
1. Create request/response DTOs in appropriate `api/` package
2. Create controller in `api/` package
3. Create service in `service/` package
4. Use `AdminClientRegistry` for Kafka operations
5. Add OpenAPI annotations for documentation

### New Cluster Configuration Field
1. Add field to `ClusterEntity` (domain)
2. Add to `RegisterClusterRequest` / `UpdateClusterRequest` (api)
3. Add to `ClusterDetailResponse` (api)
4. Update `ClusterRegistryService.apply()` methods
5. Update `AdminClientRegistry.buildHandle()` to configure the property
6. Add to `KafkaClientPropertyPolicyService.ALLOWED_KEYS` if it's a client property
7. If persistent storage is added later, document migration steps and schema changes accordingly

## Common Tasks

### Run Locally
```bash
# Start infrastructure
docker-compose up -d

# Run application
./gradlew bootRun --args='--spring.profiles.active=local'
```

### Swagger UI / OpenAPI (local profile)

When running the application with the `local` Spring profile, the OpenAPI/Swagger UI is enabled and accessible without authentication. The project uses `springdoc-openapi` and a small profile-scoped configuration (`LocalOpenApiConfig` / `LocalSecurityConfig`) to expose the docs for development and IDE use.

How to run:

PowerShell (example):

```powershell
./gradlew bootRun --args='--spring.profiles.active=local'
```

Access the UI and spec (default port 8080):

- Swagger UI (interactive): http://localhost:8080/swagger-ui/index.html or http://localhost:8080/swagger-ui.html
- OpenAPI JSON: http://localhost:8080/v3/api-docs
- OpenAPI YAML: http://localhost:8080/openapi.yaml

Notes:

- The `local` profile's security configuration explicitly permits `GET` requests to `/v3/api-docs/**`, `/openapi.yaml`, and `/swagger-ui/**`, so you do not need to provide credentials when running with `--spring.profiles.active=local`.
- The OpenAPI server base is configured as `/api/v1` in `LocalOpenApiConfig`, so example requests shown in the UI will target the `/api/v1` paths.
- If you need the generated `openapi.yaml` artifact on disk, run: `./gradlew openapiGenerate`.

## Running Compose on a remote host (external compose)

If you prefer to run the Kafka infrastructure on a remote server (for
example `10.71.135.15`) and connect your IDE / local instance of the application
to that remote infrastructure, use the included `external-compose.yaml` which is
adapted for remote access.

Quick steps (server-side):

1. Copy `external-compose.yaml` to the remote host (10.71.135.15). Example using scp:

```powershell
scp .\external-compose.yaml user@10.71.135.15:/home/user/
```

2. SSH to the server and start the stack:

```bash
ssh user@10.71.135.15
docker compose -f external-compose.yaml up -d
```

3. Ensure the following external ports are open in the server firewall / cloud security group:

- Broker client ports: 19092, 29092, 39092
- Controller ports (KRaft controller listeners): 19093, 29093, 39093
 - JMX ports: 19111, 19112, 19113 (optional, only if you need JMX access)

Connect your local IDE / app to the remote stack:

- Set the bootstrap servers to the remote host's advertised broker ports:

  - 10.71.135.15:19092,10.71.135.15:29092,10.71.135.15:39092

- If you run the application locally (PowerShell):

```powershell
$env:BOOTSTRAP_SERVERS_CONFIG = '10.71.135.15:19092,10.71.135.15:29092,10.71.135.15:39092'
$env:SPRING_PROFILES_ACTIVE = 'local'
./gradlew bootRun
```

- Or with Bash (Linux/macOS):

```bash
export BOOTSTRAP_SERVERS_CONFIG='10.71.135.15:19092,10.71.135.15:29092,10.71.135.15:39092'
export SPRING_PROFILES_ACTIVE=local
./gradlew bootRun
```

Notes:

  - `external-compose.yaml` intentionally advertises broker endpoints using the
  server IP (so clients can reach the brokers across the network). If your
  server has multiple interfaces, use the interface reachable by your laptop.
  - The external compose intentionally does not require external persistent storage; the application uses in-memory stores by default.
- Avoid running `kafka-manager` inside the remote compose if you plan to run
  the application locally; running two instances can cause port conflicts or
  confuse integration tests.

### Build
```bash
./gradlew build
```

### Run from built JAR (Java -jar)

After building the project you can run the Spring Boot fat JAR directly with the `java -jar` command. Replace the JAR name with the actual file produced under `build/libs/` (it usually follows the pattern `projectName-version.jar`).

PowerShell (Windows):

```powershell
# Build the jar
.\gradlew.bat bootJar

# Run the jar with the local profile
java -jar .\build\libs\kafka-manager-1.0.0.jar --spring.profiles.active=local
```

Bash (macOS / Linux):

```bash
# Build the jar
./gradlew bootJar

# Run the jar with the local profile
java -jar build/libs/kafka-manager-1.0.0.jar --spring.profiles.active=local
```

Notes:

- If the produced JAR filename differs, list the `build/libs` directory and use the correct filename. Example (PowerShell): `Get-ChildItem .\build\libs`.
- You can also set the active profile via environment variable `SPRING_PROFILES_ACTIVE=local` (Bash) or `$env:SPRING_PROFILES_ACTIVE='local'` (PowerShell) before running the jar.


### Generate OpenAPI Spec
```bash
./gradlew openapiGenerate
```

### Persistent Storage Migration

This project does not use persistent-storage migrations in ordinary development: it uses in-memory stores by default. If durable storage is added later, document migration steps at that time.

## Troubleshooting

### AdminClient Connection Issues
- Check cluster configuration in application configuration or cluster registry
- Verify secrets are correctly provided in configuration (do not log secrets)
- Check Kafka broker listener configuration matches cluster config
- Review `KafkaAdminClientConfiguration` and `KafkaAdminExecutionService` for AdminClient setup and property mapping

### Rate Limiting
- Check `app.rateLimit.enabled`
- Verify `keyHeader` matches client header
- Monitor Bucket4j metrics

### Circuit Breaker Open
- Check Kafka broker availability
- Review `KafkaAdminExecutionService` configuration
- Check Resilience4j metrics

## Local REST API Testing with cURL

All endpoints can be tested against a locally running Kafka Manager instance (`http://localhost:8080`) using the `local` profile. Replace `CLUSTER_ID` with your cluster UUID (e.g. `11111111-1111-1111-1111-111111111111`).

### 1. Documentation & Actuator Endpoints

```bash
# Swagger UI interactive interface
curl -s http://localhost:8080/swagger-ui/index.html

# OpenAPI 3.0 Document (JSON)
curl -s http://localhost:8080/v3/api-docs

# OpenAPI 3.0 Document (YAML)
curl -s http://localhost:8080/openapi.yaml

# Actuator Health Probe
curl -s http://localhost:8080/management/health

# Actuator Info
curl -s http://localhost:8080/management/info

# Prometheus Scrape Metrics (JVM, Spring, and application counters)
curl -s http://localhost:8080/management/prometheus
```

---

### 2. Broker Management

```bash
# List all brokers and controller status
curl -s -X GET http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/brokers

# Describe broker configuration (e.g. broker id 101)
curl -s -X GET http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/brokers/101/configs

# Alter broker dynamic configuration (incremental mutation)
curl -s -X PATCH http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/brokers/101/configs \
  -H "Content-Type: application/json" \
  -d '{
    "changes": [
      {
        "name": "log.cleaner.min.compaction.lag.ms",
        "value": "0",
        "operation": "SET"
      }
    ]
  }'
```

---

### 3. Topic Management

```bash
# List all topics (excluding internal topics)
curl -s -X GET http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/topics

# List all topics including internal topics
curl -s -X GET "http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/topics?includeInternal=true"

# Create a new topic
curl -s -i -X POST http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/topics \
  -H "Content-Type: application/json" \
  -d '{
    "topicName": "my-sample-topic",
    "partitions": 3,
    "replicationFactor": 1,
    "configs": {
      "cleanup.policy": "delete",
      "retention.ms": "604800000"
    }
  }'

# Describe topic metadata, ISR, partitions, and configs
curl -s -X GET http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/topics/my-sample-topic

# Describe topic configs as key-value map
curl -s -X GET http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/topics/my-sample-topic/configs

# Alter topic configuration (batch mutation)
curl -s -i -X PATCH http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/topics/my-sample-topic/configs \
  -H "Content-Type: application/json" \
  -d '{
    "changes": [
      {
        "name": "retention.ms",
        "value": "3600000",
        "operation": "SET"
      }
    ]
  }'

# Expand topic partition count (e.g. expand to 4 partitions)
curl -s -i -X POST http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/topics/my-sample-topic/partitions \
  -H "Content-Type: application/json" \
  -d '{
    "totalPartitions": 4
  }'

# List latest partition offsets for a topic
curl -s -X GET "http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/topics/my-sample-topic/offsets?mode=LATEST"

# Delete records up to specified offsets
curl -s -i -X POST http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/topics/my-sample-topic/records/delete \
  -H "Content-Type: application/json" \
  -d '{
    "partitions": [
      {
        "partition": 0,
        "beforeOffset": 0
      }
    ]
  }'

# Delete topic (supports ?dryRun=true)
curl -s -i -X DELETE http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/topics/my-sample-topic
```

---

### 4. Consumer Group Management

```bash
# List all consumer groups
curl -s -X GET http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/consumer-groups

# Describe a consumer group (coordinator, members, lag, offsets)
curl -s -X GET http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/consumer-groups/my-consumer-group

# Alter consumer group committed offsets
curl -s -i -X POST http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/consumer-groups/my-consumer-group/offsets \
  -H "Content-Type: application/json" \
  -d '{
    "offsets": [
      {
        "topic": "my-sample-topic",
        "partition": 0,
        "offset": 0
      }
    ]
  }'

# Remove members from a consumer group
curl -s -i -X POST http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/consumer-groups/my-consumer-group/members/remove \
  -H "Content-Type: application/json" \
  -d '{
    "memberIds": ["member-1-guid"]
  }'

# Delete a consumer group
curl -s -i -X DELETE http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/consumer-groups/my-consumer-group
```

---

### 5. Cluster Admin & Actions

```bash
# Trigger preferred leader election
curl -s -i -X POST http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/actions/leader-election \
  -H "Content-Type: application/json" \
  -d '{
    "electionType": "PREFERRED",
    "topicPartitions": [
      {
        "topic": "my-sample-topic",
        "partition": 0
      }
    ]
  }'

# List active partition reassignments
curl -s -X GET http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/actions/partition-reassignments

# Start partition reassignment plan
curl -s -i -X PUT http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/actions/partition-reassignments \
  -H "Content-Type: application/json" \
  -d '{
    "reassignments": [
      {
        "topic": "my-sample-topic",
        "partition": 0,
        "targetReplicas": [101, 102]
      }
    ]
  }'

# Describe log directories across brokers
curl -s -X GET "http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/actions/log-dirs?brokerIds=101,102,103"

# Alter replica log directories
curl -s -i -X PUT http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/actions/log-dirs \
  -H "Content-Type: application/json" \
  -d '{
    "moves": [
      {
        "topic": "my-sample-topic",
        "partition": 0,
        "brokerId": 101,
        "targetLogDir": "/var/lib/kafka/data"
      }
    ]
  }'
```

---

### 6. Metadata Quorum (KRaft) & Client Metrics

```bash
# Get KRaft metadata quorum status (voters, leader, observers, high watermark)
curl -s -X GET http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/metadata-quorum

# List discovered client metric resources
curl -s -X GET http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/client-metrics
```

---

### 7. Access Control Lists (ACLs)

```bash
# List all ACLs
curl -s -X GET http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/acls

# Create ACL entries
curl -s -i -X POST http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/acls \
  -H "Content-Type: application/json" \
  -d '{
    "bindings": [
      {
        "resourceType": "TOPIC",
        "resourceName": "my-sample-topic",
        "patternType": "LITERAL",
        "principal": "User:alice",
        "host": "*",
        "operation": "READ",
        "permissionType": "ALLOW"
      }
    ]
  }'

# Delete ACL entries matching filter
curl -s -i -X POST http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/acls/delete \
  -H "Content-Type: application/json" \
  -d '{
    "filter": {
      "resourceType": "TOPIC",
      "resourceName": "my-sample-topic",
      "patternType": "LITERAL",
      "principal": "User:alice",
      "host": "*",
      "operation": "READ",
      "permissionType": "ALLOW"
    }
  }'
```

---

### 8. SCRAM Credentials

```bash
# Describe SCRAM credentials for user(s)
curl -s -X GET "http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/scram/users?userNames=alice,bob"

# Upsert SCRAM credential for a user
curl -s -i -X PUT http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/scram/users/alice \
  -H "Content-Type: application/json" \
  -d '{
    "mechanism": "SCRAM-SHA-512",
    "iterations": 4096,
    "password": "SamplePassword123!"
  }'

# Delete SCRAM credential for a user
curl -s -i -X DELETE http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/scram/users/alice \
  -H "Content-Type: application/json" \
  -d '{
    "mechanism": "SCRAM-SHA-512"
  }'
```

---

### 9. Delegation Tokens

```bash
# List all delegation tokens
curl -s -X GET http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/delegation-tokens

# Create a new delegation token
curl -s -X POST http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/delegation-tokens \
  -H "Content-Type: application/json" \
  -d '{
    "maxLifeTimeMs": 86400000
  }'

# Renew a delegation token
curl -s -X POST http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/delegation-tokens/{tokenId}/renew \
  -H "Content-Type: application/json" \
  -d '{
    "hmacBase64": "your-token-hmac-base64"
  }'

# Expire a delegation token
curl -s -i -X POST http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/delegation-tokens/{tokenId}/expire \
  -H "Content-Type: application/json" \
  -d '{
    "hmacBase64": "your-token-hmac-base64",
    "expiryTimePeriodMs": 0
  }'
```

---

### 10. Asynchronous Operations Tracking

```bash
# Submit a new asynchronous operation
curl -s -X POST http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/operations \
  -H "Content-Type: application/json" \
  -d '{
    "operationType": "create-topic",
    "dryRun": false,
    "requestedBy": "admin-user",
    "resourceName": "my-sample-topic",
    "payload": {
      "partitions": 3,
      "replicationFactor": 1
    }
  }'

# List operations with pagination
curl -s -X GET "http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/operations?page=0&size=10"

# Get operation details by ID
curl -s -X GET http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/operations/{operationId}

# Get operation event lifecycle logs
curl -s -X GET http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/operations/{operationId}/events

# Request cancellation of a pending operation
curl -s -i -X POST http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/operations/{operationId}/cancel

# Retry a failed operation
curl -s -i -X POST http://localhost:8080/api/v1/clusters/11111111-1111-1111-1111-111111111111/operations/{operationId}/retry
```

---

### 11. Metrics & Diagnostics

The application no longer exposes built-in metrics or diagnostic REST endpoints. For production metrics and diagnostics, follow the recommendations in `docs/PRODUCTION_MONITORING.md`: collect broker JMX metrics via a JMX exporter and scrape with Prometheus (or use a vendor-managed solution), compute derived rates in the metrics pipeline, and dashboard/alert from the central metrics store.
