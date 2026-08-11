# Kafka Manager - AI Agent Guide

This guide helps AI coding agents be immediately productive in the Kafka Manager codebase.

## Project Overview

**Kafka Manager** is a KRaft-native Kafka cluster manager built with Spring Boot 4.1.0, Spring MVC, Spring Security, and the Kafka AdminClient. It provides a REST API for cluster administration, topic management, broker operations, consumer group management, ACLs, SCRAM credentials, delegation tokens, and metadata quorum operations.

**Stack**: Java 25, Spring Boot 4.1.0, Spring Web MVC, Kafka AdminClient, OAuth2 Resource Server, Basic Auth, Micrometer + Prometheus, springdoc-openapi, Docker Compose, Bucket4j (rate limiting), Resilience4j (circuit breaker).

## Architecture & Key Components

### Domain Structure (src/main/java/.../kafkamanager/)

```
config/                    # Spring configuration
  AsyncConfiguration.java  # ThreadPoolTaskExecutor with metrics
  CorrelationIdFilter.java # Request correlation ID (X-Correlation-Id) via MDC
  JacksonConfig.java       # ObjectMapper config
  KafkaManagerProperties.java # @ConfigurationProperties for app.*
  OpenApiConfig.java       # Swagger/OpenAPI config
  RateLimitingConfig.java  # Bucket4j token bucket filter
  SecurityConfig.java      # Spring Security filter chain (Basic Auth / OAuth2 RS)

common/                    # Shared utilities & exceptions
  ApiProblemAdvice.java    # RFC 9457 Problem Details handler
  ProblemResponseWriter.java
  ApiException.java        # Base exception
  KafkaAdminException.java # Kafka-specific exceptions

clusterregistry/           # Cluster registration & management
  api/                     # REST controllers & DTOs
  domain/                  # JPA entities (ClusterEntity, SecretEntity) & repositories
  service/                 # Business logic (ClusterRegistryService, SecretCipherService, SecretStoreService)

kafkaadmin/                # Kafka AdminClient abstraction
  AdminClientRegistry.java # Caffeine cache + AdminClient builder (fingerprint invalidation)
  KafkaAdminExecutionService.java # Resilience4j circuit breaker wrapper
  KafkaClientPropertyPolicyService.java # Property validation

topics/                    # Topic management (CRUD, configs, partitions, offsets)
brokers/                   # Broker management
consumergroups/            # Consumer group management
acls/                      # ACL management
scram/                     # SCRAM credential management
delegationtokens/          # Delegation token management
metadataquorum/            # KRaft metadata quorum
clientmetrics/             # Client metrics
operations/                # Long-running async operations tracking
```

### Key Design Patterns

1. **AdminClient Caching with Fingerprint Invalidation** - `AdminClientRegistry` uses Caffeine cache keyed by clusterId + fingerprint hash of connection properties. Config changes auto-invalidate cache.

2. **Secret Encryption** - All sensitive data (passwords, keystore passwords) encrypted with AES-256-GCM via `SecretCipherService`. Master key from `app.security.master-key-base64`.

3. **Circuit Breaker** - `KafkaAdminExecutionService` wraps AdminClient calls with Resilience4j (sliding window: 10, failure threshold: 50%, wait: 30s).

4. **RFC 9457 Problem Details** - All errors returned as `application/problem+json` via `ApiProblemAdvice`.

5. **Correlation ID Tracking** - `CorrelationIdFilter` extracts/generates `X-Correlation-Id`, adds to MDC for logging, includes in response headers.

6. **Async Operations** - Mutating operations persisted with events, support retry/cancel, polling-based status tracking.

## Build & Test Commands

```bash
# Full build (compile, test, integrationTest)
./gradlew build

# Unit tests only (H2 in-memory DB)
./gradlew test

# Integration tests only (requires Docker Compose - PostgreSQL + Kafka KRaft cluster)
./gradlew integrationTest

# Clean build
./gradlew clean build

# Code formatting (Google Java Format via Spotless)
./gradlew spotlessCheck   # Check
./gradlew spotlessApply   # Apply


# Generate OpenAPI spec
./gradlew openapiGenerate
```

## Run Locally

```bash
# Start infrastructure (3-controller KRaft quorum + 3 brokers)
docker compose up -d

# Run application (local profile)
./gradlew bootRun --args='--spring.profiles.active=local'

# App: http://localhost:8080
# Actuator: http://localhost:8080/management
# OpenAPI: http://localhost:8080/api-docs (or openapi.yaml in repo root)
```

## Profiles

-- `default`: Application runtime (in-memory cluster registry; Kafka brokers required for live validation)
-- `local`: Development profile (uses `application-local.yml`)
-- `it`: Compose-backed integration profile for live Kafka validation
-- `test`: Unit tests

## Key Configuration (application.yaml)

```yaml
app:
  service-name: kafka-manager
  rate-limit:
    enabled: true
    capacity: 300
    refill-period: 1m
    key-header: X-Client-Id
  security:
    master-key-base64: ${KAFKA_MANAGER_MASTER_KEY_BASE64}  # REQUIRED: 32-byte base64 key
    basic-auth:
      username: ${KAFKA_MANAGER_BASIC_AUTH_USERNAME:admin}
      password: ${KAFKA_MANAGER_BASIC_AUTH_PASSWORD:admin}
    oauth2-resource-server:
      issuer-uri: ${KAFKA_MANAGER_OAUTH2_ISSUER_URI:}
      jwk-set-uri: ${KAFKA_MANAGER_OAUTH2_JWK_SET_URI:}
  admin:
    cache-size: 64
    default-request-timeout: 5s
    default-operation-timeout: 30s
    connection-validation-timeout: 5s
  operations:
    poll-interval: 15s
    lease-duration: 60s
  cluster-registry:
    max-page-size: 100
    max-client-properties: 32
```

**Required env vars**: 
- `KAFKA_MANAGER_MASTER_KEY_BASE64` (generate: `openssl rand -base64 32`)
- `KAFKA_MANAGER_BASIC_AUTH_USERNAME` / `KAFKA_MANAGER_BASIC_AUTH_PASSWORD` (for Basic Auth)
- `KAFKA_MANAGER_OAUTH2_ISSUER_URI` or `KAFKA_MANAGER_OAUTH2_JWK_SET_URI` (for OAuth2 Resource Server)

## API Endpoints (Base: `/api/v1`)

### Clusters
- `GET /clusters` - List clusters
- `POST /clusters` - Register cluster
- `GET /clusters/{id}` - Get cluster detail
- `PATCH /clusters/{id}` - Update cluster
- `DELETE /clusters/{id}` - Delete cluster
- `POST /clusters/{id}/validate` - Validate connectivity
- `GET /clusters/{id}/capability` - Capability report

### Topics
- `GET /clusters/{clusterId}/topics` - List topics
- `POST /clusters/{clusterId}/topics` - Create topic
- `GET /clusters/{clusterId}/topics/{topicName}` - Describe topic
- `DELETE /clusters/{clusterId}/topics/{topicName}` - Delete topic
- `POST /clusters/{clusterId}/topics/{topicName}/partitions` - Increase partitions
- `GET /clusters/{clusterId}/topics/{topicName}/configs` - Describe configs
- `PATCH /clusters/{clusterId}/topics/{topicName}/configs` - Alter configs (incremental)
- `GET /clusters/{clusterId}/topics/{topicName}/offsets` - List offsets
- `POST /clusters/{clusterId}/topics/{topicName}/records/delete` - Delete records

### Brokers
- `GET /clusters/{clusterId}/brokers` - List brokers
- `GET /clusters/{clusterId}/brokers/{brokerId}` - Describe broker
- `GET /clusters/{clusterId}/brokers/{brokerId}/configs` - Describe broker configs
- `PATCH /clusters/{clusterId}/brokers/{brokerId}/configs` - Alter broker configs

### Consumer Groups
- `GET /clusters/{clusterId}/consumer-groups` - List groups
- `GET /clusters/{clusterId}/consumer-groups/{groupId}` - Describe group
- `DELETE /clusters/{clusterId}/consumer-groups/{groupId}` - Delete group
- `POST /clusters/{clusterId}/consumer-groups/{groupId}/offsets` - Alter offsets
- `POST /clusters/{clusterId}/consumer-groups/{groupId}/members/remove` - Remove members

### Cluster Actions (Async)
- `POST /clusters/{clusterId}/leader-election/preferred` - Preferred leader election
- `POST /clusters/{clusterId}/leader-election/unclean` - Unclean leader election
- `POST /clusters/{clusterId}/partition-reassignment` - Partition reassignment
- `GET /clusters/{clusterId}/log-dirs` - Describe log dirs
- `PATCH /clusters/{clusterId}/log-dirs` - Alter log dirs (replica moves)

### Metadata Quorum (KRaft)
- `GET /clusters/{clusterId}/metadata-quorum` - Quorum status

### Client Metrics
- `GET /clusters/{clusterId}/client-metrics` - List client metrics resources

### ACLs
- `GET /clusters/{clusterId}/acls` - List ACLs (with filters)
- `POST /clusters/{clusterId}/acls` - Create ACLs
- `DELETE /clusters/{clusterId}/acls` - Delete ACLs

### SCRAM Credentials
- `GET /clusters/{clusterId}/scram` - List SCRAM users
- `POST /clusters/{clusterId}/scram` - Upsert SCRAM credential
- `DELETE /clusters/{clusterId}/scram` - Delete SCRAM credential

### Delegation Tokens
- `GET /clusters/{clusterId}/delegation-tokens` - List tokens
- `POST /clusters/{clusterId}/delegation-tokens` - Create token
- `POST /clusters/{clusterId}/delegation-tokens/renew` - Renew token
- `POST /clusters/{clusterId}/delegation-tokens/expire` - Expire token

### Operations (Async)
- `POST /operations` - Submit operation
- `GET /operations` - List operations (with filters)
- `GET /operations/{id}` - Get operation detail
- `GET /operations/{id}/events` - Get operation events (polling)
- `POST /operations/{id}/retry` - Retry failed operation
- `POST /operations/{id}/cancel` - Cancel pending operation

## Security

- **Basic Authentication**: HTTP Basic Auth for username/password authentication (configured via `app.security.basic-auth.username` and `app.security.basic-auth.password`)
- **OAuth2 Resource Server**: Validates JWT Bearer tokens via issuer-uri or JWK set URI (configured via `app.security.oauth2-resource-server.issuer-uri` or `app.security.oauth2-resource-server.jwk-set-uri`)
- **Cluster secrets encrypted**: Bootstrap servers, SASL credentials, TLS config encrypted at rest with AES-GCM
- **Actuator endpoints**: Secured, read-only access for health/info/prometheus
- **CORS**: Disabled by default; configure via `SecurityConfig` if needed
- **Rate limiting**: Bucket4j token bucket (300 req/min per `X-Client-Id` header, falls back to remote address)

## Adding New Features

### New REST Endpoint
1. Create request/response DTOs in appropriate `api/` package (use `record` types)
2. Create controller in `api/` package (`@RestController`, `@RequestMapping("/api/v1/...")`)
3. Create service in `service/` package (`@Service`, `@RequiredArgsConstructor`)
4. Use `AdminClientRegistry` for Kafka operations
5. Add OpenAPI annotations (`@Operation`, `@ApiResponse`, `@Parameter`)

### New Cluster Configuration Field
1. Add field to `ClusterEntity` (domain)
2. Add to `RegisterClusterRequest` / `UpdateClusterRequest` (api)
3. Add to `ClusterDetailResponse` (api)
4. Update `ClusterRegistryService.apply()` methods
5. Update `AdminClientRegistry.buildHandle()` to configure the property
6. Add to `KafkaClientPropertyPolicyService.ALLOWED_KEYS` if it's a client property
7. If you add persistent storage you will need to create appropriate migrations; the project currently uses in-memory stores by default.

## Testing Patterns

### Unit Tests (src/test/)
- Use `@ExtendWith(MockitoExtension.class)`
- Mock services with `@Mock`, inject into controller via `MockMvcBuilders.standaloneSetup()`
- Test profile: `test` (H2 in-memory, Flyway disabled)
- Run: `./gradlew test`

### Integration Tests (src/integrationTest/)
- `@SpringBootTest` + `@ActiveProfiles("it")`
- Uses Testcontainers for PostgreSQL + Kafka
- Requires Docker Compose running
- Run: `./gradlew integrationTest`

### Test Utilities
- `KafkaManagerComposeIntegrationIT` - Full compose-backed integration suite (uses Basic Auth)
- `SecurityIntegrationTest` - Verifies protected endpoints require authentication (Basic Auth)
- Basic Auth header generation in integration tests

## Common Tasks

### Debugging
```bash
# Enable debug logging
logging:
  level:
    com.opentext.security.analytics.messagehub.kafkamanager: DEBUG
    org.springframework.security: DEBUG
    org.apache.kafka: DEBUG

# Health check
curl http://localhost:8080/management/health

# Metrics
curl http://localhost:8080/management/metrics

# List clusters (requires X-Client-Id header for rate limiting)
curl -H "X-Client-Id: test" http://localhost:8080/api/v1/clusters
```

### Docker
```bash
# Start infrastructure
docker compose up -d

# Stop infrastructure
docker compose down

# View logs
docker compose logs -f kafka-manager

# Rebuild image
docker compose build --no-cache
```

## Key Files for Reference

| File | Purpose |
|------|---------|
| `README.md` | Project overview, stack, API endpoints, run instructions |
| `docs/DEVELOPER_GUIDE.md` | Comprehensive code structure, flow, patterns, workflows |
| `docs/QUICK_REFERENCE.md` | Commands, API endpoints, config, debugging, common issues |
| `docs/ARCHITECTURE.nomnoml` | Visual architecture diagram (render with Nomnoml) |
| `build.gradle` | Dependencies, plugins, test tasks, Spotless, PMD, JaCoCo |
| `compose.yaml` | Docker Compose: 3-controller KRaft quorum + 3 brokers |
| `application.yaml` | Main configuration (all profiles) |
| `application-local.yml` | Local development overrides |
| `openapi.yaml` | Exported OpenAPI 3.0 contract |

## Gotchas & Conventions

- **Java 25** - Uses preview features, toolchain configured in build.gradle
- **Spring Boot 4.1.0** - Servlet-based (Spring MVC), not WebFlux
- **Records for DTOs** - All request/response DTOs use `record` types
- **Constructor injection** - Use `@RequiredArgsConstructor` on services/controllers
- **No wildcard imports** - Enforced by Spotless
- **RFC 9457 errors** - All exceptions map to `application/problem+json`
- **Correlation IDs** - Every request gets `X-Correlation-Id` (MDC + response header)
- **AdminClient caching** - Never create AdminClient directly; always use `AdminClientRegistry.getAdminClient(clusterId)`
- **Secret handling** - Never log decrypted secrets; `SecretCipherService` handles encryption/decryption
- **Circuit breaker** - Kafka admin calls wrapped in `KafkaAdminExecutionService.execute()`
- **Async operations** - Mutating operations return `OperationResponse` with ID; poll `/operations/{id}/events` for status
-- **Data persistence** - In-memory stores are used by default; no Flyway or DB required for development/testing
- **Rate limit key** - Default `X-Client-Id` header; falls back to remote address
- **OAuth2 Resource Server** - Validates JWT Bearer tokens via JWK set URI or issuer URI
