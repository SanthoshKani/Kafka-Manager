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
- **Flyway** for database migrations
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
├── clusterregistry/                      # Cluster registration & management
│   ├── api/                              # REST controllers & DTOs
│   │   ├── ClusterController.java        # CRUD for clusters
│   │   ├── ClusterAdminController.java   # Admin operations (reassign, elect)
│   │   ├── RegisterClusterRequest.java
│   │   ├── UpdateClusterRequest.java
│   │   ├── ClusterDetailResponse.java
│   │   ├── ClusterSummaryResponse.java
│   │   └── ... (other DTOs)
│   ├── domain/                           # JPA entities & repositories
│   │   ├── ClusterEntity.java            # Cluster configuration (with SSL/SASL)
│   │   ├── ClusterRepository.java
│   │   ├── SecretEntity.java             # Encrypted secrets storage
│   │   └── SecretRepository.java
│   └── service/                          # Business logic
│       ├── ClusterRegistryService.java   # Cluster CRUD & validation
│       ├── ClusterAdminService.java      # Admin operations (reassign, elect)
│       ├── SecretCipherService.java      # AES-256-GCM encryption
│       └── SecretStoreService.java       # Secret storage & retrieval
├── kafkaadmin/                           # Kafka AdminClient abstraction
│   ├── AdminClientRegistry.java          # Cached AdminClient per cluster
│   ├── KafkaAdminExecutionService.java   # Circuit breaker wrapper
│   └── KafkaClientPropertyPolicyService.java # Property validation
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

### 1. Cluster Registration Flow

```
POST /api/v1/clusters
    → ClusterController.registerCluster()
    → ClusterRegistryService.register()
    → ClusterEntity (JPA) persisted
    → SecretEntity (encrypted) persisted via SecretStoreService
    → SecretCipherService.encrypt() using AES-256-GCM
    → Returns ClusterDetailResponse
```

**Key Classes:**
- `ClusterController` - REST endpoint
- `ClusterRegistryService` - Business logic, validation
- `ClusterEntity` - JPA entity with all SSL/SASL fields
- `SecretCipherService` - Encryption/decryption
- `SecretStoreService` - Secret persistence

### 2. AdminClient Creation & Caching Flow

```
Any Kafka Admin Operation
    → AdminClientRegistry.getAdminClient(clusterId)
    → Check Caffeine cache (key = clusterId)
    → If miss: build AdminClient via buildHandle()
        → ClusterRegistryService.getClusterDetail()
        → SecretStoreService.getDecryptedSecrets()
        → Configure all SSL/SASL properties
        → Cache with fingerprint (invalidates on config change)
    → Return AdminClient
```

**Key Classes:**
- `AdminClientRegistry` - Cache + AdminClient builder
- `KafkaClientPropertyPolicyService` - Validates allowed properties
- `ClusterEntity` - Holds all connection config

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
    masterKeyBase64: "<base64-encoded-32-byte-key>"  # Required for secret encryption
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
```

### Data Persistence

This project uses in-memory repositories for development and testing. The in-memory stores are implemented in the `clusterregistry.service` and `operations.service` packages and provide thread-safe ConcurrentHashMap-backed storage. Data is not persisted across restarts. If you need durability, swap in a persistent implementation behind the `ClusterStore`, `SecretStore` and `OperationStore` abstractions.

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
- PostgreSQL (via docker-compose)
- Kafka cluster (via docker-compose)
Located in `src/integrationTest/java/...`

### Test Profiles
- `test` - Unit tests with H2 in-memory DB
- `it` - Integration tests with Testcontainers

## Key Design Patterns

### 1. AdminClient Caching with Fingerprint Invalidation
`AdminClientRegistry` uses Caffeine cache keyed by clusterId. The cache key includes a fingerprint hash of all connection properties. When cluster config changes, the fingerprint changes, automatically invalidating the cache.

### 2. Secret Encryption
All sensitive data (passwords, keystore passwords) encrypted with AES-256-GCM via `SecretCipherService`. Master key configured via `app.security.masterKeyBase64`.

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
7. Run Flyway migration if schema changes

## Common Tasks

### Run Locally
```bash
# Start infrastructure
docker-compose up -d

# Run application
./gradlew bootRun --args='--spring.profiles.active=local'
```

### Build
```bash
./gradlew build
```

### Generate OpenAPI Spec
```bash
./gradlew openapiGenerate
```

### Database Migration
```bash
./gradlew flywayMigrate
```

## Troubleshooting

### AdminClient Connection Issues
- Check cluster configuration in DB
- Verify secrets are correctly encrypted/decrypted
- Check Kafka broker listener configuration matches cluster config
- Review `AdminClientRegistry.buildHandle()` for property mapping

### Rate Limiting
- Check `app.rateLimit.enabled`
- Verify `keyHeader` matches client header
- Monitor Bucket4j metrics

### Circuit Breaker Open
- Check Kafka broker availability
- Review `KafkaAdminExecutionService` configuration
- Check Resilience4j metrics

## Useful Commands

```bash
# View API docs
open http://localhost:8080/swagger-ui.html

# Health check
curl http://localhost:8080/actuator/health

# Metrics
curl http://localhost:8080/actuator/metrics

# List clusters
curl -H "X-API-Key: test" http://localhost:8080/api/v1/clusters
```
