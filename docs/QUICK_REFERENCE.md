# Kafka Manager - Quick Reference

## Common Commands

### Build & Test
```bash
# Full build (compile, test, integrationTest)
./gradlew build

# Unit tests only
./gradlew test

# Integration tests only (requires docker-compose)
./gradlew integrationTest

# Compile only
./gradlew compileJava

# Clean build
./gradlew clean build
```

### Run Application
```bash
# Local profile (uses application-local.yml)
./gradlew bootRun --args='--spring.profiles.active=local'

# Production/default profile
./gradlew bootRun
```


### Docker
```bash
# Start infrastructure (Kafka controllers + brokers)
docker-compose up -d

# Stop infrastructure
docker-compose down

# View logs
docker-compose logs -f kafka-manager

# Rebuild image
docker-compose build --no-cache
```

## Broker monitoring

Runtime and broker metrics scraping are intentionally removed from the built-in Cluster Manager for production usage. See `docs/PRODUCTION_MONITORING.md` for recommended production monitoring patterns (Prometheus JMX exporter, Jolokia), sample exporter config, and sample `prometheus.yml` scrape jobs.

## API Endpoints Quick Reference

## Adding a New Domain Feature

This project uses in-memory stores by default. When adding new domain features, follow these steps:

1. Create request/response DTOs in the appropriate `api/` package (use `record` types).
2. Create controller in the `api/` package (`@RestController`, `@RequestMapping("/api/v1/...")`).
3. Create service in the `service/` package (`@Service`, `@RequiredArgsConstructor`).
4. Use the AdminClient bean from `KafkaAdminClientConfiguration` (and `KafkaAdminExecutionService` wrapper) for Kafka operations.
5. If you add persistence later, implement repository/store abstractions and document migration steps in the project docs; by default, prefer in-memory stores for development.
|--------|----------|-------------|
| See `docs/PRODUCTION_MONITORING.md` for production monitoring recommendations and sample scrape configs |

### Operations
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/operations` | List operations |
| GET | `/api/v1/operations/{id}` | Get operation status |

## Key Configuration Properties

### Required
```yaml
app:
  security:
    basicAuth:
      username: "admin"
      password: "admin"
    oauth2ResourceServer:
      issuerUri: "https://auth.example.com"
      jwkSetUri: "https://auth.example.com/.well-known/jwks.json"
  admin:
    bootstrapServers: "localhost:19092,localhost:29092,localhost:39092"
    securityProtocol: "PLAINTEXT"
```

### Optional (with defaults)
```yaml
app:
  serviceName: "kafka-manager"
  security:
    basicAuth:
      username: "admin"
      password: "admin"
    oauth2ResourceServer:
      issuerUri: ""
      jwkSetUri: ""
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

## Cluster Registration - Required Fields

### Minimal (PLAINTEXT)
```json
{
  "name": "local-cluster",
  "bootstrapServers": "localhost:9092"
}
```

### With TLS/mTLS
```json
{
  "name": "secure-cluster",
  "bootstrapServers": "kafka1:9093,kafka2:9093,kafka3:9093",
  "securityProtocol": "SSL",
  "ssl": {
    "trustStore": "/path/to/truststore.bcfks",
    "trustStorePassword": "secret",
    "keyStore": "/path/to/keystore.bcfks",
    "keyStorePassword": "secret",
    "keyPassword": "secret",
    "trustStoreType": "BCFKS",
    "keyStoreType": "BCFKS",
    "endpointIdentificationAlgorithm": "https",
    "enabledProtocols": "TLSv1.2,TLSv1.3"
  }
}
```

## Adding a New Domain Feature

### 1. Create Domain Entity (if persistent)
```java
// src/main/java/.../newdomain/domain/NewEntity.java
@Entity
@Table(name = "new_entity")
public class NewEntity {
    @Id @GeneratedValue
    private Long id;
    // fields...
}
```

### 2. Create Repository
```java
// src/main/java/.../newdomain/domain/NewRepository.java
public interface NewRepository extends JpaRepository<NewEntity, Long> {
    // custom queries...
}
```

### 3. Create Service
```java
// src/main/java/.../newdomain/service/NewService.java
@Service
@RequiredArgsConstructor
public class NewService {
    private final NewRepository repository;
    private final AdminClientRegistry adminClientRegistry;
    
    // business logic...
}
```

### 4. Create API DTOs
```java
// src/main/java/.../newdomain/api/NewRequest.java
public record NewRequest(String field1, String field2) {}

// src/main/java/.../newdomain/api/NewResponse.java
public record NewResponse(Long id, String field1, String field2) {}
```

### 5. Create Controller
```java
// src/main/java/.../newdomain/api/NewController.java
@RestController
@RequestMapping("/api/v1/clusters/{clusterId}/newdomain")
@RequiredArgsConstructor
public class NewController {
    private final NewService service;
    
    @GetMapping
    public List<NewResponse> list(@PathVariable Long clusterId) {
        return service.list(clusterId);
    }
    
    @PostMapping
    public NewResponse create(@PathVariable Long clusterId, @Valid @RequestBody NewRequest request) {
        return service.create(clusterId, request);
    }
}
```

### 6. Persistence (not required)

This project uses in-memory stores by default; persistent schema migrations are not required for normal development. If you add persistent storage later, document schema and migration steps at that time.

## Debugging Tips

### Enable Debug Logging
```yaml
logging:
  level:
    com.opentext.security.analytics.messagehub.kafkamanager: DEBUG
    org.springframework.security: DEBUG
    org.apache.kafka: DEBUG
```

### Check AdminClient Cache
```bash
# Via actuator (if exposed)
curl http://localhost:8080/actuator/caches
```

### View Correlation IDs in Logs
```bash
# Logs include X-Correlation-Id in MDC
grep "X-Correlation-Id" application.log
```

### Test Cluster Connection
```bash
curl -X GET "http://localhost:8080/api/v1/clusters/1/validate" \
  -H "X-API-Key: test-key"
```

## Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| AdminClient connection timeout | Check `bootstrapServers`, network, firewall |
| SSL handshake failure | Verify truststore/keystore paths, passwords |
| Kafka auth failure | Check security protocol and TLS/keystore/truststore configuration |
| Rate limit exceeded | Increase `capacity` or `refillPeriod` |
| Circuit breaker open | Check Kafka broker health, increase timeout |
| Flyway migration failed | Not applicable - project does not use Flyway by default |

## Useful Actuator Endpoints

| Endpoint | Description |
|----------|-------------|
| `/actuator/health` | Health status |
| `/actuator/info` | Application info |
| `/actuator/metrics` | Metrics list |
| `/actuator/metrics/jvm.memory.used` | JVM memory |
| `/actuator/caches` | Cache statistics |
| `/actuator/loggers` | Logger levels |
| `/actuator/env` | Environment properties |

## IDE Setup

### IntelliJ IDEA
1. Import as Gradle project
2. Enable annotation processing
3. Set JDK 21
4. Run `KafkaManagerApplication` with `--spring.profiles.active=local`

### VS Code
1. Install Extension Pack for Java
2. Install Spring Boot Extension Pack
3. Open folder, select JDK 21
4. Run via `./gradlew bootRun`

## Code Style

- **Formatter**: Google Java Format (via Spotless)
- **Check**: `./gradlew spotlessCheck`
- **Apply**: `./gradlew spotlessApply`

## Dependencies

Key dependencies in `build.gradle`:
- `spring-boot-starter-web`
- (no JPA - in-memory stores used)
- `spring-boot-starter-security`
- `spring-boot-starter-validation`
- `spring-boot-starter-actuator`
- `caffeine` (caching)
- `bucket4j` (rate limiting)
- `resilience4j` (circuit breaker)
- `springdoc-openapi-starter-webmvc-ui` (Swagger)
- `jackson-databind` (JSON)
- `kafka-clients` (AdminClient)
