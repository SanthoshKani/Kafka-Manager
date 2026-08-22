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
# Start infrastructure (PostgreSQL + Kafka)
docker-compose up -d

# Stop infrastructure
docker-compose down

# View logs
docker-compose logs -f kafka-manager

# Rebuild image
docker-compose build --no-cache
```

## API Endpoints Quick Reference

### Clusters
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters` | List all clusters |
| POST | `/api/v1/clusters` | Register new cluster |
| GET | `/api/v1/clusters/{id}` | Get cluster details |
| PUT | `/api/v1/clusters/{id}` | Update cluster |
| DELETE | `/api/v1/clusters/{id}` | Delete cluster |
| GET | `/api/v1/clusters/{id}/validate` | Validate cluster connection |

### Cluster Admin
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/clusters/{id}/reassign-partitions` | Reassign partitions |
| POST | `/api/v1/clusters/{id}/elect-leaders` | Elect preferred leaders |
| GET | `/api/v1/clusters/{id}/log-dirs` | Describe log dirs |

### Topics
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{id}/topics` | List topics |
| POST | `/api/v1/clusters/{id}/topics` | Create topic |
| GET | `/api/v1/clusters/{id}/topics/{name}` | Get topic details |
| DELETE | `/api/v1/clusters/{id}/topics/{name}` | Delete topic |
| POST | `/api/v1/clusters/{id}/topics/{name}/partitions` | Expand partitions |
| POST | `/api/v1/clusters/{id}/topics/config-mutations` | Mutate configs |
| GET | `/api/v1/clusters/{id}/topics/{name}/offsets` | Get offsets |

### Brokers
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{id}/brokers` | List brokers |
| GET | `/api/v1/clusters/{id}/brokers/{brokerId}` | Get broker details |
| POST | `/api/v1/clusters/{id}/brokers/config-mutations` | Mutate broker configs |

### Consumer Groups
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{id}/consumer-groups` | List consumer groups |
| GET | `/api/v1/clusters/{id}/consumer-groups/{groupId}` | Get group details |
| DELETE | `/api/v1/clusters/{id}/consumer-groups/{groupId}` | Delete group |
| POST | `/api/v1/clusters/{id}/consumer-groups/{groupId}/offsets` | Update offsets |
| POST | `/api/v1/clusters/{id}/consumer-groups/{groupId}/members/remove` | Remove member |

### ACLs
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{id}/acls` | List ACLs |
| POST | `/api/v1/clusters/{id}/acls` | Create ACL |
| DELETE | `/api/v1/clusters/{id}/acls` | Delete ACL |

### SCRAM
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{id}/scram` | List SCRAM credentials |
| POST | `/api/v1/clusters/{id}/scram` | Create SCRAM credential |
| DELETE | `/api/v1/clusters/{id}/scram/{username}` | Delete SCRAM credential |

### Delegation Tokens
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{id}/delegation-tokens` | List tokens |
| POST | `/api/v1/clusters/{id}/delegation-tokens` | Create token |
| POST | `/api/v1/clusters/{id}/delegation-tokens/renew` | Renew token |
| POST | `/api/v1/clusters/{id}/delegation-tokens/expire` | Expire token |

### Metadata Quorum (KRaft)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{id}/metadata-quorum` | Get quorum status |
| GET | `/api/v1/clusters/{id}/metadata-quorum/voters` | List voters |

### Client Metrics
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{id}/client-metrics` | Get client metrics |

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

### 6. Add Flyway Migration (if schema changes)
```sql
-- src/main/resources/db/migration/V2__new_domain.sql
CREATE TABLE new_entity (
    id BIGSERIAL PRIMARY KEY,
    field1 VARCHAR(255) NOT NULL,
    field2 VARCHAR(255)
);
```

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
| Flyway migration failed | Check SQL syntax, run `flywayClean` first |

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
- (no Postgres/Flyway required)
- `caffeine` (caching)
- `bucket4j` (rate limiting)
- `resilience4j` (circuit breaker)
- `springdoc-openapi-starter-webmvc-ui` (Swagger)
- `jackson-databind` (JSON)
- `kafka-clients` (AdminClient)
