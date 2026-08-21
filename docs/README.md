# Kafka Manager - Documentation

Welcome to the Kafka Manager documentation. This folder contains guides to help developers understand, navigate, and contribute to the codebase.

## Documentation Index

| Document | Description |
|----------|-------------|
| [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) | Comprehensive guide to code structure, flow, patterns, and development workflows |
| [ARCHITECTURE.nomnoml](ARCHITECTURE.nomnoml) | Visual architecture diagram (render with Nomnoml) |
| [QUICK_REFERENCE.md](QUICK_REFERENCE.md) | Quick reference for commands, API endpoints, configuration, and common tasks |

## Viewing the Architecture Diagram

The `ARCHITECTURE.nomnoml` file uses [Nomnoml](https://nomnoml.com/) syntax. You can view it in several ways:

### Online Renderer
1. Go to https://nomnoml.com/
2. Copy the contents of `ARCHITECTURE.nomnoml`
3. Paste into the editor

### VS Code Extension
1. Install "Nomnoml" extension
2. Open `ARCHITECTURE.nomnoml`
3. Press `Ctrl+Shift+V` (or `Cmd+Shift+V` on Mac) to preview

### Command Line
```bash
# Install nomnoml CLI
npm install -g nomnoml

# Render to SVG
nomnoml docs/ARCHITECTURE.nomnoml docs/architecture.svg

# Render to PNG
nomnoml docs/ARCHITECTURE.nomnoml docs/architecture.png
```

## Getting Started

1. **New to the project?** Start with [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) - it covers the overall architecture, code flow, and how to add new features.

2. **Need to run a command quickly?** Check [QUICK_REFERENCE.md](QUICK_REFERENCE.md) for common commands, API endpoints, and configuration.

3. **Want to understand the system visually?** View the [ARCHITECTURE.nomnoml](ARCHITECTURE.nomnoml) diagram.

## Project Structure Overview

```
Kafka-Manager/
├── docs/                    # This folder
├── src/
│   ├── main/
│   │   ├── java/.../kafkamanager/
│   │   │   ├── config/           # Spring configuration
│   │   │   ├── common/           # Shared utilities
│   │   │   ├── kafkaadmin/       # AdminClient abstraction
│   │   │   ├── topics/           # Topic management
│   │   │   ├── brokers/          # Broker management
│   │   │   ├── consumergroups/   # Consumer group management
│   │   │   ├── acls/             # ACL management
│   │   │   ├── scram/            # SCRAM credentials
│   │   │   ├── delegationtokens/ # Delegation tokens
│   │   │   ├── metadataquorum/   # KRaft quorum
│   │   │   ├── clientmetrics/    # Client metrics
│   │   │   └── operations/       # Long-running operations
│   │   └── resources/
│   │       ├── application.yaml
│   │       ├── application-local.yml
│   │       └── db/migration/     # Flyway migrations
│   ├── test/                     # Unit tests
├── compose.yaml                  # Docker Compose for local dev
├── build.gradle                  # Gradle build
└── README.md                     # Project README
```

## Key Concepts

- **Single Kafka Cluster**: The app connects to one Kafka cluster configured through `BOOTSTRAP_SERVERS_CONFIG`
- **Singleton AdminClient**: One process-wide Kafka Admin client is reused for all Kafka operations
- **Profile-Based Security**: Local profile is open for development, while production uses Basic Auth, OAuth2 Resource Server, and Kafka SSL/mTLS
- **Circuit Breaker**: Resilience4j protection for Kafka admin operations
- **Rate Limiting**: Bucket4j token bucket algorithm
- **Correlation IDs**: Request tracing via MDC and headers

## Contributing

1. Read the [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) for development workflows
2. Follow the existing code patterns and conventions
3. Add tests for new functionality
4. Update documentation when adding features
5. Run `./gradlew spotlessApply` before committing

## Support

- Check the main [README.md](../README.md) for project overview
- Review [QUICK_REFERENCE.md](QUICK_REFERENCE.md) for troubleshooting
- Open an issue for bugs or feature requests
