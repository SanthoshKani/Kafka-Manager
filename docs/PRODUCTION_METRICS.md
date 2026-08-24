# Production Metrics Guidance

The repository no longer ships an internal metrics subsystem (REST endpoints or collectors). For production observability we recommend collecting broker JMX metrics with a JMX exporter and scraping them centrally with Prometheus, or using a vendor-managed metrics solution.

See `docs/PRODUCTION_MONITORING.md` for detailed guidance, recommended dashboards, and alert rules.

