# Controllers

This document lists Spring MVC controllers discovered in the repository, their base paths, endpoints, request/response DTOs, and security/mapping annotations. No project-level CORS or rate-limiting annotations were found in controller classes.

Notes:
- All controllers declare @SecurityRequirement(name = "bearerAuth") (OpenAPI annotation) indicating bearer-token (OAuth2/JWT) style protection in the API docs. The codebase does not use explicit Basic auth annotations on controllers.
- No controller-level CORS or rate-limiting annotations (@CrossOrigin, @RateLimiter) were found.

## Summary list of controllers

- BrokerController (/api/v1/clusters/{clusterId}/brokers)
- ConsumerGroupController (/api/v1/clusters/{clusterId}/consumer-groups)
- DelegationTokenController (/api/v1/clusters/{clusterId}/delegation-tokens)
- AclController (/api/v1/clusters/{clusterId}/acls)
- ClientMetricsController (/api/v1/clusters/{clusterId}/client-metrics)
- TopicController (/api/v1/clusters/{clusterId}/topics)
- ClusterAdminController (/api/v1/clusters/{clusterId}/actions)
- OperationController (/api/v1/clusters/{clusterId}/operations)
- ScramController (/api/v1/clusters/{clusterId}/scram/users)
- BrokerJmxMetricsController (/api/v1/metrics/broker-jmx)
- BrokerMetricsDiagnosticsController (/api/v1/clusters/{clusterId}/brokers/{brokerId}/metrics/diagnostics)
- ClusterStructuralMetricsController (/api/v1/clusters/{clusterId}/metrics/structural)
- RuntimeMetricsController (base: /api/v1)
- MetadataQuorumController (/api/v1/clusters/{clusterId}/metadata-quorum)
- StructuralMetricsController (/api/v1/metrics/structural)

---

## Controller details

### BrokerController
- Base path: /api/v1/clusters/{clusterId}/brokers
- Annotations: @RestController, @RequestMapping, @Tag("Brokers"), @SecurityRequirement(name = "bearerAuth")
- Endpoints:
  - GET /api/v1/clusters/{clusterId}/brokers — List brokers
    - Response DTO: BrokerSummaryResponse (List)
  - GET /api/v1/clusters/{clusterId}/brokers/{brokerId}/configs — Broker configs
    - Response DTO: Map<String, String>
  - PATCH /api/v1/clusters/{clusterId}/brokers/{brokerId}/configs — Alter broker configs
    - Request DTO: BrokerConfigMutationRequest
    - Response: 204 No Content

### ConsumerGroupController
- Base path: /api/v1/clusters/{clusterId}/consumer-groups
- Annotations: @RestController, @RequestMapping, @Tag("Consumer Groups"), @SecurityRequirement(name = "bearerAuth")
- Endpoints:
  - GET /api/v1/clusters/{clusterId}/consumer-groups — List consumer groups
    - Response DTO: ConsumerGroupSummaryResponse (List)
  - GET /api/v1/clusters/{clusterId}/consumer-groups/{groupId} — Describe consumer group
    - Response DTO: ConsumerGroupDetailResponse
  - DELETE /api/v1/clusters/{clusterId}/consumer-groups/{groupId} — Delete consumer group
    - Response: 204 No Content
  - POST /api/v1/clusters/{clusterId}/consumer-groups/{groupId}/offsets — Alter consumer group offsets
    - Request DTO: ConsumerGroupOffsetUpdateRequest
    - Response: 204 No Content
  - POST /api/v1/clusters/{clusterId}/consumer-groups/{groupId}/members/remove — Remove members
    - Request DTO: ConsumerGroupMemberRemovalRequest
    - Response: 204 No Content

### DelegationTokenController
- Base path: /api/v1/clusters/{clusterId}/delegation-tokens
- Annotations: @RestController, @RequestMapping, @Tag("Delegation Tokens"), @SecurityRequirement(name = "bearerAuth")
- Endpoints:
  - GET /api/v1/clusters/{clusterId}/delegation-tokens — List delegation tokens
    - Response DTO: DelegationTokenResponse (List)
  - POST /api/v1/clusters/{clusterId}/delegation-tokens — Create token
    - Request DTO: DelegationTokenCreateRequest
    - Response DTO: DelegationTokenResponse
  - POST /api/v1/clusters/{clusterId}/delegation-tokens/{tokenId}/renew — Renew token
    - Request DTO: DelegationTokenRenewRequest
    - Response DTO: DelegationTokenResponse
  - POST /api/v1/clusters/{clusterId}/delegation-tokens/{tokenId}/expire — Expire token
    - Request DTO: DelegationTokenExpireRequest
    - Response: 204 No Content

### AclController
- Base path: /api/v1/clusters/{clusterId}/acls
- Annotations: @RestController, @RequestMapping, @Tag("ACLs"), @SecurityRequirement(name = "bearerAuth")
- Endpoints:
  - GET /api/v1/clusters/{clusterId}/acls — List ACLs (supports multiple query filters)
    - Response DTO: AclResponse (List)
  - POST /api/v1/clusters/{clusterId}/acls — Create ACLs
    - Request DTO: AclCreateRequest
    - Response: 204 No Content
  - POST /api/v1/clusters/{clusterId}/acls/delete — Delete ACLs (by filter)
    - Request DTO: AclDeleteRequest
    - Response: 204 No Content

### ClientMetricsController
- Base path: /api/v1/clusters/{clusterId}/client-metrics
- Annotations: @RestController, @RequestMapping, @Tag("Client Metrics"), @SecurityRequirement(name = "bearerAuth")
- Endpoints:
  - GET /api/v1/clusters/{clusterId}/client-metrics — List client metric resources
    - Response DTO: ClientMetricResourceResponse (List)

### TopicController
- Base path: /api/v1/clusters/{clusterId}/topics
- Annotations: @RestController, @RequestMapping, @Tag("Topics"), @SecurityRequirement(name = "bearerAuth")
- Endpoints (selected):
  - GET /api/v1/clusters/{clusterId}/topics — List topics (query: includeInternal, prefix)
    - Response DTO: TopicSummaryResponse (List)
  - GET /api/v1/clusters/{clusterId}/topics/{topicName} — Describe topic
    - Response DTO: TopicDetailResponse
  - GET /api/v1/clusters/{clusterId}/topics/{topicName}/configs — Topic configs
    - Response DTO: Map<String, String>
  - POST /api/v1/clusters/{clusterId}/topics — Create topic
    - Request DTO: TopicCreateRequest
    - Response: 204 No Content
  - DELETE /api/v1/clusters/{clusterId}/topics/{topicName} — Delete topic (query: dryRun)
    - Response: 204 No Content
  - GET /api/v1/clusters/{clusterId}/topics/{topicName}/offsets — List offsets (query: mode, timestamp)
    - Response DTO: TopicOffsetResponse (List)
  - POST /api/v1/clusters/{clusterId}/topics/{topicName}/records/delete — Delete records
    - Request DTO: TopicRecordDeleteRequest
    - Response: 204 No Content
  - POST /api/v1/clusters/{clusterId}/topics/{topicName}/partitions — Create partitions
    - Request DTO: TopicPartitionExpansionRequest
    - Response: 204 No Content
  - PATCH /api/v1/clusters/{clusterId}/topics/{topicName}/configs — Alter topic configs
    - Request DTO: TopicConfigMutationBatchRequest
    - Response: 204 No Content

### ClusterAdminController
- Base path: /api/v1/clusters/{clusterId}/actions
- Annotations: @RestController, @RequestMapping, @Tag("Cluster Admin"), @SecurityRequirement(name = "bearerAuth")
- Endpoints:
  - POST /api/v1/clusters/{clusterId}/actions/leader-election — Preferred leader election
    - Request DTO: LeaderElectionRequest
    - Response: 204 No Content
  - PUT /api/v1/clusters/{clusterId}/actions/partition-reassignments — Start reassignment
    - Request DTO: PartitionReassignmentRequest
    - Response: 204 No Content
  - GET /api/v1/clusters/{clusterId}/actions/partition-reassignments — List reassignments
    - Response DTO: PartitionReassignmentResponse (List)
  - GET /api/v1/clusters/{clusterId}/actions/log-dirs — Describe log dirs (query: brokerIds)
    - Response DTO: BrokerLogDirResponse (List)
  - PUT /api/v1/clusters/{clusterId}/actions/log-dirs — Alter replica log dirs
    - Request DTO: ReplicaLogDirRequest
    - Response: 204 No Content

### OperationController
- Base path: /api/v1/clusters/{clusterId}/operations
- Annotations: @RestController, @RequestMapping, @Tag("Operations"), @SecurityRequirement(name = "bearerAuth")
- Endpoints:
  - POST /api/v1/clusters/{clusterId}/operations — Submit operation
    - Request DTO: SubmitOperationRequest
    - Response DTO: OperationDetailResponse
  - GET /api/v1/clusters/{clusterId}/operations — List operations (query: page, size)
    - Response DTO: Page<OperationSummaryResponse>
  - GET /api/v1/clusters/{clusterId}/operations/{operationId} — Get operation
    - Response DTO: OperationDetailResponse
  - GET /api/v1/clusters/{clusterId}/operations/{operationId}/events — Get operation events
    - Response DTO: OperationEventResponse (List)
  - POST /api/v1/clusters/{clusterId}/operations/{operationId}/cancel — Cancel operation
    - Response: 204 No Content
  - POST /api/v1/clusters/{clusterId}/operations/{operationId}/retry — Retry operation
    - Response: 204 No Content

### ScramController
- Base path: /api/v1/clusters/{clusterId}/scram/users
- Annotations: @RestController, @RequestMapping, @Tag("SCRAM"), @SecurityRequirement(name = "bearerAuth")
- Endpoints:
  - GET /api/v1/clusters/{clusterId}/scram/users — Describe SCRAM users (query: userNames)
    - Response DTO: ScramUsersResponse
  - PUT /api/v1/clusters/{clusterId}/scram/users/{userName} — Upsert SCRAM credential
    - Request DTO: ScramCredentialUpsertRequest
    - Response: 204 No Content
  - DELETE /api/v1/clusters/{clusterId}/scram/users/{userName} — Delete SCRAM credential
    - Request DTO: ScramCredentialDeleteRequest
    - Response: 204 No Content

### BrokerJmxMetricsController
- Base path: /api/v1/metrics/broker-jmx
- Annotations: @RestController, @RequestMapping, @Tag("Broker JMX Metrics"), @SecurityRequirement(name = "bearerAuth")
- Endpoints:
  - GET /api/v1/metrics/broker-jmx — Get latest broker JMX metrics
    - Response DTO: BrokerJmxMetricsCollectorService.BrokerJmxSnapshot

### BrokerMetricsDiagnosticsController
- Base path: /api/v1/clusters/{clusterId}/brokers/{brokerId}/metrics/diagnostics
- Annotations: @RestController, @RequestMapping, @Tag("Metrics Diagnostics"), @SecurityRequirement(name = "bearerAuth")
- Endpoints:
  - GET /api/v1/clusters/{clusterId}/brokers/{brokerId}/metrics/diagnostics — List recognized metric names and diagnostic statuses
    - Response DTO: DiagnosticsResponse (record declared in controller)
    - Notes: feature-gated by PrometheusScrapeProperties.diagnosticsEnabled(); requires admin role check inside controller (ROLE_ADMIN or ADMIN). Returns 403 if disabled or not admin.

### ClusterStructuralMetricsController
- Base path: /api/v1/clusters/{clusterId}/metrics/structural
- Annotations: @RestController, @RequestMapping, @Tag("Structural Metrics"), @SecurityRequirement(name = "bearerAuth")
- Endpoints:
  - GET /api/v1/clusters/{clusterId}/metrics/structural — Get cached structural cluster metrics
    - Response DTO: AdminClientMetricsSnapshot
    - Notes: returns 404 if cluster unknown, 503 if metrics not yet available.

### RuntimeMetricsController
- Base path: /api/v1
- Annotations: @RestController, @RequestMapping("/api/v1"), @Tag("Runtime Metrics"), @SecurityRequirement(name = "bearerAuth")
- Endpoints (selected):
  - GET /api/v1/clusters/{clusterId}/brokers/{brokerId}/metrics — Get broker runtime metrics (aggregated)
    - Response DTO: BrokerMetricsResponse (record declared in controller)
    - Query: window (supported values: null or "1m")
  - GET /api/v1/clusters/{clusterId}/topics/{topic}/metrics — Get topic aggregated metrics
    - Response DTO: TopicMetricsResponse
    - Query: window, perBroker
  - GET /api/v1/clusters/{clusterId}/metrics — Get cluster aggregated metrics
    - Response DTO: ClusterMetricsResponse

### MetadataQuorumController
- Base path: /api/v1/clusters/{clusterId}/metadata-quorum
- Annotations: @RestController, @RequestMapping, @Tag("Metadata Quorum"), @SecurityRequirement(name = "bearerAuth")
- Endpoints:
  - GET /api/v1/clusters/{clusterId}/metadata-quorum — Metadata quorum status
    - Response DTO: MetadataQuorumResponse

### StructuralMetricsController
- Base path: /api/v1/metrics/structural
- Annotations: @RestController, @RequestMapping, @Tag("Structural Metrics"), @SecurityRequirement(name = "bearerAuth")
- Endpoints:
  - GET /api/v1/metrics/structural — Get structural cluster metrics
    - Response DTO: AdminClientMetricsSnapshot

---