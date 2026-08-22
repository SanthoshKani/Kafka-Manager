package com.opentext.security.analytics.messagehub.kafkamanager.topics.service;

import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiErrorCode;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiException;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ResourceNotFoundException;
import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.service.AdminMutationRecorder;
import com.opentext.security.analytics.messagehub.kafkamanager.topics.api.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/**
 * Service responsible for topic-level operations against Kafka clusters.
 *
 * <p>This service uses the {@code KafkaAdminExecutionService} to perform resilient AdminClient
 * operations and {@code AdminMutationRecorder} to record mutating actions for auditability.
 * Typical responsibilities include listing topics, describing topic metadata and configs,
 * creating/deleting topics, expanding partitions, altering topic configs, reading offsets and
 * deleting records.
 */
@Service
public class TopicService {

    private final KafkaAdminExecutionService adminExecutionService;
    private final AdminMutationRecorder mutationRecorder;
    private final KafkaManagerProperties properties;

    public TopicService(
            KafkaAdminExecutionService adminExecutionService,
            AdminMutationRecorder mutationRecorder,
            KafkaManagerProperties properties) {
        this.adminExecutionService = adminExecutionService;
        this.mutationRecorder = mutationRecorder;
        this.properties = properties;
    }

    /**
     * List topics in a cluster with optional filtering.
     *
     * @param clusterId the target Kafka cluster id
     * @param includeInternal when true, include internal Kafka topics in the result
     * @param prefix optional substring filter; when null or blank no filtering by name occurs
     * @return list of {@link TopicSummaryResponse} objects representing discovered topics
     */
    public List<TopicSummaryResponse> list(UUID clusterId, boolean includeInternal, String prefix) {
        return adminExecutionService.execute(
                clusterId, "list-topics", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    ListTopicsOptions options = new ListTopicsOptions().listInternal(includeInternal);
                    var names = adminExecutionService.await(
                            clusterId,
                            "list-topics",
                            properties.admin().defaultRequestTimeout(),
                            admin.listTopics(options).names());
                    return names.stream()
                            .filter(name -> prefix == null || prefix.isBlank() || name.contains(prefix))
                            .sorted()
                            .map(name -> summarize(clusterId, admin, name))
                            .toList();
                });
    }

    /**
     * Describe a single topic including partition layout and topic-level config map.
     *
     * @param clusterId the target Kafka cluster id
     * @param topicName the name of the topic to describe
     * @return detailed {@link TopicDetailResponse} for the requested topic
     * @throws ResourceNotFoundException if the topic does not exist
     */
    public TopicDetailResponse describe(UUID clusterId, String topicName) {
        return adminExecutionService.execute(
                clusterId, "describe-topic", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    TopicDescription description = topicDescription(clusterId, admin, topicName);
                    Map<String, String> configs = topicConfigs(clusterId, admin, topicName);
                    return new TopicDetailResponse(
                            description.name(),
                            description.isInternal(),
                            description.partitions().size(),
                            (short) description.partitions().stream()
                                    .mapToInt(p -> p.replicas().size())
                                    .max()
                                    .orElse(0),
                            description.partitions().stream()
                                    .map(this::partition)
                                    .toList(),
                            configs,
                            List.of());
                });
    }

    /**
     * Retrieve the configuration map for a topic.
     *
     * @param clusterId the target Kafka cluster id
     * @param topicName the name of the topic
     * @return ordered map of config key to value for the topic
     */
    public Map<String, String> describeConfigs(UUID clusterId, String topicName) {
        return adminExecutionService.execute(
                clusterId,
                "describe-topic-configs",
                properties.admin().defaultRequestTimeout(),
                handle -> topicConfigs(clusterId, handle.admin(), topicName));
    }

    /**
     * Delete a topic from the cluster or perform a dry-run validation.
     *
     * @param clusterId the target Kafka cluster id
     * @param topicName the name of the topic to delete
     * @param dryRun when true, validate the deletion without performing it
     */
    public void delete(UUID clusterId, String topicName, boolean dryRun) {
        mutationRecorder.record(
                clusterId,
                dryRun ? "dry-run-delete-topic" : "delete-topic",
                topicName,
                dryRun,
                topicName,
                () -> adminExecutionService.execute(
                        clusterId,
                        dryRun ? "dry-run-delete-topic" : "delete-topic",
                        properties.admin().defaultOperationTimeout(),
                        handle -> {
                            Admin admin = handle.admin();
                            if (!dryRun) {
                                admin.deleteTopics(List.of(topicName))
                                        .all()
                                        .toCompletionStage()
                                        .toCompletableFuture()
                                        .join();
                            }
                            return null;
                        }));
    }

    /**
     * Create a new topic in the cluster according to the supplied request.
     *
     * @param clusterId the target Kafka cluster id
     * @param request the {@link TopicCreateRequest} containing name, partitions, replication and configs
     */
    public void create(UUID clusterId, TopicCreateRequest request) {
        mutationRecorder.record(
                clusterId,
                "create-topic",
                request.topicName(),
                false,
                request,
                () -> adminExecutionService.execute(
                        clusterId, "create-topic", properties.admin().defaultOperationTimeout(), handle -> {
                            Admin admin = handle.admin();
                            org.apache.kafka.clients.admin.NewTopic topic = new org.apache.kafka.clients.admin.NewTopic(
                                            request.topicName(), request.partitions(), request.replicationFactor())
                                    .configs(request.configs());
                            admin.createTopics(List.of(topic))
                                    .all()
                                    .toCompletionStage()
                                    .toCompletableFuture()
                                    .join();
                            return null;
                        }));
    }

    /**
     * Increase the partition count for an existing topic.
     *
     * <p>This validates that the requested total partition count is greater than the current
     * partition count and then issues a createPartitions call to Kafka.
     *
     * @param clusterId the target Kafka cluster id
     * @param topicName the name of the topic to expand
     * @param request the partition expansion request
     * @throws ApiException when validation fails (for example when requested total is not larger)
     */
    public void createPartitions(UUID clusterId, String topicName, TopicPartitionExpansionRequest request) {
        executeMutation(
                clusterId,
                "create-topic-partitions",
                topicName,
                false,
                request,
                properties.admin().defaultOperationTimeout(),
                admin -> {
                    int currentPartitions = topicDescription(clusterId, admin, topicName)
                            .partitions()
                            .size();
                    if (request.totalPartitions() <= currentPartitions) {
                        throw new ApiException(
                                HttpStatus.BAD_REQUEST,
                                ApiErrorCode.VALIDATION_ERROR,
                                "totalPartitions must be greater than current partition count");
                    }
                    NewPartitions newPartitions = request.replicaAssignments() == null
                                    || request.replicaAssignments().isEmpty()
                            ? NewPartitions.increaseTo(request.totalPartitions())
                            : NewPartitions.increaseTo(request.totalPartitions(), request.replicaAssignments());
                    adminExecutionService.await(
                            clusterId,
                            "create-topic-partitions",
                            properties.admin().defaultOperationTimeout(),
                            admin.createPartitions(Map.of(topicName, newPartitions), new CreatePartitionsOptions())
                                    .all());
                    return null;
                });
    }

    /**
     * Alter topic-level configurations using incremental alter config operations.
     *
     * @param clusterId the target Kafka cluster id
     * @param topicName the name of the topic to modify
     * @param request batch of config mutation requests
     */
    public void alterConfigs(UUID clusterId, String topicName, TopicConfigMutationBatchRequest request) {
        executeMutation(
                clusterId,
                "alter-topic-configs",
                topicName,
                false,
                request,
                properties.admin().defaultOperationTimeout(),
                admin -> {
                    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
                    List<AlterConfigOp> operations = new ArrayList<>();
                    for (TopicConfigMutationRequest change : request.changes()) {
                        AlterConfigOp.OpType opType =
                                switch (change.operation()) {
                                    case SET -> AlterConfigOp.OpType.SET;
                                    case DELETE -> AlterConfigOp.OpType.DELETE;
                                };
                        operations.add(new AlterConfigOp(new ConfigEntry(change.name(), change.value()), opType));
                    }
                    AlterConfigsResult result = admin.incrementalAlterConfigs(Map.of(resource, operations));
                    adminExecutionService.await(
                            clusterId,
                            "alter-topic-configs",
                            properties.admin().defaultOperationTimeout(),
                            result.all());
                    return null;
                });
    }

    /**
     * List offsets for each partition of a topic according to the lookup mode.
     *
     * <p>Supported modes: EARLIEST, LATEST, TIMESTAMP. When using TIMESTAMP, the {@code timestamp}
     * parameter must be supplied.
     *
     * @param clusterId the target Kafka cluster id
     * @param topicName the name of the topic
     * @param mode the lookup mode (earliest/latest/timestamp)
     * @param timestamp required when {@code mode} is TIMESTAMP; the epoch millis to lookup
     * @return list of {@link TopicOffsetResponse} representing partition offsets
     */
    public List<TopicOffsetResponse> listOffsets(
            UUID clusterId, String topicName, TopicOffsetLookupMode mode, Long timestamp) {
        return adminExecutionService.execute(
                clusterId, "list-topic-offsets", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    TopicDescription description = topicDescription(clusterId, admin, topicName);
                    Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
                    for (TopicPartitionInfo partition : description.partitions()) {
                        TopicPartition topicPartition = new TopicPartition(topicName, partition.partition());
                        OffsetSpec spec =
                                switch (mode) {
                                    case EARLIEST -> OffsetSpec.earliest();
                                    case LATEST -> OffsetSpec.latest();
                                    case TIMESTAMP -> {
                                        if (timestamp == null) {
                                            throw new ApiException(
                                                    HttpStatus.BAD_REQUEST,
                                                    ApiErrorCode.VALIDATION_ERROR,
                                                    "timestamp is required when mode=TIMESTAMP");
                                        }
                                        yield OffsetSpec.forTimestamp(timestamp);
                                    }
                                };
                        request.put(topicPartition, spec);
                    }
                    return adminExecutionService
                            .await(
                                    clusterId,
                                    "list-topic-offsets",
                                    properties.admin().defaultRequestTimeout(),
                                    admin.listOffsets(request).all())
                            .entrySet()
                            .stream()
                            .map(entry -> new TopicOffsetResponse(
                                    entry.getKey().partition(),
                                    entry.getValue().offset(),
                                    entry.getValue().timestamp()))
                            .toList();
                });
    }

    /**
     * Delete records up to specified offsets for topic partitions.
     *
     * @param clusterId the target Kafka cluster id
     * @param topicName the name of the topic
     * @param request partitions and offsets to delete before
     */
    public void deleteRecords(UUID clusterId, String topicName, TopicRecordDeleteRequest request) {
        executeMutation(
                clusterId,
                "delete-topic-records",
                topicName,
                false,
                request,
                properties.admin().defaultOperationTimeout(),
                admin -> {
                    Map<TopicPartition, RecordsToDelete> records = new LinkedHashMap<>();
                    for (var partition : request.partitions()) {
                        records.put(
                                new TopicPartition(topicName, partition.partition()),
                                RecordsToDelete.beforeOffset(partition.beforeOffset()));
                    }
                    adminExecutionService.await(
                            clusterId,
                            "delete-topic-records",
                            properties.admin().defaultOperationTimeout(),
                            admin.deleteRecords(records).all());
                    return null;
                });
    }

    private TopicSummaryResponse summarize(UUID clusterId, Admin admin, String name) {
        TopicDescription description = topicDescription(clusterId, admin, name);
        int partitions = description.partitions().size();
        short replicationFactor = description.partitions().isEmpty()
                ? 0
                : (short) description.partitions().get(0).replicas().size();
        return new TopicSummaryResponse(
                name,
                description.isInternal(),
                partitions,
                replicationFactor,
                description.partitions().stream()
                        .map(partition -> partition.partition())
                        .sorted()
                        .toList(),
                List.of());
    }

    private TopicDescription topicDescription(UUID clusterId, Admin admin, String topicName) {
        DescribeTopicsResult result = admin.describeTopics(List.of(topicName));
        try {
            return adminExecutionService
                    .await(
                            clusterId,
                            "describe-topic",
                            properties.admin().defaultRequestTimeout(),
                            result.allTopicNames())
                    .get(topicName);
        } catch (Exception exception) {
            throw new ResourceNotFoundException("Topic not found");
        }
    }

    private Map<String, String> topicConfigs(UUID clusterId, Admin admin, String topicName) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Config config = adminExecutionService
                .await(
                        clusterId,
                        "describe-topic-configs",
                        properties.admin().defaultRequestTimeout(),
                        admin.describeConfigs(List.of(resource)).all())
                .get(resource);
        return config.entries().stream()
                .collect(Collectors.toMap(
                        ConfigEntry::name, ConfigEntry::value, (left, right) -> left, LinkedHashMap::new));
    }

    private TopicPartitionResponse partition(TopicPartitionInfo info) {
        return new TopicPartitionResponse(
                info.partition(),
                leaderId(info.leader()),
                info.replicas().stream().map(Node::id).toList(),
                info.isr().stream().map(Node::id).toList(),
                info.leader() == null || info.leader().id() < 0,
                info.isr().size() < info.replicas().size());
    }

    private Integer leaderId(Node leader) {
        return leader == null ? null : leader.id();
    }

    private <T> T executeMutation(
            UUID clusterId,
            String action,
            String target,
            boolean dryRun,
            Object payload,
            Duration timeout,
            java.util.function.Function<Admin, T> function) {
        return mutationRecorder.record(
                clusterId,
                action,
                target,
                dryRun,
                payload,
                () -> adminExecutionService.execute(
                        clusterId, action, timeout, handle -> function.apply(handle.admin())));
    }
}
