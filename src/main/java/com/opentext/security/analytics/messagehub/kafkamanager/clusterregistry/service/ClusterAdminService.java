package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service;

import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api.*;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiErrorCode;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiException;
import com.opentext.security.analytics.messagehub.kafkamanager.common.TopicPartitionRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.service.AdminMutationRecorder;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/**
 * Cluster-level administrative service for leader election, partition reassignments and log-dir operations.
 *
 * <p>Provides higher-level orchestration over AdminClient features such as preferred/unclean leader
 * election, partition reassignment management and broker log directory inspection and changes.
 */
@Service
public class ClusterAdminService {

    private static final String ELECT_LEADERS_ACTION = "elect-leaders";

    private final KafkaAdminExecutionService adminExecutionService;
    private final AdminMutationRecorder mutationRecorder;
    private final KafkaManagerProperties properties;

    public ClusterAdminService(
            KafkaAdminExecutionService adminExecutionService,
            AdminMutationRecorder mutationRecorder,
            KafkaManagerProperties properties) {
        this.adminExecutionService = adminExecutionService;
        this.mutationRecorder = mutationRecorder;
        this.properties = properties;
    }

    /**
     * Trigger leader election for specified partitions or the whole cluster.
     *
     * <p>When requesting an UNCLEAN election, explicit partitions must be provided. Preferred
     * elections are no-ops on single-node clusters.
     *
     * @param clusterId the target Kafka cluster id
     * @param request request describing election type and targeted partitions
     * @throws ApiException for invalid request combinations
     */
    public void electLeaders(UUID clusterId, LeaderElectionRequest request) {
        if (request.electionType() == ElectionType.UNCLEAN
                && (request.partitions() == null || request.partitions().isEmpty())) {
            throw new ApiException(
                    HttpStatus.BAD_REQUEST,
                    ApiErrorCode.VALIDATION_ERROR,
                    "UNCLEAN leader election must target explicit partitions");
        }
        mutationRecorder.record(
                clusterId,
                ELECT_LEADERS_ACTION,
                "cluster",
                false,
                request,
                () -> adminExecutionService.execute(
                        clusterId, ELECT_LEADERS_ACTION, properties.admin().defaultOperationTimeout(), handle -> {
                            Admin admin = handle.admin();
                            Set<TopicPartition> partitions = request.partitions() == null
                                            || request.partitions().isEmpty()
                                    ? null
                                    : request.partitions().stream()
                                            .map(this::partition)
                                            .collect(Collectors.toSet());
                            if (request.electionType() == ElectionType.PREFERRED
                                    && adminExecutionService
                                                    .await(
                                                            clusterId,
                                                            ELECT_LEADERS_ACTION,
                                                            properties.admin().defaultRequestTimeout(),
                                                            admin.describeCluster()
                                                                    .nodes())
                                                    .size()
                                            <= 1) {
                                return null;
                            }
                            try {
                                adminExecutionService.await(
                                        clusterId,
                                        ELECT_LEADERS_ACTION,
                                        properties.admin().defaultOperationTimeout(),
                                        admin.electLeaders(request.electionType(), partitions)
                                                .all());
                            } catch (ElectionNotNeededException ignored) {
                                return null;
                            }
                            return null;
                        }));
    }

    /**
     * Alter partition replica assignments (start/cancel reassignments) for multiple partitions.
     *
     * @param clusterId the target Kafka cluster id
     * @param request the reassignment request with changes
     */
    public void alterPartitionReassignments(UUID clusterId, PartitionReassignmentRequest request) {
        mutationRecorder.record(
                clusterId,
                "alter-partition-reassignments",
                "cluster",
                false,
                request,
                () -> adminExecutionService.execute(
                        clusterId,
                        "alter-partition-reassignments",
                        properties.admin().defaultOperationTimeout(),
                        handle -> {
                            Admin admin = handle.admin();
                            Map<TopicPartition, Optional<NewPartitionReassignment>> changes = new LinkedHashMap<>();
                            for (PartitionReassignmentChange change : request.changes()) {
                                TopicPartition partition = new TopicPartition(change.topic(), change.partition());
                                changes.put(
                                        partition,
                                        change.cancel()
                                                ? Optional.empty()
                                                : Optional.of(new NewPartitionReassignment(change.replicas())));
                            }
                            adminExecutionService.await(
                                    clusterId,
                                    "alter-partition-reassignments",
                                    properties.admin().defaultOperationTimeout(),
                                    admin.alterPartitionReassignments(changes).all());
                            return null;
                        }));
    }

    /**
     * List currently active partition reassignments in the cluster.
     *
     * @param clusterId the target Kafka cluster id
     * @return list of {@link PartitionReassignmentResponse}
     */
    public List<PartitionReassignmentResponse> listPartitionReassignments(UUID clusterId) {
        return adminExecutionService.execute(
                clusterId, "list-partition-reassignments", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    Map<TopicPartition, org.apache.kafka.clients.admin.PartitionReassignment> reassignments =
                            adminExecutionService.await(
                                    clusterId,
                                    "list-partition-reassignments",
                                    properties.admin().defaultRequestTimeout(),
                                    admin.listPartitionReassignments().reassignments());
                    return reassignments.entrySet().stream()
                            .map(entry -> toResponse(entry.getKey(), entry.getValue()))
                            .toList();
                });
    }

    /**
     * Describe log directories for the provided brokers, returning replica information and sizes.
     *
     * @param clusterId the target Kafka cluster id
     * @param brokerIds list of broker ids to describe
     * @return list of {@link BrokerLogDirResponse}
     */
    public List<BrokerLogDirResponse> describeLogDirs(UUID clusterId, List<Integer> brokerIds) {
        return adminExecutionService.execute(
                clusterId, "describe-log-dirs", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    Map<Integer, Map<String, LogDirDescription>> result = adminExecutionService.await(
                            clusterId,
                            "describe-log-dirs",
                            properties.admin().defaultRequestTimeout(),
                            admin.describeLogDirs(brokerIds).allDescriptions());
                    return result.entrySet().stream()
                            .flatMap(entry -> entry.getValue().entrySet().stream()
                                    .map(dir -> toLogDirResponse(entry.getKey(), dir.getKey(), dir.getValue())))
                            .toList();
                });
    }

    /**
     * Alter replica log directory assignments for replicas on brokers.
     *
     * @param clusterId the target Kafka cluster id
     * @param request request containing topic/partition/broker -> logDir mappings
     */
    public void alterReplicaLogDirs(UUID clusterId, ReplicaLogDirRequest request) {
        mutationRecorder.record(
                clusterId,
                "alter-replica-log-dirs",
                "cluster",
                false,
                request,
                () -> adminExecutionService.execute(
                        clusterId, "alter-replica-log-dirs", properties.admin().defaultOperationTimeout(), handle -> {
                            Admin admin = handle.admin();
                            Map<TopicPartitionReplica, String> assignments = new LinkedHashMap<>();
                            for (ReplicaLogDirChange change : request.changes()) {
                                assignments.put(
                                        new TopicPartitionReplica(
                                                change.topic(), change.partition(), change.brokerId()),
                                        change.logDir());
                            }
                            adminExecutionService.await(
                                    clusterId,
                                    "alter-replica-log-dirs",
                                    properties.admin().defaultOperationTimeout(),
                                    admin.alterReplicaLogDirs(assignments).all());
                            return null;
                        }));
    }

    private TopicPartition partition(TopicPartitionRequest request) {
        return new TopicPartition(request.topic(), request.partition());
    }

    private PartitionReassignmentResponse toResponse(
            TopicPartition partition, org.apache.kafka.clients.admin.PartitionReassignment reassignment) {
        return new PartitionReassignmentResponse(
                partition.topic(),
                partition.partition(),
                reassignment.replicas(),
                reassignment.addingReplicas(),
                reassignment.removingReplicas());
    }

    private BrokerLogDirResponse toLogDirResponse(int brokerId, String logDir, LogDirDescription description) {
        List<BrokerLogDirReplicaResponse> replicas = description.replicaInfos().entrySet().stream()
                .map(entry -> toReplica(entry.getKey(), entry.getValue()))
                .toList();
        return new BrokerLogDirResponse(
                brokerId,
                logDir,
                description.totalBytes().orElse(-1),
                description.usableBytes().orElse(-1),
                description.error() == null ? null : description.error().getMessage(),
                replicas);
    }

    private BrokerLogDirReplicaResponse toReplica(TopicPartition partition, ReplicaInfo info) {
        return new BrokerLogDirReplicaResponse(
                partition.topic(), partition.partition(), info.size(), info.offsetLag(), info.isFuture());
    }
}
