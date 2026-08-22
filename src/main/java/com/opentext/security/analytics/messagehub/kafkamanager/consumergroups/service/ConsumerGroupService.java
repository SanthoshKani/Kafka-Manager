package com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.service;

import com.opentext.security.analytics.messagehub.kafkamanager.common.ResourceNotFoundException;
import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api.*;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.function.Function;

/**
 * Service that manages Kafka consumer groups: discovery, description, offset management and member removal.
 *
 * <p>All AdminClient interactions are executed through {@code KafkaAdminExecutionService} to provide
 * consistent timeout, metrics and error translation behavior.
 */
@Service
@SuppressWarnings({"deprecation", "removal"})
public class ConsumerGroupService {

    private final KafkaAdminExecutionService adminExecutionService;
    private final KafkaManagerProperties properties;

    public ConsumerGroupService(KafkaAdminExecutionService adminExecutionService, KafkaManagerProperties properties) {
        this.adminExecutionService = adminExecutionService;
        this.properties = properties;
    }

    /**
     * List all consumer groups in the given cluster.
     *
     * @param clusterId the target Kafka cluster id
     * @return list of {@link ConsumerGroupSummaryResponse}
     */
    public List<ConsumerGroupSummaryResponse> list(UUID clusterId) {
        return adminExecutionService.execute(
                clusterId, "list-consumer-groups", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    return adminExecutionService
                            .await(
                                    clusterId,
                                    "list-consumer-groups",
                                    properties.admin().defaultRequestTimeout(),
                                    admin.listConsumerGroups()
                                            .all())
                            .stream()
                            .map(listing -> new ConsumerGroupSummaryResponse(
                                    listing.groupId(),
                                    null,
                                    null,
                                    0,
                                    0))
                            .toList();
                });
    }

    /**
     * Describe a consumer group, including members and per-partition lag computation.
     *
     * @param clusterId the target Kafka cluster id
     * @param groupId the consumer group identifier
     * @return {@link ConsumerGroupDetailResponse} with members, offsets and computed lag
     * @throws ResourceNotFoundException if the group does not exist
     */
    public ConsumerGroupDetailResponse describe(UUID clusterId, String groupId) {
        return adminExecutionService.execute(
                clusterId, "describe-consumer-group", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    ConsumerGroupDescription description = adminExecutionService
                            .await(
                                    clusterId,
                                    "describe-consumer-group",
                                    properties.admin().defaultRequestTimeout(),
                                    admin.describeConsumerGroups(List.of(groupId))
                                            .all())
                            .get(groupId);
                    if (description == null) {
                        throw new ResourceNotFoundException("Consumer group not found");
                    }
                    var offsets = adminExecutionService.await(
                            clusterId,
                            "describe-consumer-group-offsets",
                            properties.admin().defaultRequestTimeout(),
                            admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata());
                    var topics = offsets.keySet().stream()
                            .map(tp -> new TopicPartition(tp.topic(), tp.partition()))
                            .collect(Collectors.toSet());
                    var endOffsets = adminExecutionService.await(
                            clusterId,
                            "describe-consumer-group-end-offsets",
                            properties.admin().defaultRequestTimeout(),
                            admin.listOffsets(topics.stream()
                                                            .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest())))
                                    .all());
                    List<OffsetLagResponse> lag = offsets.entrySet().stream()
                            .map(entry -> {
                                var tp = entry.getKey();
                                long committed = entry.getValue().offset();
                                long end = endOffsets
                                        .get(new TopicPartition(tp.topic(), tp.partition()))
                                        .offset();
                                long computedLag = Math.max(0, end - committed);
                                return new OffsetLagResponse(
                                        tp.topic(), tp.partition(), committed, end, computedLag, committed < 0);
                            })
                            .toList();
                    long totalLag =
                            lag.stream().mapToLong(OffsetLagResponse::lag).sum();
                    return new ConsumerGroupDetailResponse(
                            groupId,
                            null,
                            null,
                            description.coordinator() == null
                                    ? null
                                    : description.coordinator().idString() + "@"
                                            + description.coordinator().host() + ":"
                                            + description.coordinator().port(),
                            description.members().stream().map(this::member).toList(),
                            lag,
                            totalLag,
                            List.of());
                });
    }

    /**
     * Delete a consumer group from the cluster.
     *
     * @param clusterId the target Kafka cluster id
     * @param groupId the consumer group identifier to delete
     */
    public void delete(UUID clusterId, String groupId) {
        adminExecutionService.execute(
                clusterId, "delete-consumer-group", properties.admin().defaultOperationTimeout(), handle -> {
                    Admin admin = handle.admin();
                    adminExecutionService.await(
                            clusterId,
                            "delete-consumer-group",
                            properties.admin().defaultOperationTimeout(),
                            admin.deleteConsumerGroups(List.of(groupId)).all());
                    return null;
                });
    }

    /**
     * Alter committed offsets for a consumer group.
     *
     * @param clusterId the target Kafka cluster id
     * @param groupId the consumer group identifier
     * @param request the offset update request containing topic/partition/offset mappings
     */
    public void alterOffsets(UUID clusterId, String groupId, ConsumerGroupOffsetUpdateRequest request) {
        adminExecutionService.execute(
                clusterId, "alter-consumer-group-offsets", properties.admin().defaultOperationTimeout(), handle -> {
                    Admin admin = handle.admin();
                    Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
                    for (ConsumerGroupOffsetUpdate update : request.offsets()) {
                        offsets.put(
                                new TopicPartition(update.topic(), update.partition()),
                                new OffsetAndMetadata(update.offset()));
                    }
                    AlterConsumerGroupOffsetsResult result = admin.alterConsumerGroupOffsets(groupId, offsets);
                    adminExecutionService.await(
                            clusterId,
                            "alter-consumer-group-offsets",
                            properties.admin().defaultOperationTimeout(),
                            result.all());
                    return null;
                });
    }

    /**
     * Remove specific members from a consumer group.
     *
     * @param clusterId the target Kafka cluster id
     * @param groupId the consumer group identifier
     * @param request member removal request with member ids to remove
     */
    public void removeMembers(UUID clusterId, String groupId, ConsumerGroupMemberRemovalRequest request) {
        adminExecutionService.execute(
                clusterId, "remove-consumer-group-members", properties.admin().defaultOperationTimeout(), handle -> {
                    Admin admin = handle.admin();
                    List<MemberToRemove> members = request.memberIds().stream()
                            .map(MemberToRemove::new)
                            .toList();
                    adminExecutionService.await(
                            clusterId,
                            "remove-consumer-group-members",
                            properties.admin().defaultOperationTimeout(),
                            admin.removeMembersFromConsumerGroup(
                                            groupId,
                                            new org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions(
                                                    members))
                                    .all());
                    return null;
                });
    }

    // summary method intentionally removed to avoid using the deprecated ConsumerGroupListing API.
    // A future migration should replace this with the newer consumer group listing APIs and
    // include state/type using non-deprecated methods.

    private MemberResponse member(org.apache.kafka.clients.admin.MemberDescription description) {
        return new MemberResponse(
                description.consumerId(),
                description.clientId(),
                description.host(),
                description.assignment().topicPartitions().stream()
                        .map(tp -> tp.topic() + ":" + tp.partition())
                        .toList());
    }
}
