package com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.service;

import com.opentext.security.analytics.messagehub.kafkamanager.common.ResourceNotFoundException;
import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api.ConsumerGroupDetailResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api.ConsumerGroupMemberRemovalRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api.ConsumerGroupOffsetUpdate;
import com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api.ConsumerGroupOffsetUpdateRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api.ConsumerGroupSummaryResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api.MemberResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api.OffsetLagResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

@Service
public class ConsumerGroupService {

    private final KafkaAdminExecutionService adminExecutionService;
    private final KafkaManagerProperties properties;

    public ConsumerGroupService(KafkaAdminExecutionService adminExecutionService, KafkaManagerProperties properties) {
        this.adminExecutionService = adminExecutionService;
        this.properties = properties;
    }

    public List<ConsumerGroupSummaryResponse> list(UUID clusterId) {
        return adminExecutionService.execute(
                clusterId, "list-consumer-groups", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    return adminExecutionService
                            .await(
                                    clusterId,
                                    "list-consumer-groups",
                                    properties.admin().defaultRequestTimeout(),
                                    admin.listConsumerGroups(new ListConsumerGroupsOptions())
                                            .all())
                            .stream()
                            .map(this::summary)
                            .toList();
                });
    }

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
                                            .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())))
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
                            String.valueOf(description.state()),
                            String.valueOf(description.type()),
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

    private ConsumerGroupSummaryResponse summary(ConsumerGroupListing listing) {
        return new ConsumerGroupSummaryResponse(
                listing.groupId(),
                String.valueOf(listing.state().orElse(null)),
                String.valueOf(listing.type().orElse(null)),
                0,
                0);
    }

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
