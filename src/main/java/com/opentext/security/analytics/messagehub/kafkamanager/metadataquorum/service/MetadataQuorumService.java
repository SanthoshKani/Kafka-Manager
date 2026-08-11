package com.opentext.security.analytics.messagehub.kafkamanager.metadataquorum.service;

import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.metadataquorum.api.MetadataQuorumObserverResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.metadataquorum.api.MetadataQuorumResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.metadataquorum.api.MetadataQuorumVoterResponse;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.springframework.stereotype.Service;

@Service
public class MetadataQuorumService {

    private final KafkaAdminExecutionService adminExecutionService;
    private final KafkaManagerProperties properties;

    public MetadataQuorumService(KafkaAdminExecutionService adminExecutionService, KafkaManagerProperties properties) {
        this.adminExecutionService = adminExecutionService;
        this.properties = properties;
    }

    public MetadataQuorumResponse get(UUID clusterId) {
        return adminExecutionService.execute(
                clusterId, "describe-metadata-quorum", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    var info = adminExecutionService.await(
                            clusterId,
                            "describe-metadata-quorum",
                            properties.admin().defaultRequestTimeout(),
                            admin.describeMetadataQuorum().quorumInfo());
                    return new MetadataQuorumResponse(
                            info.leaderId(),
                            info.leaderEpoch(),
                            info.highWatermark(),
                            info.voters().stream()
                                    .map(voter -> new MetadataQuorumVoterResponse(
                                            voter.replicaId(),
                                            voter.logEndOffset(),
                                            voter.lastFetchTimestamp().orElse(-1)))
                                    .toList(),
                            info.observers().stream()
                                    .map(observer -> new MetadataQuorumObserverResponse(
                                            observer.replicaId(), observer.logEndOffset()))
                                    .toList());
                });
    }
}
