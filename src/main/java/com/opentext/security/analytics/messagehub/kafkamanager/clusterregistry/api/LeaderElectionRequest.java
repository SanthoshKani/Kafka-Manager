package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import com.opentext.security.analytics.messagehub.kafkamanager.common.TopicPartitionRequest;
import jakarta.validation.Valid;
import java.util.List;
import org.apache.kafka.common.ElectionType;

public record LeaderElectionRequest(ElectionType electionType, List<@Valid TopicPartitionRequest> partitions) {}
