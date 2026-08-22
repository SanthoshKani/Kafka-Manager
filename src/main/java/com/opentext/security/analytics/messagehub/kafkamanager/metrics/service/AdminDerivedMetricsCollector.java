package com.opentext.security.analytics.messagehub.kafkamanager.metrics.service;

import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Node;
import org.springframework.stereotype.Service;

/**
 * One-shot AdminClient-backed collector for structural Kafka metrics.
 *
 * <p>This collector resolves broker, controller, and topic metadata through the shared managed
 * AdminClient, filters configured topics before describing them, and delegates the structural
 * derivation to the pure calculator.
 */
@Service
public class AdminDerivedMetricsCollector {

    private static final String ACTION = "collect-admin-derived-structural-metrics";

    private final KafkaAdminExecutionService adminExecutionService;
    private final KafkaManagerProperties properties;
    private final StructuralMetricsCalculator calculator;

    public AdminDerivedMetricsCollector(
            KafkaAdminExecutionService adminExecutionService, KafkaManagerProperties properties) {
        this.adminExecutionService = adminExecutionService;
        this.properties = properties;
        this.calculator = new StructuralMetricsCalculator();
    }

    /**
     * Collect a fresh structural snapshot for the supplied cluster identifier.
     *
     * @param clusterId the cluster identifier used for telemetry tags
     * @return the collected snapshot, or an empty snapshot when admin-derived collection is disabled
     */
    public AdminClientMetricsSnapshot collect(UUID clusterId) {
        Objects.requireNonNull(clusterId, "clusterId");

        KafkaManagerProperties.AdminDerived config = adminDerivedConfig();
        if (config == null || !Boolean.TRUE.equals(config.enabled())) {
            return emptySnapshot(clusterId);
        }

        Duration timeout = config.operationTimeout();
        return adminExecutionService.execute(clusterId, ACTION, timeout, handle -> {
            Admin admin = handle.admin();
            Instant collectedAt = Instant.now();

            var clusterDescription = admin.describeCluster();
            Collection<Node> brokers =
                    adminExecutionService.await(clusterId, ACTION, timeout, clusterDescription.nodes());
            Node controller = adminExecutionService.await(clusterId, ACTION, timeout, clusterDescription.controller());

            Map<String, TopicListing> listings = adminExecutionService.await(
                    clusterId,
                    ACTION,
                    timeout,
                    admin.listTopics(new ListTopicsOptions().listInternal(true)).namesToListings());
            List<String> topicNames = filterTopicNames(listings, config.topicExclusionPatterns());

            Map<String, TopicDescription> descriptions = topicNames.isEmpty()
                    ? Map.of()
                    : adminExecutionService.await(
                            clusterId,
                            ACTION,
                            timeout,
                            admin.describeTopics(topicNames).allTopicNames());

            return calculator.calculate(clusterId, brokers, descriptions.values(), controller, collectedAt);
        });
    }

    private KafkaManagerProperties.AdminDerived adminDerivedConfig() {
        KafkaManagerProperties.Metrics metrics = properties.metrics();
        return metrics == null ? null : metrics.adminDerived();
    }

    private List<String> filterTopicNames(Map<String, TopicListing> listings, List<String> exclusionPatterns) {
        List<Pattern> compiledPatterns = compilePatterns(exclusionPatterns);
        return listings.values().stream()
                .filter(Objects::nonNull)
                .map(TopicListing::name)
                .filter(Objects::nonNull)
                .filter(topicName -> compiledPatterns.stream()
                        .noneMatch(pattern -> pattern.matcher(topicName).matches()))
                .sorted()
                .toList();
    }

    private List<Pattern> compilePatterns(List<String> exclusionPatterns) {
        if (exclusionPatterns == null || exclusionPatterns.isEmpty()) {
            return List.of();
        }
        return exclusionPatterns.stream()
                .filter(pattern -> pattern != null && !pattern.isBlank())
                .map(Pattern::compile)
                .toList();
    }

    private AdminClientMetricsSnapshot emptySnapshot(UUID clusterId) {
        return new AdminClientMetricsSnapshot(
                clusterId, Instant.EPOCH, 0, 0, 0, 0, 0, null, CollectionStatus.UNKNOWN, null, null, List.of());
    }
}
