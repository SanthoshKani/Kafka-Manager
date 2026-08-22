package com.opentext.security.analytics.messagehub.kafkamanager.clientmetrics.service;

import com.opentext.security.analytics.messagehub.kafkamanager.clientmetrics.api.ClientMetricResourceResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import org.apache.kafka.clients.admin.Admin;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Service that exposes available client metrics resources from Kafka brokers.
 *
 * <p>Returns a list of named client metric resources which can be used by callers to fetch
 * metric details or present available metrics to users.
 */
@Service
@SuppressWarnings({"deprecation", "removal"})
public class ClientMetricsService {

    private final KafkaAdminExecutionService adminExecutionService;
    private final KafkaManagerProperties properties;

    public ClientMetricsService(KafkaAdminExecutionService adminExecutionService, KafkaManagerProperties properties) {
        this.adminExecutionService = adminExecutionService;
        this.properties = properties;
    }

    /**
     * List all available client metric resources for the cluster.
     *
     * @param clusterId the target Kafka cluster id
     * @return list of {@link ClientMetricResourceResponse} representing available resources
     */
    public java.util.List<ClientMetricResourceResponse> list(UUID clusterId) {
        return adminExecutionService.execute(
                clusterId, "list-client-metrics", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    return adminExecutionService
                            .await(
                                    clusterId,
                                    "list-client-metrics",
                                    properties.admin().defaultRequestTimeout(),
                                    admin.listClientMetricsResources().all())
                            .stream()
                            .map(resource -> new ClientMetricResourceResponse(resource.name()))
                            .toList();
                });
    }
}
