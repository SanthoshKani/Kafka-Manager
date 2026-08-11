package com.opentext.security.analytics.messagehub.kafkamanager.clientmetrics.service;

import com.opentext.security.analytics.messagehub.kafkamanager.clientmetrics.api.ClientMetricResourceResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.springframework.stereotype.Service;

@Service
public class ClientMetricsService {

    private final KafkaAdminExecutionService adminExecutionService;
    private final KafkaManagerProperties properties;

    public ClientMetricsService(KafkaAdminExecutionService adminExecutionService, KafkaManagerProperties properties) {
        this.adminExecutionService = adminExecutionService;
        this.properties = properties;
    }

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
