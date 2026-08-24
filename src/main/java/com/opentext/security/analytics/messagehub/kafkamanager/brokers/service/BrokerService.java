package com.opentext.security.analytics.messagehub.kafkamanager.brokers.service;

import com.opentext.security.analytics.messagehub.kafkamanager.brokers.api.BrokerConfigMutationRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.brokers.api.BrokerSummaryResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.service.AdminMutationRecorder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;

/**
 * Service that exposes broker-level information and configuration management.
 *
 * <p>Includes listing brokers, describing broker configs and applying incremental config mutations.
 */
@Service
public class BrokerService {

    private final KafkaAdminExecutionService adminExecutionService;
    private final AdminMutationRecorder mutationRecorder;
    private final KafkaManagerProperties properties;

    public BrokerService(
            KafkaAdminExecutionService adminExecutionService,
            AdminMutationRecorder mutationRecorder,
            KafkaManagerProperties properties) {
        this.adminExecutionService = adminExecutionService;
        this.mutationRecorder = mutationRecorder;
        this.properties = properties;
    }

    /**
     * List all brokers in the cluster with basic summary information.
     *
     * @param clusterId the target Kafka cluster id
     * @return list of {@link BrokerSummaryResponse}
     */
    public List<BrokerSummaryResponse> list(UUID clusterId) {
        return adminExecutionService.execute(
                clusterId, "list-brokers", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    var describe = admin.describeCluster()
                            .nodes()
                            .toCompletionStage()
                            .toCompletableFuture()
                            .join();
                    Node controller = admin.describeCluster()
                            .controller()
                            .toCompletionStage()
                            .toCompletableFuture()
                            .join();
                    return describe.stream()
                            .map(node -> summary(node, controller))
                            .toList();
                });
    }

    /**
     * Describe broker configuration entries as a map of name -> value.
     *
     * @param clusterId the target Kafka cluster id
     * @param brokerId the broker id to describe
     * @return ordered map of configuration entries for the broker
     */
    public Map<String, String> describeConfigs(UUID clusterId, int brokerId) {
        return adminExecutionService.execute(
                clusterId, "describe-broker-configs", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
                    Config config = adminExecutionService
                            .await(
                                    clusterId,
                                    "describe-broker-configs",
                                    properties.admin().defaultRequestTimeout(),
                                    admin.describeConfigs(List.of(resource)).all())
                            .get(resource);
                    Map<String, String> values = new LinkedHashMap<>();
                    for (ConfigEntry entry : config.entries()) {
                        values.put(entry.name(), entry.value());
                    }
                    return values;
                });
    }

    /**
     * Alter broker configuration using incremental alter-config operations.
     *
     * @param clusterId the target Kafka cluster id
     * @param brokerId the broker id to update
     * @param request the broker config mutation request
     */
    public void alterConfigs(UUID clusterId, int brokerId, BrokerConfigMutationRequest request) {
        mutationRecorder.record(
                clusterId,
                "alter-broker-configs",
                String.valueOf(brokerId),
                false,
                request,
                () -> adminExecutionService.execute(
                        clusterId, "alter-broker-configs", properties.admin().defaultOperationTimeout(), handle -> {
                            Admin admin = handle.admin();
                            ConfigResource resource =
                                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
                            List<AlterConfigOp> operations = new java.util.ArrayList<>();
                            for (var change : request.changes()) {
                                AlterConfigOp.OpType opType =
                                        switch (change.operation()) {
                                            case SET -> AlterConfigOp.OpType.SET;
                                            case DELETE -> AlterConfigOp.OpType.DELETE;
                                        };
                                operations.add(
                                        new AlterConfigOp(new ConfigEntry(change.name(), change.value()), opType));
                            }
                            adminExecutionService.await(
                                    clusterId,
                                    "alter-broker-configs",
                                    properties.admin().defaultOperationTimeout(),
                                    admin.incrementalAlterConfigs(Map.of(resource, operations))
                                            .all());
                            return null;
                        }));
    }

    private BrokerSummaryResponse summary(Node node, Node controller) {
        return new BrokerSummaryResponse(
                node.id(),
                node.host(),
                node.port(),
                controller != null && controller.id() == node.id(),
                false,
                List.of());
    }
}
