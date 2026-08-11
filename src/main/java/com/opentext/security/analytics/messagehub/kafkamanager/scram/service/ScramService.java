package com.opentext.security.analytics.messagehub.kafkamanager.scram.service;

import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.service.AdminMutationRecorder;
import com.opentext.security.analytics.messagehub.kafkamanager.scram.api.ScramCredentialDeleteRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.scram.api.ScramCredentialResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.scram.api.ScramCredentialUpsertRequest;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.springframework.stereotype.Service;

@Service
public class ScramService {

    private final KafkaAdminExecutionService adminExecutionService;
    private final AdminMutationRecorder mutationRecorder;
    private final KafkaManagerProperties properties;

    public ScramService(
            KafkaAdminExecutionService adminExecutionService,
            AdminMutationRecorder mutationRecorder,
            KafkaManagerProperties properties) {
        this.adminExecutionService = adminExecutionService;
        this.mutationRecorder = mutationRecorder;
        this.properties = properties;
    }

    public List<ScramCredentialResponse> describe(UUID clusterId, List<String> userNames) {
        return adminExecutionService.execute(
                clusterId, "describe-scram-users", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    DescribeUserScramCredentialsResult result = admin.describeUserScramCredentials(userNames);
                    return adminExecutionService
                            .await(
                                    clusterId,
                                    "describe-scram-users",
                                    properties.admin().defaultRequestTimeout(),
                                    result.all())
                            .entrySet()
                            .stream()
                            .flatMap(entry -> entry.getValue().credentialInfos().stream()
                                    .map(info -> new ScramCredentialResponse(
                                            entry.getKey(), info.mechanism().mechanismName(), info.iterations())))
                            .toList();
                });
    }

    public void upsert(UUID clusterId, String userName, ScramCredentialUpsertRequest request) {
        mutationRecorder.record(
                clusterId,
                "upsert-scram-credentials",
                userName,
                false,
                request,
                () -> adminExecutionService.execute(
                        clusterId,
                        "upsert-scram-credentials",
                        properties.admin().defaultOperationTimeout(),
                        handle -> {
                            Admin admin = handle.admin();
                            adminExecutionService.await(
                                    clusterId,
                                    "upsert-scram-credentials",
                                    properties.admin().defaultOperationTimeout(),
                                    admin.alterUserScramCredentials(List.of(new UserScramCredentialUpsertion(
                                                    userName,
                                                    new ScramCredentialInfo(
                                                            ScramMechanism.valueOf(request.mechanism()),
                                                            request.iterations()),
                                                    request.password())))
                                            .all());
                            return null;
                        }));
    }

    public void delete(UUID clusterId, String userName, ScramCredentialDeleteRequest request) {
        mutationRecorder.record(
                clusterId,
                "delete-scram-credentials",
                userName,
                false,
                request,
                () -> adminExecutionService.execute(
                        clusterId,
                        "delete-scram-credentials",
                        properties.admin().defaultOperationTimeout(),
                        handle -> {
                            Admin admin = handle.admin();
                            adminExecutionService.await(
                                    clusterId,
                                    "delete-scram-credentials",
                                    properties.admin().defaultOperationTimeout(),
                                    admin.alterUserScramCredentials(List.of(new UserScramCredentialDeletion(
                                                    userName, ScramMechanism.valueOf(request.mechanism()))))
                                            .all());
                            return null;
                        }));
    }
}
