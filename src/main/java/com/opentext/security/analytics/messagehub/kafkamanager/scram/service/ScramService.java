package com.opentext.security.analytics.messagehub.kafkamanager.scram.service;

import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.service.AdminMutationRecorder;
import com.opentext.security.analytics.messagehub.kafkamanager.scram.api.ScramCredentialDeleteRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.scram.api.ScramCredentialResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.scram.api.ScramCredentialUpsertRequest;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.admin.*;
import org.springframework.stereotype.Service;

/**
 * Service to manage SCRAM credentials for Kafka users.
 *
 * <p>Supports describing existing SCRAM credentials and creating/updating or deleting credentials
 * via AdminClient operations. Mutations are recorded for auditability.
 */
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

    /**
     * Retrieve SCRAM credential info for one or more users.
     *
     * @param clusterId the target Kafka cluster id
     * @param userNames list of user names to describe
     * @return list of {@link ScramCredentialResponse} describing mechanism and iterations
     */
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

    /**
     * Create or update SCRAM credentials for a user.
     *
     * @param clusterId the target Kafka cluster id
     * @param userName the user name for which to upsert credentials
     * @param request upsert request containing mechanism, iterations and the password
     */
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
                                                            parseScramMechanism(request.mechanism()),
                                                            request.iterations()),
                                                    request.password())))
                                            .all());
                            return null;
                        }));
    }

    /**
     * Delete SCRAM credentials for a user for the specified mechanism.
     *
     * @param clusterId the target Kafka cluster id
     * @param userName the user name whose credential will be removed
     * @param request deletion request specifying mechanism to remove
     */
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
                                                    userName, parseScramMechanism(request.mechanism()))))
                                            .all());
                            return null;
                        }));
    }

    private ScramMechanism parseScramMechanism(String name) {
        if (name == null || name.isBlank()) {
            throw new com.opentext.security.analytics.messagehub.kafkamanager.common.ApiException(
                    org.springframework.http.HttpStatus.BAD_REQUEST,
                    com.opentext.security.analytics.messagehub.kafkamanager.common.ApiErrorCode.VALIDATION_ERROR,
                    "SCRAM mechanism must not be blank");
        }
        try {
            return ScramMechanism.fromMechanismName(name.replace('_', '-').toUpperCase(java.util.Locale.ROOT));
        } catch (Exception e) {
            try {
                return ScramMechanism.valueOf(name.replace('-', '_').toUpperCase(java.util.Locale.ROOT));
            } catch (Exception ex) {
                throw new com.opentext.security.analytics.messagehub.kafkamanager.common.ApiException(
                        org.springframework.http.HttpStatus.BAD_REQUEST,
                        com.opentext.security.analytics.messagehub.kafkamanager.common.ApiErrorCode.VALIDATION_ERROR,
                        "Invalid SCRAM mechanism: " + name);
            }
        }
    }
}
