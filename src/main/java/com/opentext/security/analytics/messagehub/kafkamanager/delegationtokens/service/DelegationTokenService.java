package com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.service;

import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.api.DelegationTokenCreateRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.api.DelegationTokenExpireRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.api.DelegationTokenRenewRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.api.DelegationTokenResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.service.AdminMutationRecorder;
import org.apache.kafka.clients.admin.*;
import org.springframework.stereotype.Service;

import java.util.Base64;
import java.util.List;
import java.util.UUID;

/**
 * Service managing Kafka delegation tokens: list, create, renew and expire operations.
 *
 * <p>Delegation tokens are created and managed via AdminClient calls. The service returns
 * token identifiers and encodes/decodes HMACs where appropriate.
 */
@Service
public class DelegationTokenService {

    private final KafkaAdminExecutionService adminExecutionService;
    private final AdminMutationRecorder mutationRecorder;
    private final KafkaManagerProperties properties;

    public DelegationTokenService(
            KafkaAdminExecutionService adminExecutionService,
            AdminMutationRecorder mutationRecorder,
            KafkaManagerProperties properties) {
        this.adminExecutionService = adminExecutionService;
        this.mutationRecorder = mutationRecorder;
        this.properties = properties;
    }

    /**
     * List all delegation tokens known to the cluster.
     *
     * @param clusterId the target Kafka cluster id
     * @return list of {@link DelegationTokenResponse}
     */
    public List<DelegationTokenResponse> list(UUID clusterId) {
        return adminExecutionService.execute(
                clusterId, "list-delegation-tokens", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    DescribeDelegationTokenResult result = admin.describeDelegationToken();
                    return adminExecutionService
                            .await(
                                    clusterId,
                                    "list-delegation-tokens",
                                    properties.admin().defaultRequestTimeout(),
                                    result.delegationTokens())
                            .stream()
                            .map(token -> new DelegationTokenResponse(
                                    token.tokenInfo().tokenId(),
                                    token.tokenInfo().owner().getName(),
                                    token.tokenInfo().expiryTimestamp(),
                                    Base64.getEncoder().encodeToString(token.hmac())))
                            .toList();
                });
    }

    /**
     * Create a new delegation token with the requested maximum lifetime.
     *
     * @param clusterId the target Kafka cluster id
     * @param request creation request containing max lifetime
     * @return {@link DelegationTokenResponse} representing the created token
     */
    public DelegationTokenResponse create(UUID clusterId, DelegationTokenCreateRequest request) {
        return mutationRecorder.record(
                clusterId,
                "create-delegation-token",
                "delegation-tokens",
                false,
                request,
                () -> adminExecutionService.execute(
                        clusterId, "create-delegation-token", properties.admin().defaultOperationTimeout(), handle -> {
                            Admin admin = handle.admin();
                            var token = adminExecutionService.await(
                                    clusterId,
                                    "create-delegation-token",
                                    properties.admin().defaultOperationTimeout(),
                                    admin.createDelegationToken(new CreateDelegationTokenOptions()
                                                    .maxlifeTimeMs(request.maxLifeTimeMs()))
                                            .delegationToken());
                            return new DelegationTokenResponse(
                                    token.tokenInfo().tokenId(),
                                    token.tokenInfo().owner().getName(),
                                    token.tokenInfo().expiryTimestamp(),
                                    Base64.getEncoder().encodeToString(token.hmac()));
                        }));
    }

    /**
     * Renew an existing delegation token using its HMAC (base64).
     *
     * @param clusterId the target Kafka cluster id
     * @param tokenId token id to renew
     * @param request renewal request containing HMAC base64
     * @return updated {@link DelegationTokenResponse} with new expiry
     */
    public DelegationTokenResponse renew(UUID clusterId, String tokenId, DelegationTokenRenewRequest request) {
        return mutationRecorder.record(
                clusterId,
                "renew-delegation-token",
                tokenId,
                false,
                request,
                () -> adminExecutionService.execute(
                        clusterId, "renew-delegation-token", properties.admin().defaultOperationTimeout(), handle -> {
                            Admin admin = handle.admin();
                            long expiry = adminExecutionService.await(
                                    clusterId,
                                    "renew-delegation-token",
                                    properties.admin().defaultOperationTimeout(),
                                    admin.renewDelegationToken(
                                                    Base64.getDecoder().decode(request.hmacBase64()),
                                                    new RenewDelegationTokenOptions())
                                            .expiryTimestamp());
                            return new DelegationTokenResponse(tokenId, null, expiry, request.hmacBase64());
                        }));
    }

    /**
     * Expire (revoke) a delegation token immediately or after a specified period.
     *
     * @param clusterId the target Kafka cluster id
     * @param tokenId token id to expire
     * @param request expiration request containing HMAC base64 and expiry time period
     */
    public void expire(UUID clusterId, String tokenId, DelegationTokenExpireRequest request) {
        mutationRecorder.record(
                clusterId,
                "expire-delegation-token",
                tokenId,
                false,
                request,
                () -> adminExecutionService.execute(
                        clusterId, "expire-delegation-token", properties.admin().defaultOperationTimeout(), handle -> {
                            Admin admin = handle.admin();
                            adminExecutionService.await(
                                    clusterId,
                                    "expire-delegation-token",
                                    properties.admin().defaultOperationTimeout(),
                                    admin.expireDelegationToken(
                                                    Base64.getDecoder().decode(request.hmacBase64()),
                                                    new ExpireDelegationTokenOptions()
                                                            .expiryTimePeriodMs(request.expiryTimePeriodMs()))
                                            .expiryTimestamp());
                            return null;
                        }));
    }
}
