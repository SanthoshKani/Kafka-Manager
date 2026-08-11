package com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.service;

import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.api.DelegationTokenCreateRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.api.DelegationTokenExpireRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.api.DelegationTokenRenewRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.api.DelegationTokenResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.service.AdminMutationRecorder;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.DescribeDelegationTokenResult;
import org.apache.kafka.clients.admin.ExpireDelegationTokenOptions;
import org.apache.kafka.clients.admin.RenewDelegationTokenOptions;
import org.springframework.stereotype.Service;

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
