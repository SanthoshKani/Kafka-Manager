package com.opentext.security.analytics.messagehub.kafkamanager.acls.service;

import com.opentext.security.analytics.messagehub.kafkamanager.acls.api.AclCreateRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.acls.api.AclDeleteRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.acls.api.AclEntryRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.acls.api.AclFilterRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.acls.api.AclResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.service.AdminMutationRecorder;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.stereotype.Service;

@Service
public class AclService {

    private final KafkaAdminExecutionService adminExecutionService;
    private final AdminMutationRecorder mutationRecorder;
    private final KafkaManagerProperties properties;

    public AclService(
            KafkaAdminExecutionService adminExecutionService,
            AdminMutationRecorder mutationRecorder,
            KafkaManagerProperties properties) {
        this.adminExecutionService = adminExecutionService;
        this.mutationRecorder = mutationRecorder;
        this.properties = properties;
    }

    public List<AclResponse> list(UUID clusterId, AclFilterRequest request) {
        return adminExecutionService.execute(
                clusterId, "list-acls", properties.admin().defaultRequestTimeout(), handle -> {
                    Admin admin = handle.admin();
                    AclBindingFilter filter = toFilter(request);
                    return adminExecutionService
                            .await(
                                    clusterId,
                                    "list-acls",
                                    properties.admin().defaultRequestTimeout(),
                                    admin.describeAcls(filter).values())
                            .stream()
                            .map(this::response)
                            .toList();
                });
    }

    public void create(UUID clusterId, AclCreateRequest request) {
        mutationRecorder.record(
                clusterId,
                "create-acls",
                "acls",
                false,
                request,
                () -> adminExecutionService.execute(
                        clusterId, "create-acls", properties.admin().defaultOperationTimeout(), handle -> {
                            Admin admin = handle.admin();
                            List<AclBinding> bindings = request.bindings().stream()
                                    .map(this::binding)
                                    .toList();
                            adminExecutionService.await(
                                    clusterId,
                                    "create-acls",
                                    properties.admin().defaultOperationTimeout(),
                                    admin.createAcls(bindings).all());
                            return null;
                        }));
    }

    public void delete(UUID clusterId, AclDeleteRequest request) {
        mutationRecorder.record(
                clusterId,
                "delete-acls",
                "acls",
                false,
                request,
                () -> adminExecutionService.execute(
                        clusterId, "delete-acls", properties.admin().defaultOperationTimeout(), handle -> {
                            Admin admin = handle.admin();
                            List<AclBindingFilter> filters =
                                    request.filters().stream().map(this::filter).toList();
                            adminExecutionService.await(
                                    clusterId,
                                    "delete-acls",
                                    properties.admin().defaultOperationTimeout(),
                                    admin.deleteAcls(filters).all());
                            return null;
                        }));
    }

    private AclBinding binding(AclEntryRequest request) {
        return new AclBinding(
                new ResourcePattern(
                        ResourceType.fromString(request.resourceType()),
                        request.resourceName(),
                        PatternType.valueOf(request.patternType())),
                new AccessControlEntry(
                        request.principal(),
                        request.host(),
                        AclOperation.valueOf(request.operation()),
                        AclPermissionType.valueOf(request.permissionType())));
    }

    private AclBindingFilter toFilter(AclFilterRequest request) {
        return filter(request);
    }

    private AclBindingFilter filter(AclFilterRequest request) {
        return new AclBindingFilter(
                new ResourcePatternFilter(
                        request.resourceType() == null
                                ? ResourceType.ANY
                                : ResourceType.fromString(request.resourceType()),
                        request.resourceName(),
                        request.patternType() == null ? PatternType.ANY : PatternType.valueOf(request.patternType())),
                new AccessControlEntryFilter(
                        request.principal(),
                        request.host(),
                        request.operation() == null ? AclOperation.ANY : AclOperation.valueOf(request.operation()),
                        request.permissionType() == null
                                ? AclPermissionType.ANY
                                : AclPermissionType.valueOf(request.permissionType())));
    }

    private AclResponse response(AclBinding binding) {
        return new AclResponse(
                binding.pattern().resourceType().name(),
                binding.pattern().name(),
                binding.pattern().patternType().name(),
                binding.entry().principal(),
                binding.entry().host(),
                binding.entry().operation().name(),
                binding.entry().permissionType().name());
    }
}
