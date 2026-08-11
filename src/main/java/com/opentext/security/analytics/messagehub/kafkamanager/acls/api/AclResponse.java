package com.opentext.security.analytics.messagehub.kafkamanager.acls.api;

public record AclResponse(
        String resourceType,
        String resourceName,
        String patternType,
        String principal,
        String host,
        String operation,
        String permissionType) {}
