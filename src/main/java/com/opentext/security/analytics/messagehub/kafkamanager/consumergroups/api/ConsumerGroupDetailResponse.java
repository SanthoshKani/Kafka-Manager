package com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api;

import java.util.List;

public record ConsumerGroupDetailResponse(
        String groupId,
        String state,
        String type,
        String coordinator,
        List<MemberResponse> members,
        List<OffsetLagResponse> offsets,
        long totalLag,
        List<String> diagnostics) {}
