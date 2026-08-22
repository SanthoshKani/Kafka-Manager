package com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain;

public record BrokerLeaderCount(int brokerId, int leaderPartitionCount) {

    public BrokerLeaderCount {
        validateNonNegative("brokerId", brokerId);
        validateNonNegative("leaderPartitionCount", leaderPartitionCount);
    }

    private static void validateNonNegative(String fieldName, int value) {
        if (value < 0) {
            throw new IllegalArgumentException(fieldName + " must be greater than or equal to zero");
        }
    }
}
