package com.opentext.security.analytics.messagehub.kafkamanager.operations.domain;

public enum OperationState {
    PENDING,
    VALIDATING,
    WAITING_FOR_APPROVAL,
    SCHEDULED,
    RUNNING,
    SUCCEEDED,
    PARTIALLY_SUCCEEDED,
    FAILED,
    CANCELLATION_REQUESTED,
    CANCELLED,
    EXPIRED
}
