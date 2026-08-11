package com.opentext.security.analytics.messagehub.kafkamanager.operations.domain;

public enum OperationType {
    CLUSTER_ENABLE,
    CLUSTER_DISABLE,
    CLUSTER_DELETE,
    CLUSTER_VALIDATE,
    TOPIC_CREATE,
    TOPIC_DELETE,
    TOPIC_ADD_PARTITIONS,
    BROKER_CONFIG_ALTER,
    REASSIGN_PARTITIONS,
    PREFERRED_LEADER_ELECTION
}
