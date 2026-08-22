package com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime;

/**
 * Source backend for a runtime metric sample.
 */
public enum MetricSourceBackend {
    ADMIN_CLIENT,
    JMX,
    PROMETHEUS,
    OTHER
}
