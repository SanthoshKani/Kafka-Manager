package com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime;

/**
 * Simple mutable double holder used as a gauge backing object.
 */
public final class DoubleHolder {
    private volatile double value;

    public DoubleHolder() {
        this.value = 0.0;
    }

    public DoubleHolder(double v) {
        this.value = v;
    }

    public double get() {
        return value;
    }

    public void set(double v) {
        this.value = v;
    }
}
