package com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime;

import java.time.Instant;
import java.util.List;

/**
 * API for a bounded runtime metric sample store.
 */
public interface RuntimeMetricSampleStore {

    /** Append a sample. Returns true when the sample was accepted; false when rejected due to capacity limits. */
    boolean append(MetricSample sample);

    /** Return the latest sample for the identity, or null if none. */
    MetricSample latest(MetricIdentity identity);

    /** Return the earliest sample at or before the boundary, or null if none. */
    MetricSample earliestAtOrBefore(MetricIdentity identity, Instant boundary);

    /** Return samples in time range [fromInclusive, toInclusive]. */
    List<MetricSample> samplesInRange(MetricIdentity identity, Instant fromInclusive, Instant toInclusive);

    /** Remove all series for a broker (clusterId + brokerId). */
    void removeBroker(java.util.UUID clusterId, Integer brokerId);

    /** Remove all series for a cluster. */
    void removeCluster(java.util.UUID clusterId);

    /** Return configured maximum series allowed. */
    int maxSeriesCount();
}
