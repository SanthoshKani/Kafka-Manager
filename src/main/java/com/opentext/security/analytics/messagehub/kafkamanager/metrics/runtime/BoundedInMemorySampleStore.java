package com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

/**
 * Simple bounded in-memory sample store.
 * - Default sample interval: 10s
 * - Aggregation window: 60s
 * - Retention: 120s
 * Behavior when max series reached: reject new series (append returns false).
 */
public class BoundedInMemorySampleStore implements RuntimeMetricSampleStore {

    private final ConcurrentMap<MetricIdentity, Deque<MetricSample>> map = new ConcurrentHashMap<>();
    private final Duration retention;
    private final int maxSeries;

    public BoundedInMemorySampleStore() {
        this(Duration.ofSeconds(120), 10_000);
    }

    public BoundedInMemorySampleStore(Duration retention, int maxSeries) {
        this.retention = Objects.requireNonNull(retention, "retention");
        if (maxSeries <= 0) throw new IllegalArgumentException("maxSeries must be > 0");
        this.maxSeries = maxSeries;
    }

    @Override
    public boolean append(MetricSample sample) {
        Objects.requireNonNull(sample, "sample");
        MetricIdentity id = sample.identity();
        Deque<MetricSample> deque = map.get(id);
        if (deque == null) {
            // create new series only if capacity allows
            if (map.size() >= maxSeries) {
                return false; // reject new series
            }
            Deque<MetricSample> newDeque = new ConcurrentLinkedDeque<>();
            Deque<MetricSample> prev = map.putIfAbsent(id, newDeque);
            deque = prev == null ? newDeque : prev;
        }

        // Append (concurrent) and trim old samples by retention
        deque.addLast(sample);
        trimDeque(deque);
        return true;
    }

    private void trimDeque(Deque<MetricSample> deque) {
        Instant threshold = Instant.now().minus(retention);
        while (true) {
            MetricSample head = deque.peekFirst();
            if (head == null) break;
            if (head.timestamp().isBefore(threshold)) {
                deque.pollFirst();
            } else {
                break;
            }
        }
    }

    @Override
    public MetricSample latest(MetricIdentity identity) {
        Deque<MetricSample> deque = map.get(identity);
        if (deque == null) return null;
        return deque.peekLast();
    }

    @Override
    public MetricSample earliestAtOrBefore(MetricIdentity identity, Instant boundary) {
        Deque<MetricSample> deque = map.get(identity);
        if (deque == null) return null;
        // Return the latest sample whose timestamp is <= boundary (closest to boundary)
        MetricSample candidate = null;
        for (MetricSample s : deque) {
            if (!s.timestamp().isAfter(boundary)) {
                candidate = s;
            } else {
                break; // deque timestamps are in ascending order
            }
        }
        return candidate;
    }

    @Override
    public List<MetricSample> samplesInRange(MetricIdentity identity, Instant fromInclusive, Instant toInclusive) {
        Deque<MetricSample> deque = map.get(identity);
        if (deque == null) return List.of();
        List<MetricSample> out = new ArrayList<>();
        for (MetricSample s : deque) {
            if ((s.timestamp().equals(fromInclusive) || s.timestamp().isAfter(fromInclusive))
                    && (s.timestamp().equals(toInclusive) || s.timestamp().isBefore(toInclusive))) {
                out.add(s);
            }
        }
        return Collections.unmodifiableList(out);
    }

    @Override
    public void removeBroker(java.util.UUID clusterId, Integer brokerId) {
        if (brokerId == null) return;
        // Remove series where identity.clusterId == clusterId && identity.brokerId == brokerId
        for (Iterator<MetricIdentity> it = map.keySet().iterator(); it.hasNext(); ) {
            MetricIdentity id = it.next();
            if (id.clusterId().equals(clusterId) && brokerId.equals(id.brokerId())) {
                it.remove();
            }
        }
    }

    @Override
    public void removeCluster(java.util.UUID clusterId) {
        for (Iterator<MetricIdentity> it = map.keySet().iterator(); it.hasNext(); ) {
            MetricIdentity id = it.next();
            if (id.clusterId().equals(clusterId)) {
                it.remove();
            }
        }
    }

    @Override
    public int maxSeriesCount() {
        return maxSeries;
    }
}
