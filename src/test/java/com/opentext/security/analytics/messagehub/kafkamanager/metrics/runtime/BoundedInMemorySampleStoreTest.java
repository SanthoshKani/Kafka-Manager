package com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class BoundedInMemorySampleStoreTest {

    @Test
    void appendAndRetrieve() {
        BoundedInMemorySampleStore store = new BoundedInMemorySampleStore();
        UUID cid = UUID.randomUUID();
        MetricIdentity id = new MetricIdentity(cid, 1, null, "m1");
        Instant now = Instant.now();
        MetricSample s1 = new MetricSample(
                id,
                1.0,
                now.minusSeconds(30),
                MetricSemanticType.GAUGE,
                "count",
                MetricSourceBackend.ADMIN_CLIENT,
                null);
        MetricSample s2 = new MetricSample(
                id,
                2.0,
                now.minusSeconds(20),
                MetricSemanticType.GAUGE,
                "count",
                MetricSourceBackend.ADMIN_CLIENT,
                null);
        MetricSample s3 = new MetricSample(
                id,
                3.0,
                now.minusSeconds(10),
                MetricSemanticType.GAUGE,
                "count",
                MetricSourceBackend.ADMIN_CLIENT,
                null);
        assertThat(store.append(s1)).isTrue();
        assertThat(store.append(s2)).isTrue();
        assertThat(store.append(s3)).isTrue();
        assertThat(store.latest(id)).isEqualTo(s3);
        List<MetricSample> inRange = store.samplesInRange(id, now.minusSeconds(25), now.minusSeconds(5));
        assertThat(inRange).containsExactly(s2, s3);
        MetricSample earliest = store.earliestAtOrBefore(id, now.minusSeconds(15));
        assertThat(earliest).isEqualTo(s2);
    }

    @Test
    void capacityRejectsNewSeries() {
        BoundedInMemorySampleStore store = new BoundedInMemorySampleStore(java.time.Duration.ofMinutes(2), 1);
        UUID cid = UUID.randomUUID();
        MetricIdentity id1 = new MetricIdentity(cid, 1, null, "m1");
        MetricIdentity id2 = new MetricIdentity(cid, 2, null, "m2");
        Instant now = Instant.now();
        MetricSample s1 = new MetricSample(
                id1, 1.0, now, MetricSemanticType.GAUGE, "count", MetricSourceBackend.ADMIN_CLIENT, null);
        MetricSample s2 = new MetricSample(
                id2, 2.0, now, MetricSemanticType.GAUGE, "count", MetricSourceBackend.ADMIN_CLIENT, null);
        assertThat(store.append(s1)).isTrue();
        assertThat(store.append(s2)).isFalse();
    }
}
