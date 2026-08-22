package com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class MetricModelsTest {

    @Test
    void identityNormalizationAndEquality() {
        UUID cid = UUID.randomUUID();
        MetricIdentity a = new MetricIdentity(cid, 1, "TopicA", "My.Metric");
        MetricIdentity b = new MetricIdentity(cid, 1, "topica", "my.metric");
        assertThat(a).isEqualTo(b);
    }

    @Test
    void sampleRejectsNaNAndInfinite() {
        UUID cid = UUID.randomUUID();
        MetricIdentity id = new MetricIdentity(cid, null, null, "m");
        Instant now = Instant.now();
        assertThatThrownBy(() -> new MetricSample(
                        id, Double.NaN, now, MetricSemanticType.GAUGE, "count", MetricSourceBackend.ADMIN_CLIENT, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MetricSample(
                        id,
                        Double.POSITIVE_INFINITY,
                        now,
                        MetricSemanticType.GAUGE,
                        "count",
                        MetricSourceBackend.ADMIN_CLIENT,
                        null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
