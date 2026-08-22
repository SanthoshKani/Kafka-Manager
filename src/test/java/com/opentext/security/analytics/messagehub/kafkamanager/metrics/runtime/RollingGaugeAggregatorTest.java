package com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RollingGaugeAggregatorTest {

    @Test
    void computesTimeWeightedAverageAndStats() {
        UUID cid = UUID.randomUUID();
        MetricIdentity id = new MetricIdentity(cid, null, null, "cpu_idle");
        Instant now = Instant.now();
        // window 60s
        RollingGaugeAggregator agg = new RollingGaugeAggregator(Duration.ofSeconds(60));
        // sample before window start
        MetricSample s0 = new MetricSample(
                id, 10.0, now.minusSeconds(70), MetricSemanticType.GAUGE, "%", MetricSourceBackend.PROMETHEUS, null);
        // sample at -50s
        MetricSample s1 = new MetricSample(
                id, 20.0, now.minusSeconds(50), MetricSemanticType.GAUGE, "%", MetricSourceBackend.PROMETHEUS, null);
        // sample at -10s
        MetricSample s2 = new MetricSample(
                id, 40.0, now.minusSeconds(10), MetricSemanticType.GAUGE, "%", MetricSourceBackend.PROMETHEUS, null);
        var result = agg.aggregate(List.of(s0, s1, s2), now);
        assertThat(result).isInstanceOf(RollingGaugeAggregator.AggregationResult.Valid.class);
        var v = (RollingGaugeAggregator.AggregationResult.Valid) result;
        // weighted average: from -60 to -50 -> value 10 for 10s; -50 to -10 -> 20 for 40s; -10 to 0 -> 40 for 10s
        double expected = (10 * 10 + 20 * 40 + 40 * 10) / 60.0;
        assertThat(v.average()).isEqualTo(expected);
        assertThat(v.min()).isEqualTo(10.0);
        assertThat(v.max()).isEqualTo(40.0);
        assertThat(v.count()).isEqualTo(3);
    }

    @Test
    void insufficientWhenNoSampleBeforeWindow() {
        UUID cid = UUID.randomUUID();
        MetricIdentity id = new MetricIdentity(cid, null, null, "m");
        Instant now = Instant.now();
        RollingGaugeAggregator agg = new RollingGaugeAggregator(Duration.ofSeconds(60));
        // first sample at -30s only (no sample at or before -60s)
        MetricSample s1 = new MetricSample(
                id, 10.0, now.minusSeconds(30), MetricSemanticType.GAUGE, "u", MetricSourceBackend.PROMETHEUS, null);
        var result = agg.aggregate(List.of(s1), now);
        assertThat(result).isInstanceOf(RollingGaugeAggregator.AggregationResult.InsufficientData.class);
    }

    @Test
    void invalidWhenNonFinite() {
        UUID cid = UUID.randomUUID();
        MetricIdentity id = new MetricIdentity(cid, null, null, "m");
        Instant now = Instant.now();
        RollingGaugeAggregator agg = new RollingGaugeAggregator(Duration.ofSeconds(60));
        MetricSample s0 = new MetricSample(
                id, 10.0, now.minusSeconds(70), MetricSemanticType.GAUGE, "u", MetricSourceBackend.PROMETHEUS, null);
        // constructing a sample with NaN is invalid and should throw at creation time
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new MetricSample(
                        id,
                        Double.NaN,
                        now.minusSeconds(10),
                        MetricSemanticType.GAUGE,
                        "u",
                        MetricSourceBackend.PROMETHEUS,
                        null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
