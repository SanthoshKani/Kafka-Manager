package com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RateCalculatorTest {

    @ParameterizedTest
    @CsvSource({
        "10, 100, 110, 60, 60", // 10s interval, delta 10 -> per-minute = 10/10*60 = 60
    })
    void normalInterval(
            long elapsedSeconds, double prevValue, double currValue, double expectedRatePerMinute, long dummy) {
        UUID cid = UUID.randomUUID();
        MetricIdentity id = new MetricIdentity(cid, 1, null, "c");
        Instant now = Instant.now();
        Instant prevTs = now.minusSeconds(elapsedSeconds);
        MetricSample prev = new MetricSample(
                id,
                prevValue,
                prevTs,
                MetricSemanticType.MONOTONIC_COUNTER,
                "count",
                MetricSourceBackend.ADMIN_CLIENT,
                null);
        MetricSample curr = new MetricSample(
                id,
                currValue,
                now,
                MetricSemanticType.MONOTONIC_COUNTER,
                "count",
                MetricSourceBackend.ADMIN_CLIENT,
                null);
        RateCalculator calc = new RateCalculator(0);
        var result = calc.calculate(prev, curr);
        assertThat(result).isInstanceOf(RateCalculator.RateResult.ValidRate.class);
        RateCalculator.RateResult.ValidRate vr = (RateCalculator.RateResult.ValidRate) result;
        assertThat(vr.ratePerMinute()).isEqualTo(expectedRatePerMinute);
    }

    @ParameterizedTest
    @CsvSource({
        "10, 100, 100", // no change -> rate 0
        "5, 0, 1000" // large delta
    })
    void variousCases(long elapsedSeconds, double prevValue, double currValue) {
        UUID cid = UUID.randomUUID();
        MetricIdentity id = new MetricIdentity(cid, 1, null, "c");
        Instant now = Instant.now();
        Instant prevTs = now.minusSeconds(elapsedSeconds);
        MetricSample prev = new MetricSample(
                id,
                prevValue,
                prevTs,
                MetricSemanticType.MONOTONIC_COUNTER,
                "count",
                MetricSourceBackend.ADMIN_CLIENT,
                null);
        MetricSample curr = new MetricSample(
                id,
                currValue,
                now,
                MetricSemanticType.MONOTONIC_COUNTER,
                "count",
                MetricSourceBackend.ADMIN_CLIENT,
                null);
        RateCalculator calc = new RateCalculator(0);
        var result = calc.calculate(prev, curr);
        if (currValue < prevValue) {
            assertThat(result).isInstanceOf(RateCalculator.RateResult.CounterReset.class);
        } else {
            assertThat(result).isInstanceOf(RateCalculator.RateResult.ValidRate.class);
        }
    }
}
