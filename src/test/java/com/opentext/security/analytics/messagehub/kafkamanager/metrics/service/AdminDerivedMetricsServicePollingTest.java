package com.opentext.security.analytics.messagehub.kafkamanager.metrics.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.BrokerLeaderCount;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AdminDerivedMetricsServicePollingTest {

    @Test
    void refreshSavesSuccessAndRecordsFailurePreservingValuesAndPreventsOverlap() throws Exception {
        AdminDerivedMetricsCollector collector = Mockito.mock(AdminDerivedMetricsCollector.class);
        InMemoryAdminDerivedMetricsStore store = new InMemoryAdminDerivedMetricsStore();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        // Prepare successful snapshot
        UUID clusterId = UUID.nameUUIDFromBytes("kafka-manager-admin-derived-metrics".getBytes(StandardCharsets.UTF_8));
        Instant t0 = Instant.parse("2026-08-22T00:00:00Z");
        AdminClientMetricsSnapshot success = new AdminClientMetricsSnapshot(
                clusterId,
                t0,
                1,
                1,
                1,
                0,
                0,
                0,
                CollectionStatus.SUCCESS,
                t0,
                null,
                List.of(new BrokerLeaderCount(0, 1)));

        Mockito.when(collector.collect(any(UUID.class))).thenReturn(success);

        AdminDerivedMetricsService service = new AdminDerivedMetricsService(collector, registry, store);

        // First refresh: should save success
        service.refresh();
        AdminClientMetricsSnapshot stored = store.getCurrent(clusterId);
        assertThat(stored.collectionStatus()).isEqualTo(CollectionStatus.SUCCESS);
        assertThat(stored.brokerCount()).isEqualTo(1);

        // Now simulate long running collect to test overlap prevention and then failure
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(1);

        Mockito.when(collector.collect(any(UUID.class))).thenAnswer(invocation -> {
            startLatch.countDown();
            // Wait for main thread to try a concurrent refresh
            finishLatch.await(3, TimeUnit.SECONDS);
            throw new RuntimeException("Connection refused");
        });

        // Run refresh in background thread (simulates long running collector)
        Thread background = new Thread(service::refresh);
        background.start();

        // Wait until collector started
        assertThat(startLatch.await(2, TimeUnit.SECONDS)).isTrue();

        // Now call refresh again concurrently - it should return quickly because inProgress prevents overlap
        service.refresh(); // second call should detect in-progress and return

        // Allow background to finish
        finishLatch.countDown();
        background.join(3000);

        // After failure, store should have FAILURE but preserve prior successful numeric values
        AdminClientMetricsSnapshot after = store.getCurrent(clusterId);
        assertThat(after.collectionStatus()).isEqualTo(CollectionStatus.FAILURE);
        assertThat(after.brokerCount()).isEqualTo(1);
        assertThat(after.sanitizedFailureReason()).contains("Connection refused");
    }
}
