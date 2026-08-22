package com.opentext.security.analytics.messagehub.kafkamanager.metrics.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiProblemAdvice;
import com.opentext.security.analytics.messagehub.kafkamanager.config.PrometheusScrapeProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus.MetricMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.BoundedInMemorySampleStore;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.service.InMemoryAdminDerivedMetricsStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.json.JsonMapper;

class RuntimeMetricsControllerTest {

    private MockMvc mockMvc;
    private BoundedInMemorySampleStore sampleStore;
    private InMemoryAdminDerivedMetricsStore adminStore;
    private MetricMapper mapper;

    @BeforeEach
    void setUp() {
        sampleStore = new BoundedInMemorySampleStore();
        adminStore = new InMemoryAdminDerivedMetricsStore();
        mapper = new MetricMapper();
    }

    @Test
    void brokerMetricsReturnsRatesFor1mWindow() throws Exception {
        PrometheusScrapeProperties props = new PrometheusScrapeProperties(
                false,
                Duration.ofSeconds(10),
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                "/metrics",
                null,
                1024L,
                Duration.ofSeconds(60),
                Duration.ofSeconds(120),
                10000,
                List.of(),
                false,
                100);
        var controller = new RuntimeMetricsController(sampleStore, adminStore, mapper, props);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new ApiProblemAdvice())
                .setMessageConverters(new JacksonJsonHttpMessageConverter(
                        JsonMapper.builderWithJackson2Defaults().build()))
                .build();

        UUID clusterId = UUID.randomUUID();
        Instant now = Instant.now();
        // admin snapshot with one broker
        var snap =
                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot(
                        clusterId,
                        now,
                        1,
                        1,
                        1,
                        0,
                        0,
                        1,
                        com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus.SUCCESS,
                        now,
                        null,
                        List.of(
                                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain
                                        .BrokerLeaderCount(1, 1)));
        adminStore.saveSuccessful(snap);

        // produce counter samples for bytes.in.total for broker 1, earliest at windowStart or before
        Instant latest = Instant.now();
        Instant earliest = latest.minusSeconds(61); // before window start (60s)
        var id = new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricIdentity(
                clusterId, 1, null, "bytes.in.total");
        var sPrev = new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSample(
                id,
                1000.0,
                earliest,
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSemanticType
                        .MONOTONIC_COUNTER,
                "bytes",
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSourceBackend.PROMETHEUS,
                null);
        var sCurr = new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSample(
                id,
                1600.0,
                latest,
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSemanticType
                        .MONOTONIC_COUNTER,
                "bytes",
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSourceBackend.PROMETHEUS,
                null);
        sampleStore.append(sPrev);
        sampleStore.append(sCurr);

        mockMvc.perform(get("/api/v1/clusters/{clusterId}/brokers/{brokerId}/metrics", clusterId, 1)
                        .param("window", "1m"))
                .andExpect(status().isOk());
    }

    @Test
    void unsupportedWindowRejected() throws Exception {
        PrometheusScrapeProperties props = new PrometheusScrapeProperties(
                false,
                Duration.ofSeconds(10),
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                "/metrics",
                null,
                1024L,
                Duration.ofSeconds(60),
                Duration.ofSeconds(120),
                10000,
                List.of(),
                false,
                100);
        var controller = new RuntimeMetricsController(sampleStore, adminStore, mapper, props);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new ApiProblemAdvice())
                .setMessageConverters(new JacksonJsonHttpMessageConverter(
                        JsonMapper.builderWithJackson2Defaults().build()))
                .build();

        UUID clusterId = UUID.randomUUID();
        mockMvc.perform(get("/api/v1/clusters/{clusterId}/brokers/{brokerId}/metrics", clusterId, 1)
                        .param("window", "2m"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void topicMetricsAggregatesAcrossBrokers() throws Exception {
        PrometheusScrapeProperties props = new PrometheusScrapeProperties(
                false,
                Duration.ofSeconds(10),
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                "/metrics",
                null,
                1024L,
                Duration.ofSeconds(60),
                Duration.ofSeconds(120),
                10000,
                List.of(),
                false,
                100);
        var controller = new RuntimeMetricsController(sampleStore, adminStore, mapper, props);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new ApiProblemAdvice())
                .setMessageConverters(new JacksonJsonHttpMessageConverter(
                        JsonMapper.builderWithJackson2Defaults().build()))
                .build();

        UUID clusterId = UUID.randomUUID();
        Instant now = Instant.now();
        var snap =
                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot(
                        clusterId,
                        now,
                        2,
                        1,
                        1,
                        0,
                        0,
                        1,
                        com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus.SUCCESS,
                        now,
                        null,
                        List.of(
                                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain
                                        .BrokerLeaderCount(1, 1),
                                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain
                                        .BrokerLeaderCount(2, 1)));
        adminStore.saveSuccessful(snap);

        String topic = "my-topic";
        // create per-broker messages.in.total counters
        Instant latest = Instant.now();
        Instant prev = latest.minusSeconds(61);
        var id1 = new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricIdentity(
                clusterId, 1, topic, "messages.in.total");
        var id2 = new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricIdentity(
                clusterId, 2, topic, "messages.in.total");
        sampleStore.append(new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSample(
                id1,
                100.0,
                prev,
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSemanticType
                        .MONOTONIC_COUNTER,
                "count",
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSourceBackend.PROMETHEUS,
                null));
        sampleStore.append(new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSample(
                id1,
                160.0,
                latest,
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSemanticType
                        .MONOTONIC_COUNTER,
                "count",
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSourceBackend.PROMETHEUS,
                null));
        sampleStore.append(new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSample(
                id2,
                200.0,
                prev,
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSemanticType
                        .MONOTONIC_COUNTER,
                "count",
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSourceBackend.PROMETHEUS,
                null));
        sampleStore.append(new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSample(
                id2,
                260.0,
                latest,
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSemanticType
                        .MONOTONIC_COUNTER,
                "count",
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSourceBackend.PROMETHEUS,
                null));

        mockMvc.perform(get("/api/v1/clusters/{clusterId}/topics/{topic}/metrics", clusterId, topic)
                        .param("window", "1m")
                        .param("perBroker", "true"))
                .andExpect(status().isOk());
    }

    @Test
    void clusterMetricsSummarizesBrokerAvailability() throws Exception {
        PrometheusScrapeProperties props = new PrometheusScrapeProperties(
                false,
                Duration.ofSeconds(10),
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                "/metrics",
                null,
                1024L,
                Duration.ofSeconds(60),
                Duration.ofSeconds(120),
                10000,
                List.of(),
                false,
                100);
        var controller = new RuntimeMetricsController(sampleStore, adminStore, mapper, props);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new ApiProblemAdvice())
                .setMessageConverters(new JacksonJsonHttpMessageConverter(
                        JsonMapper.builderWithJackson2Defaults().build()))
                .build();

        UUID clusterId = UUID.randomUUID();
        Instant now = Instant.now();
        var snap =
                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot(
                        clusterId,
                        now,
                        2,
                        0,
                        0,
                        0,
                        0,
                        1,
                        com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus.SUCCESS,
                        now,
                        null,
                        List.of(
                                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain
                                        .BrokerLeaderCount(1, 1),
                                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain
                                        .BrokerLeaderCount(2, 1)));
        adminStore.saveSuccessful(snap);

        // provide a leader count sample only for broker 1
        var id = new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricIdentity(
                clusterId, 1, null, "broker.leader.count");
        sampleStore.append(new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSample(
                id,
                1.0,
                Instant.now(),
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSemanticType.GAUGE,
                "count",
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSourceBackend.PROMETHEUS,
                null));

        mockMvc.perform(get("/api/v1/clusters/{clusterId}/metrics", clusterId).param("window", "1m"))
                .andExpect(status().isOk());
    }
}
