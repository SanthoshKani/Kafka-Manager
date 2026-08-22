package com.opentext.security.analytics.messagehub.kafkamanager.metrics.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiProblemAdvice;
import com.opentext.security.analytics.messagehub.kafkamanager.config.PrometheusScrapeProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus.MetricMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.service.InMemoryAdminDerivedMetricsStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.json.JsonMapper;

class BrokerMetricsDiagnosticsControllerTest {

    private MockMvc mockMvc;
    private InMemoryAdminDerivedMetricsStore store;
    private MetricMapper mapper;
    private com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.BoundedInMemorySampleStore
            sampleStore;

    @BeforeEach
    void setUp() {
        store = new InMemoryAdminDerivedMetricsStore();
        mapper = new MetricMapper();
        sampleStore =
                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime
                        .BoundedInMemorySampleStore();
    }

    @AfterEach
    void tearDown() {
        // clear security context
        org.springframework.security.core.context.SecurityContextHolder.clearContext();
    }

    @Test
    void returnsForbiddenWhenDiagnosticsDisabled() throws Exception {
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
                1000,
                List.of(),
                false,
                100);
        var controller = new BrokerMetricsDiagnosticsController(mapper, store, props, sampleStore);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new ApiProblemAdvice())
                .setMessageConverters(new JacksonJsonHttpMessageConverter(
                        JsonMapper.builderWithJackson2Defaults().build()))
                .build();

        UUID clusterId = UUID.randomUUID();
        mockMvc.perform(get("/api/v1/clusters/{clusterId}/brokers/{brokerId}/metrics/diagnostics", clusterId, 1))
                .andExpect(status().isForbidden());
    }

    @Test
    void returnsForbiddenForNonAdminWhenEnabled() throws Exception {
        PrometheusScrapeProperties props = new PrometheusScrapeProperties(
                true,
                Duration.ofSeconds(10),
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                "/metrics",
                null,
                1024L,
                Duration.ofSeconds(60),
                Duration.ofSeconds(120),
                1000,
                List.of(),
                true,
                100);
        var controller = new BrokerMetricsDiagnosticsController(mapper, store, props, sampleStore);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new ApiProblemAdvice())
                .setMessageConverters(new JacksonJsonHttpMessageConverter(
                        JsonMapper.builderWithJackson2Defaults().build()))
                .build();

        // set authenticated user without admin role
        var auth =
                new UsernamePasswordAuthenticationToken("user", null, List.of(new SimpleGrantedAuthority("ROLE_USER")));
        org.springframework.security.core.context.SecurityContextHolder.getContext()
                .setAuthentication(auth);

        UUID clusterId = UUID.randomUUID();
        mockMvc.perform(get("/api/v1/clusters/{clusterId}/brokers/{brokerId}/metrics/diagnostics", clusterId, 1))
                .andExpect(status().isForbidden());
    }

    @Test
    void returnsDiagnosticsWhenEnabledAndAdmin() throws Exception {
        PrometheusScrapeProperties props = new PrometheusScrapeProperties(
                true,
                Duration.ofSeconds(10),
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                "/metrics",
                null,
                1024L,
                Duration.ofSeconds(60),
                Duration.ofSeconds(120),
                1000,
                List.of(),
                true,
                100);
        var controller = new BrokerMetricsDiagnosticsController(mapper, store, props, sampleStore);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new ApiProblemAdvice())
                .setMessageConverters(new JacksonJsonHttpMessageConverter(
                        JsonMapper.builderWithJackson2Defaults().build()))
                .build();

        // set admin auth
        var auth = new UsernamePasswordAuthenticationToken(
                "admin", null, List.of(new SimpleGrantedAuthority("ROLE_ADMIN")));
        org.springframework.security.core.context.SecurityContextHolder.getContext()
                .setAuthentication(auth);

        UUID clusterId = UUID.randomUUID();
        // populate admin store with a snapshot so cluster exists
        Instant now = Instant.now();
        var snapshot =
                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot(
                        clusterId,
                        now,
                        3,
                        5,
                        10,
                        0,
                        0,
                        1,
                        com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus.SUCCESS,
                        now,
                        null,
                        List.of(
                                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain
                                        .BrokerLeaderCount(1, 5),
                                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain
                                        .BrokerLeaderCount(2, 3),
                                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain
                                        .BrokerLeaderCount(3, 2)));
        store.saveSuccessful(snapshot);

        mockMvc.perform(get("/api/v1/clusters/{clusterId}/brokers/{brokerId}/metrics/diagnostics", clusterId, 1))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.recognizedMetrics").isArray())
                .andExpect(jsonPath("$.adminSnapshot.brokerCount").value(3));
    }

    @Test
    void detectsCounterResetForLeaderElections() {
        // create two samples: previous larger than current -> reset
        var id = new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricIdentity(
                UUID.randomUUID(), 1, null, "leader.elections.total");
        var now = Instant.now();
        var prev = new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSample(
                id,
                100.0,
                now.minusSeconds(60),
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSemanticType
                        .MONOTONIC_COUNTER,
                "count",
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSourceBackend.PROMETHEUS,
                null);
        var curr = new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSample(
                id,
                5.0,
                now,
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSemanticType
                        .MONOTONIC_COUNTER,
                "count",
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSourceBackend.PROMETHEUS,
                null);

        com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.RateCalculator calc =
                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.RateCalculator();
        var res = calc.calculate(prev, curr);
        assertThat(res)
                .isInstanceOf(
                        com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.RateCalculator
                                .RateResult.CounterReset.class);
    }

    @Test
    void controllerTransitionIsReflectedInAdminSnapshot() throws Exception {
        UUID clusterId = UUID.randomUUID();
        Instant t1 = Instant.now().minusSeconds(120);
        var snap1 =
                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot(
                        clusterId,
                        t1,
                        2,
                        4,
                        8,
                        0,
                        0,
                        1,
                        com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus.SUCCESS,
                        t1,
                        null,
                        List.of(
                                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain
                                        .BrokerLeaderCount(1, 4),
                                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain
                                        .BrokerLeaderCount(2, 4)));
        store.saveSuccessful(snap1);

        // transition: controller moves to broker 2
        Instant t2 = Instant.now();
        var snap2 =
                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot(
                        clusterId,
                        t2,
                        2,
                        4,
                        8,
                        0,
                        0,
                        2,
                        com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus.SUCCESS,
                        t2,
                        null,
                        List.of(
                                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain
                                        .BrokerLeaderCount(1, 3),
                                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain
                                        .BrokerLeaderCount(2, 5)));
        store.saveSuccessful(snap2);

        var current = store.getCurrent(clusterId);
        assertThat(current.controllerBrokerId()).isEqualTo(2);
    }
}
