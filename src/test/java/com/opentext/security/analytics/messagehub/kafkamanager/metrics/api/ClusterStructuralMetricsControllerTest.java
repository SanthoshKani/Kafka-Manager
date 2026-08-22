package com.opentext.security.analytics.messagehub.kafkamanager.metrics.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiProblemAdvice;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.BrokerLeaderCount;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.service.InMemoryAdminDerivedMetricsStore;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(SpringExtension.class)
class ClusterStructuralMetricsControllerTest {

    private MockMvc mockMvc;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private InMemoryAdminDerivedMetricsStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryAdminDerivedMetricsStore();
        mockMvc = MockMvcBuilders.standaloneSetup(new ClusterStructuralMetricsController(store))
                .setControllerAdvice(new ApiProblemAdvice())
                .setMessageConverters(new JacksonJsonHttpMessageConverter(
                        JsonMapper.builderWithJackson2Defaults().build()))
                .build();
    }

    @Test
    void returnsCurrentSnapshotWhenAvailable() throws Exception {
        UUID clusterId = UUID.randomUUID();
        Instant now = Instant.now();
        AdminClientMetricsSnapshot snapshot = new AdminClientMetricsSnapshot(
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
                List.of(new BrokerLeaderCount(1, 5), new BrokerLeaderCount(2, 3), new BrokerLeaderCount(3, 2)));

        store.saveSuccessful(snapshot);

        mockMvc.perform(get("/api/v1/clusters/{clusterId}/metrics/structural", clusterId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.brokerCount").value(3))
                .andExpect(jsonPath("$.topicCount").value(5));
    }

    @Test
    void returnsServiceUnavailableWhenInitializing() throws Exception {
        UUID clusterId = UUID.randomUUID();
        store.recordFailure(clusterId, Instant.now(), "no-connect");

        mockMvc.perform(get("/api/v1/clusters/{clusterId}/metrics/structural", clusterId))
                .andExpect(status().isServiceUnavailable());
    }

    @Test
    void returnsLastKnownGoodWhenLatestFailed() throws Exception {
        UUID clusterId = UUID.randomUUID();
        Instant t1 = Instant.now().minusSeconds(60);
        AdminClientMetricsSnapshot good = new AdminClientMetricsSnapshot(
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
                List.of(new BrokerLeaderCount(1, 5), new BrokerLeaderCount(2, 3)));
        store.saveSuccessful(good);

        // Now a failing attempt occurs
        store.recordFailure(clusterId, Instant.now(), "timeout");

        mockMvc.perform(get("/api/v1/clusters/{clusterId}/metrics/structural", clusterId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.collectionStatus").value("FAILURE"))
                .andExpect(jsonPath("$.brokerCount").value(2));
    }

    @Test
    void returnsNotFoundForUnknownCluster() throws Exception {
        UUID clusterId = UUID.randomUUID();
        mockMvc.perform(get("/api/v1/clusters/{clusterId}/metrics/structural", clusterId))
                .andExpect(status().isNotFound());
    }
}
