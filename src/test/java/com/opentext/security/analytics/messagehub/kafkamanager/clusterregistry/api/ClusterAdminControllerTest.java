package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service.ClusterAdminService;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@ExtendWith(MockitoExtension.class)
class ClusterAdminControllerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    ClusterAdminService service;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(new ClusterAdminController(service))
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
    }

    @Test
    void electsLeaders() throws Exception {
        UUID clusterId = UUID.randomUUID();
        mockMvc.perform(
                        post("/api/v1/clusters/{clusterId}/actions/leader-election", clusterId)
                                .contentType("application/json")
                                .content(objectMapper.writeValueAsString(new LeaderElectionRequest(
                                        org.apache.kafka.common.ElectionType.PREFERRED,
                                        List.of(new com.opentext.security.analytics.messagehub.kafkamanager.common
                                                .TopicPartitionRequest("orders", 0))))))
                .andExpect(status().isNoContent());

        verify(service).electLeaders(eq(clusterId), any());
    }

    @Test
    void altersPartitionReassignments() throws Exception {
        UUID clusterId = UUID.randomUUID();
        mockMvc.perform(put("/api/v1/clusters/{clusterId}/actions/partition-reassignments", clusterId)
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(new PartitionReassignmentRequest(
                                List.of(new PartitionReassignmentChange("orders", 0, List.of(1, 2, 3), false))))))
                .andExpect(status().isNoContent());

        verify(service).alterPartitionReassignments(eq(clusterId), any());
    }

    @Test
    void listsPartitionReassignments() throws Exception {
        UUID clusterId = UUID.randomUUID();
        when(service.listPartitionReassignments(eq(clusterId)))
                .thenReturn(
                        List.of(new PartitionReassignmentResponse("orders", 0, List.of(1, 2), List.of(2), List.of(3))));

        mockMvc.perform(get("/api/v1/clusters/{clusterId}/actions/partition-reassignments", clusterId))
                .andExpect(status().isOk())
                .andExpect(
                        content()
                                .json(
                                        "[{\"topic\":\"orders\",\"partition\":0,\"replicas\":[1,2],\"addingReplicas\":[2],\"removingReplicas\":[3]}]"));
    }
}
