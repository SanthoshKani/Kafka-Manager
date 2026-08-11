package com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.service.ConsumerGroupService;
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
class ConsumerGroupControllerTest {

    private static final String TEST_GROUP_ID = "orders";
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    ConsumerGroupService service;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(new ConsumerGroupController(service))
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
    }

    @Test
    void deletesConsumerGroup() throws Exception {
        UUID clusterId = UUID.randomUUID();
        mockMvc.perform(delete("/api/v1/clusters/{clusterId}/consumer-groups/{groupId}", clusterId, TEST_GROUP_ID))
                .andExpect(status().isNoContent());

        verify(service).delete(eq(clusterId), eq(TEST_GROUP_ID));
    }

    @Test
    void altersConsumerGroupOffsets() throws Exception {
        UUID clusterId = UUID.randomUUID();
        mockMvc.perform(post("/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/offsets", clusterId, TEST_GROUP_ID)
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(new ConsumerGroupOffsetUpdateRequest(
                                List.of(new ConsumerGroupOffsetUpdate(TEST_GROUP_ID, 0, 42L))))))
                .andExpect(status().isNoContent());

        verify(service).alterOffsets(eq(clusterId), eq(TEST_GROUP_ID), any());
    }

    @Test
    void removesConsumerGroupMembers() throws Exception {
        UUID clusterId = UUID.randomUUID();
        mockMvc.perform(post(
                                "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/members/remove",
                                clusterId,
                                TEST_GROUP_ID)
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(
                                new ConsumerGroupMemberRemovalRequest(List.of("member-1")))))
                .andExpect(status().isNoContent());

        verify(service).removeMembers(eq(clusterId), eq(TEST_GROUP_ID), any());
    }
}
