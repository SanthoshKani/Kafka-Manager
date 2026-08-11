package com.opentext.security.analytics.messagehub.kafkamanager.topics.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.topics.service.TopicService;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
class TopicControllerTest {

    private static final String TEST_TOPIC_NAME = "orders";
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    TopicService topicService;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(new TopicController(topicService))
                .setMessageConverters(new JacksonJsonHttpMessageConverter(
                        JsonMapper.builderWithJackson2Defaults().build()))
                .build();
    }

    @Test
    void describesTopicConfigs() throws Exception {
        UUID clusterId = UUID.randomUUID();
        when(topicService.describeConfigs(eq(clusterId), eq(TEST_TOPIC_NAME)))
                .thenReturn(Map.of("cleanup.policy", "delete"));

        mockMvc.perform(get("/api/v1/clusters/{clusterId}/topics/{topicName}/configs", clusterId, TEST_TOPIC_NAME))
                .andExpect(status().isOk())
                .andExpect(content().json("{\"cleanup.policy\":\"delete\"}"));
    }

    @Test
    void createsPartitions() throws Exception {
        UUID clusterId = UUID.randomUUID();
        mockMvc.perform(post("/api/v1/clusters/{clusterId}/topics/{topicName}/partitions", clusterId, TEST_TOPIC_NAME)
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(new TopicPartitionExpansionRequest(6, null))))
                .andExpect(status().isNoContent());

        verify(topicService).createPartitions(eq(clusterId), eq(TEST_TOPIC_NAME), any());
    }

    @Test
    void altersTopicConfigs() throws Exception {
        UUID clusterId = UUID.randomUUID();
        mockMvc.perform(patch("/api/v1/clusters/{clusterId}/topics/{topicName}/configs", clusterId, TEST_TOPIC_NAME)
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(
                                new TopicConfigMutationBatchRequest(java.util.List.of(new TopicConfigMutationRequest(
                                        "retention.ms", "60000", ConfigMutationOperation.SET))))))
                .andExpect(status().isNoContent());

        verify(topicService).alterConfigs(eq(clusterId), eq(TEST_TOPIC_NAME), any());
    }
}
