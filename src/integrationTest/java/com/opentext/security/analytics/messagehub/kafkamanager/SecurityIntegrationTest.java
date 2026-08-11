package com.opentext.security.analytics.messagehub.kafkamanager;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@SpringBootTest
@ActiveProfiles("it")
class SecurityIntegrationTest {

    @Autowired
    WebApplicationContext context;

    @Test
    void protectedEndpointsRequireAuthentication() throws Exception {
        MockMvc mockMvc = MockMvcBuilders.webAppContextSetup(context)
                .apply(springSecurity())
                .build();
        mockMvc.perform(get("/api/v1/clusters").with(httpBasic("admin", "admin")))
                .andExpect(status().isOk());
    }

    @Test
    void protectedEndpointsRejectInvalidCredentials() throws Exception {
        MockMvc mockMvc = MockMvcBuilders.webAppContextSetup(context)
                .apply(springSecurity())
                .build();
        mockMvc.perform(get("/api/v1/clusters").with(httpBasic("admin", "wrong")))
                .andExpect(status().is4xxClientError());
    }

    @Test
    void protectedEndpointsRejectMissingCredentials() throws Exception {
        MockMvc mockMvc = MockMvcBuilders.webAppContextSetup(context)
                .apply(springSecurity())
                .build();
        mockMvc.perform(get("/api/v1/clusters")).andExpect(status().is4xxClientError());
    }
}
