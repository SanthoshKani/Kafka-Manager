package com.opentext.security.analytics.messagehub.kafkamanager.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ProblemResponseWriter;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimitingConfigTest {

    @Test
    void rejectsRequestsAfterTheConfiguredCapacityIsExceeded() throws Exception {
        RateLimitingConfig filter = new RateLimitingConfig(
                new KafkaManagerProperties(
                        "test",
                        new KafkaManagerProperties.Security(
                                "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
                                new KafkaManagerProperties.BasicAuth("testuser", "testpass"),
                                new KafkaManagerProperties.OAuth2ResourceServer("", "")),
                        new KafkaManagerProperties.Admin(
                                "localhost:9092",
                                "PLAINTEXT",
                                null,
                                Duration.ofSeconds(1),
                                Duration.ofSeconds(1)),
                        new KafkaManagerProperties.RateLimit(true, 1, Duration.ofMinutes(1), "X-Client-Id")),
                new ProblemResponseWriter(new ObjectMapper()));

        MockHttpServletRequest firstRequest = new MockHttpServletRequest("GET", "/api/v1/clusters");
        firstRequest.addHeader("X-Client-Id", "client-1");
        MockHttpServletResponse firstResponse = new MockHttpServletResponse();
        filter.doFilter(firstRequest, firstResponse, new MockFilterChain());

        assertThat(firstResponse.getStatus()).isEqualTo(200);
        assertThat(firstResponse.getHeader("X-Rate-Limit-Remaining")).isEqualTo("0");

        MockHttpServletRequest secondRequest = new MockHttpServletRequest("GET", "/api/v1/clusters");
        secondRequest.addHeader("X-Client-Id", "client-1");
        secondRequest.addHeader(CorrelationIdFilter.HEADER, "corr-123");
        MockHttpServletResponse secondResponse = new MockHttpServletResponse();
        filter.doFilter(secondRequest, secondResponse, new MockFilterChain());

        assertThat(secondResponse.getStatus()).isEqualTo(429);
        assertThat(secondResponse.getContentAsString()).contains("RATE_LIMITED");
        assertThat(secondResponse.getHeader("Retry-After")).isNotBlank();
    }
}
