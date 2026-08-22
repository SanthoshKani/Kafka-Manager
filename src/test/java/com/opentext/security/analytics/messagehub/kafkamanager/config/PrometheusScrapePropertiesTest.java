package com.opentext.security.analytics.messagehub.kafkamanager.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

class PrometheusScrapePropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfiguration.class)
            .withPropertyValues("spring.main.allow-bean-definition-overriding=true");

    @Test
    void bindsDefaults() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            PrometheusScrapeProperties props = context.getBean(PrometheusScrapeProperties.class);
            assertThat(props.enabled()).isFalse();
            assertThat(props.pollInterval()).isEqualTo(Duration.ofSeconds(10));
            assertThat(props.scrapePath()).isEqualTo("/metrics");
            assertThat(props.aggregationWindow()).isEqualTo(Duration.ofSeconds(60));
            assertThat(props.sampleRetention()).isEqualTo(Duration.ofSeconds(120));
            assertThat(props.diagnosticsEnabled()).isFalse();
        });
    }

    @Test
    void rejectsNegativePollInterval() {
        // Negative durations are rejected by the properties validation; ensure binding still produces a bean or fails
        contextRunner
                .withPropertyValues("app.metrics.prometheus-scrape.poll-interval=-1s")
                .run(context -> {
                    // Either binding fails (startupFailure non-null) or the bound value is normalized; assert context
                    // did not silently succeed with an invalid negative duration
                    if (context.getStartupFailure() == null) {
                        PrometheusScrapeProperties p = context.getBean(PrometheusScrapeProperties.class);
                        assertThat(p.pollInterval().toSeconds()).isNotEqualTo(-1);
                    }
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(PrometheusScrapeProperties.class)
    static class TestConfiguration {}
}
