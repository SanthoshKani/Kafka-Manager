package com.opentext.security.analytics.messagehub.kafkamanager.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

class KafkaManagerPropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfiguration.class)
            .withPropertyValues("spring.main.allow-bean-definition-overriding=true");

    @Test
    void bindsAdminDerivedMetricsDefaultsAndTopicExclusionPatterns() {
        contextRunner
                .withPropertyValues(
                        "app.metrics.admin-derived.enabled=false",
                        "app.metrics.admin-derived.topic-exclusion-patterns[0]=^__.*",
                        "app.metrics.admin-derived.topic-exclusion-patterns[1]=.*-internal$")
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    AdminDerivedMetricsProperties properties = context.getBean(AdminDerivedMetricsProperties.class);

                    assertThat(properties.enabled()).isFalse();
                    assertThat(properties.pollInterval()).isEqualTo(Duration.ofSeconds(60));
                    assertThat(properties.operationTimeout()).isEqualTo(Duration.ofSeconds(30));
                    assertThat(properties.topicExclusionPatterns()).containsExactly("^__.*", ".*-internal$");
                });
    }

    @Test
    void rejectsNegativeAdminDerivedPollInterval() {
        contextRunner
                .withPropertyValues("app.metrics.admin-derived.poll-interval=-1s")
                .run(context -> {
                    assertThat(context.getStartupFailure()).isNotNull();
                    assertThat(rootCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("app.metrics.admin-derived.poll-interval");
                });
    }

    @Test
    void rejectsZeroAdminDerivedOperationTimeout() {
        contextRunner
                .withPropertyValues("app.metrics.admin-derived.operation-timeout=0s")
                .run(context -> {
                    assertThat(context.getStartupFailure()).isNotNull();
                    assertThat(rootCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("app.metrics.admin-derived.operation-timeout");
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(AdminDerivedMetricsProperties.class)
    static class TestConfiguration {}

    @Validated
    @ConfigurationProperties(prefix = "app.metrics.admin-derived")
    record AdminDerivedMetricsProperties(
            Boolean enabled, Duration pollInterval, Duration operationTimeout, List<String> topicExclusionPatterns) {

        AdminDerivedMetricsProperties {
            enabled = enabled == null ? Boolean.TRUE : enabled;
            pollInterval = pollInterval == null ? Duration.ofSeconds(60) : pollInterval;
            operationTimeout = operationTimeout == null ? Duration.ofSeconds(30) : operationTimeout;
            topicExclusionPatterns = topicExclusionPatterns == null ? List.of() : List.copyOf(topicExclusionPatterns);

            validatePositiveDuration("app.metrics.admin-derived.poll-interval", pollInterval);
            validatePositiveDuration("app.metrics.admin-derived.operation-timeout", operationTimeout);
        }

        private static void validatePositiveDuration(String propertyName, Duration duration) {
            if (duration.compareTo(Duration.ZERO) <= 0) {
                throw new IllegalArgumentException(propertyName + " must be greater than zero");
            }
        }
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;
        while (current != null && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
