package com.opentext.security.analytics.messagehub.kafkamanager.config;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus.BrokerEndpointRegistry;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus.InMemoryBrokerEndpointRegistry;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus.MetricMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus.PrometheusTextParser;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.BoundedInMemorySampleStore;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.RuntimeMetricSampleStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration to provide a default in-memory RuntimeMetricSampleStore when none is defined.
 */
@Configuration
@ConditionalOnProperty(prefix = "app.features", name = "metrics.enabled", havingValue = "true", matchIfMissing = false)
public class MetricsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(RuntimeMetricSampleStore.class)
    public RuntimeMetricSampleStore runtimeMetricSampleStore() {
        return new BoundedInMemorySampleStore();
    }

    @Bean
    @ConditionalOnMissingBean(BrokerEndpointRegistry.class)
    public BrokerEndpointRegistry brokerEndpointRegistry() {
        return new InMemoryBrokerEndpointRegistry();
    }

    @Bean
    @ConditionalOnMissingBean(PrometheusTextParser.class)
    public PrometheusTextParser prometheusTextParser(MetricMapper mapper) {
        // Allowlist is taken from the MetricMapper's known keys; max input size set conservatively
        return new PrometheusTextParser(mapper.mapKeys(), 64 * 1024);
    }
}

