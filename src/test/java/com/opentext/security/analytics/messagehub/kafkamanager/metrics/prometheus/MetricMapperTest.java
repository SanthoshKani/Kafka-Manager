package com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus;

import static org.assertj.core.api.Assertions.assertThat;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSemanticType;
import org.junit.jupiter.api.Test;

class MetricMapperTest {

    @Test
    void defaultMappingsContainExpectedKeys() {
        MetricMapper mapper = new MetricMapper();
        var keys = mapper.mapKeys();
        assertThat(keys).contains("kafka_server_broker_topic_metrics_bytes_in_total");
        assertThat(mapper.get("kafka_network_processor_avg_idle_percent")).isNotNull();
        var desc = mapper.get("kafka_server_broker_topic_metrics_messages_in_total");
        assertThat(desc.canonicalName).isEqualTo("messages.in.total");
        assertThat(desc.semanticType).isEqualTo(MetricSemanticType.MONOTONIC_COUNTER);
    }
}
