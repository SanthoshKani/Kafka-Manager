package com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PrometheusTextParserTest {

    @Test
    void parsesAllowedMetricsAndLabels() throws Exception {
        String fixture = "# HELP kafka_server_broker_topic_metrics  some help\n"
                + "kafka_server_broker_topic_metrics{broker=\"1\",topic=\"orders\\\"-prod\",request=\"Fetch\"} 123.0 1620000000000\n"
                + "other_metric 5\n";
        PrometheusTextParser parser = new PrometheusTextParser(Set.of("kafka_server_broker_topic_metrics"), 1024);
        List<PrometheusTextParser.ParsedMetric> parsed = parser.parse(fixture.getBytes(StandardCharsets.UTF_8));
        assertThat(parsed).hasSize(1);
        var m = parsed.get(0);
        assertThat(m.name).isEqualTo("kafka_server_broker_topic_metrics");
        assertThat(m.labels.get("broker")).isEqualTo("1");
        assertThat(m.labels.get("topic")).isEqualTo("orders\"-prod");
        assertThat(m.labels.get("request")).isEqualTo("Fetch");
    }
}
