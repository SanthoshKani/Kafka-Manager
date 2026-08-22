package com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus;

import static org.assertj.core.api.Assertions.assertThat;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricIdentity;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSample;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PrometheusNormalizationTest {

    @Test
    void percentValueIsNormalizedToRatio() throws Exception {
        String body = "kafka_network_processor_avg_idle_percent 75\n";
        var server = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress(0), 0);
        server.createContext("/metrics", exchange -> {
            exchange.sendResponseHeaders(200, body.getBytes().length);
            exchange.getResponseBody().write(body.getBytes());
            exchange.close();
        });
        server.start();
        try {
            int port = server.getAddress().getPort();
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(2))
                    .build();
            PrometheusTextParser parser =
                    new PrometheusTextParser(Set.of("kafka_network_processor_avg_idle_percent"), 1024);
            MetricMapper mapper = new MetricMapper();
            // mapper already has this key with SourceUnit.PERCENT by default
            PrometheusScraper scraper = new PrometheusScraper(
                    client,
                    parser,
                    Set.of("kafka_network_processor_avg_idle_percent"),
                    Map.of("kafka_network_processor_avg_idle_percent", "network_processor_idle"),
                    mapper,
                    1024);
            var res = scraper.scrape(
                    new URI("http://localhost:" + port + "/metrics"),
                    new MetricIdentity(java.util.UUID.randomUUID(), 1, null, "unused"),
                    Duration.ofSeconds(2),
                    Duration.ofSeconds(5));
            assertThat(res.samples).hasSize(1);
            MetricSample s = res.samples.get(0);
            assertThat(s.unit()).isEqualTo("ratio");
            assertThat(s.value()).isEqualTo(0.75);
        } finally {
            server.stop(0);
        }
    }
}
