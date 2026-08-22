package com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus;

import static org.assertj.core.api.Assertions.assertThat;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricIdentity;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSample;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PrometheusScraperHttpTest {

    private HttpServer server;
    private int port;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        port = server.getAddress().getPort();
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void successfulScrapeParsesAllowedMetric() throws Exception {
        String body = "kafka_metric{broker=\"1\",topic=\"orders\"} 42\n";
        server.createContext("/metrics", new SimpleHandler(200, body));
        HttpClient client =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        PrometheusTextParser parser = new PrometheusTextParser(Set.of("kafka_metric"), 1024);
        // create a simple mapper for test
        MetricMapper mapper = new MetricMapper();
        mapper.register(
                "kafka_metric",
                "kafka_metric",
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSemanticType.GAUGE);
        PrometheusScraper scraper = new PrometheusScraper(
                client, parser, Set.of("kafka_metric"), Map.of("kafka_metric", "kafka_metric"), mapper, 1024);
        URI uri = new URI("http://localhost:" + port + "/metrics");
        MetricIdentity id = new MetricIdentity(java.util.UUID.randomUUID(), 1, null, "unused");
        var res = scraper.scrape(uri, id, Duration.ofSeconds(2), Duration.ofSeconds(5));
        assertThat(res.samples).hasSize(1);
        MetricSample s = res.samples.get(0);
        assertThat(s.value()).isEqualTo(42.0);
        assertThat(res.health.status).isEqualTo(PrometheusScraper.CollectionHealth.Status.SUCCESS);
    }

    static final class SimpleHandler implements HttpHandler {
        private final int status;
        private final String body;

        SimpleHandler(int status, String body) {
            this.status = status;
            this.body = body;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.sendResponseHeaders(status, body.getBytes().length);
            exchange.getResponseBody().write(body.getBytes());
            exchange.close();
        }
    }
}
