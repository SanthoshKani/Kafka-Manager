package com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus.PrometheusTextParser.ParsedMetric;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricIdentity;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSample;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSemanticType;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSourceBackend;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * One-shot Prometheus scraper for a single broker. Performs HTTP GET, enforces timeouts and maxSize,
 * parses only allowed metrics and normalizes to MetricSample.
 */
public final class PrometheusScraper {

    public static final class ScrapeResult {
        public final List<MetricSample> samples;
        public final CollectionHealth health;

        public ScrapeResult(List<MetricSample> samples, CollectionHealth health) {
            this.samples = List.copyOf(samples);
            this.health = health;
        }
    }

    public static final class CollectionHealth {
        public final Instant lastAttempt;
        public final Instant lastSuccess; // may be null
        public final Duration duration; // attempt duration
        public final Status status;
        public final String sanitizedError; // may be null

        public enum Status {
            SUCCESS,
            FAILURE
        }

        public CollectionHealth(
                Instant lastAttempt, Instant lastSuccess, Duration duration, Status status, String sanitizedError) {
            this.lastAttempt = lastAttempt;
            this.lastSuccess = lastSuccess;
            this.duration = duration;
            this.status = status;
            this.sanitizedError = sanitizedError;
        }
    }

    private final HttpClient client;
    private final PrometheusTextParser parser;
    private final Set<String> allowlist;
    private final Map<String, String> metricNameMap; // source -> canonical
    private final MetricMapper mapper;
    private final int maxResponseSize;

    public PrometheusScraper(
            HttpClient client,
            PrometheusTextParser parser,
            Set<String> allowlist,
            Map<String, String> metricNameMap,
            MetricMapper mapper,
            int maxResponseSize) {
        this.client = Objects.requireNonNull(client);
        this.parser = Objects.requireNonNull(parser);
        this.allowlist = Objects.requireNonNull(allowlist);
        this.metricNameMap = Objects.requireNonNull(metricNameMap);
        this.mapper = Objects.requireNonNull(mapper);
        this.maxResponseSize = maxResponseSize;
    }

    public ScrapeResult scrape(URI uri, MetricIdentity identity, Duration connectTimeout, Duration readTimeout) {
        Objects.requireNonNull(uri);
        Objects.requireNonNull(identity);
        Instant start = Instant.now();
        try {
            HttpRequest req =
                    HttpRequest.newBuilder(uri).GET().timeout(readTimeout).build();
            HttpClient c = client;
            HttpResponse<InputStream> resp = c.send(req, HttpResponse.BodyHandlers.ofInputStream());
            int status = resp.statusCode();
            if (status != 200) {
                return new ScrapeResult(
                        List.of(),
                        new CollectionHealth(
                                start,
                                null,
                                Duration.between(start, Instant.now()),
                                CollectionHealth.Status.FAILURE,
                                "HTTP_STATUS_" + status));
            }
            InputStream in = resp.body();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            int read;
            int total = 0;
            while ((read = in.read(buf)) != -1) {
                total += read;
                if (total > maxResponseSize) {
                    return new ScrapeResult(
                            List.of(),
                            new CollectionHealth(
                                    start,
                                    null,
                                    Duration.between(start, Instant.now()),
                                    CollectionHealth.Status.FAILURE,
                                    "OVERSIZED_RESPONSE"));
                }
                baos.write(buf, 0, read);
            }
            byte[] body = baos.toByteArray();
            List<ParsedMetric> parsed = parser.parse(body);
            List<MetricSample> samples = new ArrayList<>();
            for (ParsedMetric pm : parsed) {
                String canonical = metricNameMap.getOrDefault(pm.name, pm.name);
                if (!allowlist.contains(pm.name)) continue;
                // map labels to identity parts
                Integer brokerId = identity.brokerId();
                String topic = null;
                Map<String, String> labels = pm.labels;
                if (labels.containsKey("topic")) topic = labels.get("topic");
                // build sample
                Instant ts = pm.timestamp == null ? Instant.now() : pm.timestamp;
                // consult mapper for semantic type and unit/normalization
                MetricMapper.Descriptor desc = mapper.get(pm.name);
                MetricSemanticType semantic = desc == null ? MetricSemanticType.GAUGE : desc.semanticType;
                String unit = desc == null ? "count" : desc.unit;
                double value = pm.value;
                if (desc != null && desc.sourceUnit == MetricMapper.SourceUnit.PERCENT) {
                    // source may report 0..100 (percent) or 0..1 (ratio). Normalize to ratio (0..1).
                    if (value > 1.0) {
                        value = value / 100.0;
                    }
                }
                MetricSample ms = new MetricSample(
                        new MetricIdentity(identity.clusterId(), brokerId, topic, canonical),
                        value,
                        ts,
                        semantic,
                        unit,
                        MetricSourceBackend.PROMETHEUS,
                        pm.name);
                samples.add(ms);
            }
            return new ScrapeResult(
                    samples,
                    new CollectionHealth(
                            start,
                            Instant.now(),
                            Duration.between(start, Instant.now()),
                            CollectionHealth.Status.SUCCESS,
                            null));
        } catch (Exception e) {
            String sanitized = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            if (sanitized.length() > 256) sanitized = sanitized.substring(0, 256);
            return new ScrapeResult(
                    List.of(),
                    new CollectionHealth(
                            start,
                            null,
                            Duration.between(start, Instant.now()),
                            CollectionHealth.Status.FAILURE,
                            sanitized));
        }
    }
}
