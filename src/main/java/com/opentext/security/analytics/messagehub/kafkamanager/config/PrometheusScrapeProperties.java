package com.opentext.security.analytics.messagehub.kafkamanager.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app.metrics.prometheus-scrape")
public record PrometheusScrapeProperties(
        Boolean enabled,
        Duration pollInterval,
        Duration connectTimeout,
        Duration readTimeout,
        String scrapePath,
        String endpointTemplate,
        Long maxResponseSize,
        Duration aggregationWindow,
        Duration sampleRetention,
        Integer maxSeriesCount,
        List<String> allowedMetricNames,
        // diagnostics feature (disabled by default in production)
        Boolean diagnosticsEnabled,
        Integer diagnosticsMaxItems) {

    public PrometheusScrapeProperties {
        Boolean e = enabled == null ? Boolean.FALSE : enabled;
        Duration pi = pollInterval == null ? Duration.ofSeconds(10) : pollInterval;
        Duration ct = connectTimeout == null ? Duration.ofSeconds(2) : connectTimeout;
        Duration rt = readTimeout == null ? Duration.ofSeconds(5) : readTimeout;
        String sp = scrapePath == null ? "/metrics" : scrapePath;
        String et = endpointTemplate == null ? null : endpointTemplate;
        Long mrs = maxResponseSize == null ? 1_048_576L : maxResponseSize;
        Duration aw = aggregationWindow == null ? Duration.ofSeconds(60) : aggregationWindow;
        Duration sr = sampleRetention == null ? Duration.ofSeconds(120) : sampleRetention;
        Integer msc = maxSeriesCount == null ? 10000 : maxSeriesCount;
        boolean exportTopicTagsFlag = false; // default: avoid high-cardinality topic tags
        List<String> amn = allowedMetricNames == null ? List.of() : List.copyOf(allowedMetricNames);
        Boolean diagEnabled = diagnosticsEnabled == null ? Boolean.FALSE : diagnosticsEnabled;
        Integer diagMax = diagnosticsMaxItems == null ? 500 : diagnosticsMaxItems;

        // validations
        validatePositive("app.metrics.prometheus-scrape.poll-interval", pi);
        validateNonNegative("app.metrics.prometheus-scrape.max-response-size", mrs);
        validatePositive("app.metrics.prometheus-scrape.aggregation-window", aw);
        validatePositive("app.metrics.prometheus-scrape.sample-retention", sr);
        if (msc <= 0) throw new IllegalArgumentException("app.metrics.prometheus-scrape.max-series-count must be > 0");

        // assign normalized
        enabled = e;
        pollInterval = pi;
        connectTimeout = ct;
        readTimeout = rt;
        scrapePath = sp;
        endpointTemplate = et;
        maxResponseSize = mrs;
        aggregationWindow = aw;
        sampleRetention = sr;
        maxSeriesCount = msc;
        allowedMetricNames = amn;
        // topic tag export default
        // Note: kept as non-configured here; to enable, add property app.metrics.prometheus-scrape.export-topic-tags in
        // application config
        // For property binding, users can add an explicit record component later if desired.
        diagnosticsEnabled = diagEnabled;
        diagnosticsMaxItems = diagMax;
    }

    private static void validatePositive(String name, Duration d) {
        if (d == null || d.compareTo(Duration.ZERO) <= 0) throw new IllegalArgumentException(name + " must be > 0");
    }

    private static void validateNonNegative(String name, long v) {
        if (v < 0) throw new IllegalArgumentException(name + " must be >= 0");
    }
}
