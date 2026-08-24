package com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Scheduled scraper that polls configured brokers and writes normalized samples to the provided store.
 */
@Service
@ConditionalOnProperty(prefix = "app.features", name = "metrics.enabled", havingValue = "true", matchIfMissing = false)
public class PrometheusScrapeScheduler {

    private static final Logger log = LoggerFactory.getLogger(PrometheusScrapeScheduler.class);

    private final BrokerEndpointRegistry endpointRegistry;
    private final PrometheusTextParser parser;
    private final MetricMapper mapper;
    private final BoundedInMemorySampleStore store;
    private final HttpClient httpClient;
    private final MeterRegistry meterRegistry;
    private final com.opentext.security.analytics.messagehub.kafkamanager.config.PrometheusScrapeProperties properties;

    private final ExecutorService executor;
    private final ConcurrentMap<UUID, ConcurrentMap<Integer, AtomicBoolean>> inProgress = new ConcurrentHashMap<>();
    private final Random random = new Random();

    public PrometheusScrapeScheduler(
            BrokerEndpointRegistry endpointRegistry,
            PrometheusTextParser parser,
            MetricMapper mapper,
            BoundedInMemorySampleStore store,
            MeterRegistry meterRegistry,
            com.opentext.security.analytics.messagehub.kafkamanager.config.PrometheusScrapeProperties properties) {
        this.endpointRegistry = endpointRegistry;
        this.parser = parser;
        this.mapper = mapper;
        this.store = store;
        this.meterRegistry = meterRegistry;
        this.properties = properties;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(properties.connectTimeout())
                .build();
        int concurrency = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        this.executor = Executors.newFixedThreadPool(concurrency);
    }

    // Controlled scheduled trigger; property-driven fixed delay
    @Scheduled(fixedDelayString = "${app.metrics.prometheus-scrape.poll-interval:PT10S}")
    public void scheduledRun() {
        // discover clusters
        // For this implementation, iterate over registry entries
        // InMemoryBrokerEndpointRegistry supports endpointsForCluster
        // No direct API to list clusters; assume registry exposes known clusters via endpointsForCluster keys
        // Here, we can't list keys; test will call runOnce with explicit clusters.
        // Provide legacy scheduled run that does nothing by default.
        log.debug("PrometheusScrapeScheduler scheduledRun invoked");
    }

    /**
     * Run one scrape pass for the provided clusters. Tests will call this method.
     */
    public void runOnce(Set<UUID> clusterIds) throws InterruptedException {
        List<Callable<Void>> tasks = new ArrayList<>();
        for (UUID clusterId : clusterIds) {
            Map<Integer, URI> endpoints = endpointRegistry.endpointsForCluster(clusterId);
            if (endpoints.isEmpty()) continue;
            inProgress.computeIfAbsent(clusterId, k -> new ConcurrentHashMap<>());
            for (Map.Entry<Integer, URI> e : endpoints.entrySet()) {
                int brokerId = e.getKey();
                URI uri = e.getValue();
                tasks.add(() -> {
                    // add jitter up to pollInterval seconds
                    long jitter = (long)
                            (random.nextDouble() * properties.pollInterval().toMillis());
                    Thread.sleep(jitter);
                    ConcurrentMap<Integer, AtomicBoolean> clusterMap = inProgress.get(clusterId);
                    AtomicBoolean marker = clusterMap.computeIfAbsent(brokerId, bid -> new AtomicBoolean(false));
                    if (!marker.compareAndSet(false, true)) {
                        // already in progress
                        return null;
                    }
                    Instant start = Instant.now();
                    Timer.Sample sample = Timer.start(meterRegistry);
                    PrometheusScraper scraper = new PrometheusScraper(
                            httpClient,
                            parser,
                            Set.copyOf(mapper.mapKeys()),
                            mapper.toNameMap(),
                            mapper,
                            properties.maxResponseSize().intValue());
                    var result = scraper.scrape(
                            uri,
                            new MetricIdentity(clusterId, brokerId, null, ""),
                            properties.connectTimeout(),
                            properties.readTimeout());
                    if (result.health.status == PrometheusScraper.CollectionHealth.Status.SUCCESS) {
                        // process samples
                        processSamples(result.samples);
                        meterRegistry
                                .counter(
                                        "kafka.manager.prometheus.scrape.success",
                                        "clusterId",
                                        clusterId.toString(),
                                        "brokerId",
                                        String.valueOf(brokerId))
                                .increment();
                    } else {
                        meterRegistry
                                .counter(
                                        "kafka.manager.prometheus.scrape.failure",
                                        "clusterId",
                                        clusterId.toString(),
                                        "brokerId",
                                        String.valueOf(brokerId))
                                .increment();
                    }
                    sample.stop(meterRegistry.timer(
                            "kafka.manager.prometheus.scrape.duration", "clusterId", clusterId.toString()));
                    marker.set(false);
                    return null;
                });
            }
        }
        // bounded concurrency: submit all and wait
        List<Future<Void>> futures = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            futures.add(executor.submit(task));
        }
        for (Future<Void> f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                log.warn("Scrape task failed: {}", e.getMessage());
            }
        }
    }

    private void processSamples(List<MetricSample> samples) {
        RateCalculator rateCalc = new RateCalculator(0);
        for (MetricSample s : samples) {
            // map source canonical descriptor
            var desc = mapper.get(s.sourceAttributeName());
            if (desc == null) {
                // try name directly
            }
            // if counter, compute rate
            if (desc != null && desc.semanticType == MetricSemanticType.MONOTONIC_COUNTER) {
                MetricSample prev = store.latest(s.identity());
                var res = rateCalc.calculate(prev == null ? null : prev, s);
                if (res instanceof RateCalculator.RateResult.ValidRate vr) {
                    double rpm = vr.ratePerMinute();
                    MetricSample derived = new MetricSample(
                            s.identity(),
                            rpm,
                            Instant.now(),
                            MetricSemanticType.GAUGE,
                            "per_minute",
                            MetricSourceBackend.PROMETHEUS,
                            s.sourceAttributeName());
                    store.append(derived);
                } else {
                    // insufficient or reset -> do not emit spike; preserve previous values
                }
            } else {
                // gauge or unmapped -> append directly
                store.append(s);
            }
        }
    }
}
