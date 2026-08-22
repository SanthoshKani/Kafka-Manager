package com.opentext.security.analytics.messagehub.kafkamanager.metrics.service;

import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Collects Kafka broker-side JMX metrics for request rates, latencies and topic throughput.
 *
 * <p>The collector polls configured broker JMX endpoints, reads Kafka 4.3.x metric families, and
 * publishes the latest values to Micrometer so they can be scraped by Prometheus.
 */
@Service
public class BrokerJmxMetricsCollectorService {

    private static final Logger log = LoggerFactory.getLogger(BrokerJmxMetricsCollectorService.class);

    private static final List<String> REQUEST_TYPES = List.of("Produce", "FetchConsumer", "FetchFollower");
    private static final List<String> TOPIC_METRICS = List.of(
            "BytesInPerSec",
            "BytesOutPerSec",
            "MessagesInPerSec",
            "FailedFetchRequestsPerSec",
            "FailedProduceRequestsPerSec",
            "BytesRejectedPerSec");

    private final KafkaManagerProperties properties;
    private final MeterRegistry meterRegistry;
    private final AtomicReference<BrokerJmxSnapshot> latestSnapshot;
    private final ConcurrentHashMap<String, AtomicReference<Double>> gaugeValues;

    public BrokerJmxMetricsCollectorService(KafkaManagerProperties properties, MeterRegistry meterRegistry) {
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.latestSnapshot = new AtomicReference<>(BrokerJmxSnapshot.empty());
        this.gaugeValues = new ConcurrentHashMap<>();
    }

    /**
     * Warm up the broker JMX snapshot once the application is ready.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void warmUp() {
        refresh();
    }

    /**
     * Refresh the broker JMX snapshot on a fixed schedule.
     */
    @Scheduled(fixedDelayString = "${app.metrics.broker-jmx.poll-interval:30s}")
    public void refresh() {
        KafkaManagerProperties.BrokerJmx config = brokerJmxConfig();
        if (config == null || !config.enabled() || config.targets().isEmpty()) {
            return;
        }

        try {
            BrokerJmxSnapshot snapshot = collectSnapshot(config);
            latestSnapshot.set(snapshot);
            publish(snapshot);
        } catch (RuntimeException exception) {
            log.warn("Unable to refresh broker JMX metrics: {}", exception.getMessage(), exception);
        }
    }

    /**
     * Return the latest broker JMX snapshot captured by the collector.
     *
     * @return latest broker JMX snapshot
     */
    public BrokerJmxSnapshot current() {
        return latestSnapshot.get();
    }

    private KafkaManagerProperties.BrokerJmx brokerJmxConfig() {
        KafkaManagerProperties.Metrics metrics = properties.metrics();
        return metrics == null ? null : metrics.brokerJmx();
    }

    private BrokerJmxSnapshot collectSnapshot(KafkaManagerProperties.BrokerJmx config) {
        List<BrokerNodeSnapshot> brokers = new ArrayList<>();
        for (KafkaManagerProperties.BrokerJmxTarget target : config.targets()) {
            brokers.add(collectBroker(target));
        }
        return new BrokerJmxSnapshot(Instant.now(), List.copyOf(brokers));
    }

    private BrokerNodeSnapshot collectBroker(KafkaManagerProperties.BrokerJmxTarget target) {
        Map<String, RequestMetricsSnapshot> requestMetrics = new LinkedHashMap<>();
        Map<String, TopicMetricsAccumulator> topicMetrics = new LinkedHashMap<>();
        double networkProcessorAvgIdlePercent = 0.0;
        double requestHandlerAvgIdlePercent = 0.0;
        double logFlushRate = 0.0;
        String errorMessage = null;

        try (JMXConnector connector = connect(target)) {
            MBeanServerConnection connection = connector.getMBeanServerConnection();

            networkProcessorAvgIdlePercent = readMetric(
                    connection, "kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent", "Value");
            requestHandlerAvgIdlePercent = readMetric(
                    connection, "kafka.network:type=SocketServer,name=RequestHandlerAvgIdlePercent", "Value");
            logFlushRate = readFirstAvailableMetric(
                    connection,
                    List.of(
                            "kafka.log:type=LogFlushStats,name=LogFlushRate",
                            "kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs"),
                    "OneMinuteRate",
                    "MeanRate",
                    "Value");

            for (String requestType : REQUEST_TYPES) {
                double requestsPerSec = readMetric(
                        connection,
                        "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=" + requestType,
                        "OneMinuteRate",
                        "MeanRate",
                        "Count");
                double meanLatencyMs = readMetric(
                        connection,
                        "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=" + requestType,
                        "Mean",
                        "Value");
                double maxLatencyMs = readMetric(
                        connection,
                        "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=" + requestType,
                        "Max",
                        "Value");
                requestMetrics.put(
                        requestType,
                        new RequestMetricsSnapshot(requestType, requestsPerSec, meanLatencyMs, maxLatencyMs));
            }

            for (String metricName : TOPIC_METRICS) {
                Set<ObjectName> names = connection.queryNames(
                        new ObjectName("kafka.server:type=BrokerTopicMetrics,name=" + metricName + ",topic=*"), null);
                for (ObjectName objectName : names) {
                    String topic = objectName.getKeyProperty("topic");
                    if (topic == null || topic.isBlank()) {
                        continue;
                    }
                    TopicMetricsAccumulator accumulator =
                            topicMetrics.computeIfAbsent(topic, TopicMetricsAccumulator::new);
                    double value = readMetric(connection, objectName, "OneMinuteRate", "MeanRate", "Value", "Count");
                    accumulator.set(metricName, value);
                }
            }
        } catch (Exception exception) {
            errorMessage = exception.getClass().getSimpleName() + ": " + exception.getMessage();
        }

        List<TopicMetricsSnapshot> topics = topicMetrics.values().stream()
                .map(TopicMetricsAccumulator::toSnapshot)
                .sorted((left, right) -> left.topic().compareToIgnoreCase(right.topic()))
                .toList();

        return new BrokerNodeSnapshot(
                target.name(),
                target.host(),
                target.port(),
                errorMessage,
                networkProcessorAvgIdlePercent,
                requestHandlerAvgIdlePercent,
                logFlushRate,
                Map.copyOf(requestMetrics),
                topics);
    }

    private void publish(BrokerJmxSnapshot snapshot) {
        gaugeValues.values().forEach(ref -> ref.set(0.0));
        for (BrokerNodeSnapshot broker : snapshot.brokers()) {
            setGauge(
                    "kafka.manager.broker.jmx.network.processor.idle.percent",
                    broker.networkProcessorAvgIdlePercent(),
                    "broker",
                    broker.name());
            setGauge(
                    "kafka.manager.broker.jmx.request.handler.idle.percent",
                    broker.requestHandlerAvgIdlePercent(),
                    "broker",
                    broker.name());
            setGauge("kafka.manager.broker.jmx.log.flush.rate", broker.logFlushRate(), "broker", broker.name());

            for (RequestMetricsSnapshot request : broker.requests().values()) {
                setGauge(
                        "kafka.manager.broker.jmx.requests.rate",
                        request.requestsPerSec(),
                        "broker",
                        broker.name(),
                        "request",
                        request.requestType());
                setGauge(
                        "kafka.manager.broker.jmx.request.latency.mean.ms",
                        request.meanLatencyMs(),
                        "broker",
                        broker.name(),
                        "request",
                        request.requestType());
                setGauge(
                        "kafka.manager.broker.jmx.request.latency.max.ms",
                        request.maxLatencyMs(),
                        "broker",
                        broker.name(),
                        "request",
                        request.requestType());
            }

            for (TopicMetricsSnapshot topic : broker.topics()) {
                setGauge(
                        "kafka.manager.broker.jmx.topic.bytes.in.rate",
                        topic.bytesInPerSec(),
                        "broker",
                        broker.name(),
                        "topic",
                        topic.topic());
                setGauge(
                        "kafka.manager.broker.jmx.topic.bytes.out.rate",
                        topic.bytesOutPerSec(),
                        "broker",
                        broker.name(),
                        "topic",
                        topic.topic());
                setGauge(
                        "kafka.manager.broker.jmx.topic.messages.in.rate",
                        topic.messagesInPerSec(),
                        "broker",
                        broker.name(),
                        "topic",
                        topic.topic());
                setGauge(
                        "kafka.manager.broker.jmx.topic.failed.fetch.requests.rate",
                        topic.failedFetchRequestsPerSec(),
                        "broker",
                        broker.name(),
                        "topic",
                        topic.topic());
                setGauge(
                        "kafka.manager.broker.jmx.topic.failed.produce.requests.rate",
                        topic.failedProduceRequestsPerSec(),
                        "broker",
                        broker.name(),
                        "topic",
                        topic.topic());
                setGauge(
                        "kafka.manager.broker.jmx.topic.bytes.rejected.rate",
                        topic.bytesRejectedPerSec(),
                        "broker",
                        broker.name(),
                        "topic",
                        topic.topic());
            }
        }
    }

    private void setGauge(String meterName, double value, String... tags) {
        metricRef(meterName, tags).set(value);
    }

    private AtomicReference<Double> metricRef(String meterName, String... tags) {
        String key = meterKey(meterName, tags);
        return gaugeValues.computeIfAbsent(key, ignored -> {
            AtomicReference<Double> ref = new AtomicReference<>(0.0);
            Gauge.builder(meterName, ref, value -> value.get() == null ? 0.0 : value.get())
                    .tags(tags)
                    .register(meterRegistry);
            return ref;
        });
    }

    private String meterKey(String meterName, String... tags) {
        return meterName + "|" + String.join("|", tags);
    }

    private JMXConnector connect(KafkaManagerProperties.BrokerJmxTarget target) throws IOException {
        JMXServiceURL url =
                new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + target.host() + ":" + target.port() + "/jmxrmi");
        return JMXConnectorFactory.connect(url, Collections.emptyMap());
    }

    private double readFirstAvailableMetric(
            MBeanServerConnection connection, List<String> objectNames, String... attributes) {
        for (String objectName : objectNames) {
            try {
                Set<ObjectName> names = connection.queryNames(new ObjectName(objectName), null);
                for (ObjectName name : names) {
                    return readMetric(connection, name, attributes);
                }
            } catch (Exception ignored) {
                // try the next object name
            }
        }
        return 0.0;
    }

    private double readMetric(MBeanServerConnection connection, String objectName, String... attributes) {
        try {
            Set<ObjectName> names = connection.queryNames(new ObjectName(objectName), null);
            for (ObjectName name : names) {
                return readMetric(connection, name, attributes);
            }
        } catch (Exception ignored) {
            // fall through to 0.0
        }
        return 0.0;
    }

    private double readMetric(MBeanServerConnection connection, ObjectName objectName, String... attributes) {
        for (String attribute : attributes) {
            try {
                Object value = connection.getAttribute(objectName, attribute);
                if (value instanceof Number number) {
                    return number.doubleValue();
                }
                if (value != null) {
                    return Double.parseDouble(value.toString());
                }
            } catch (Exception ignored) {
                // try the next attribute
            }
        }
        return 0.0;
    }

    /**
     * Latest broker JMX snapshot.
     */
    public record BrokerJmxSnapshot(Instant capturedAt, List<BrokerNodeSnapshot> brokers) {
        static BrokerJmxSnapshot empty() {
            return new BrokerJmxSnapshot(Instant.EPOCH, List.of());
        }
    }

    /**
     * Per-broker JMX metrics.
     */
    public record BrokerNodeSnapshot(
            String name,
            String host,
            int port,
            String errorMessage,
            double networkProcessorAvgIdlePercent,
            double requestHandlerAvgIdlePercent,
            double logFlushRate,
            Map<String, RequestMetricsSnapshot> requests,
            List<TopicMetricsSnapshot> topics) {}

    /**
     * Per-request JMX metrics.
     */
    public record RequestMetricsSnapshot(
            String requestType, double requestsPerSec, double meanLatencyMs, double maxLatencyMs) {}

    /**
     * Per-topic JMX metrics.
     */
    public record TopicMetricsSnapshot(
            String topic,
            double bytesInPerSec,
            double bytesOutPerSec,
            double messagesInPerSec,
            double failedFetchRequestsPerSec,
            double failedProduceRequestsPerSec,
            double bytesRejectedPerSec) {}

    private static final class TopicMetricsAccumulator {
        private final String topic;
        private double bytesInPerSec;
        private double bytesOutPerSec;
        private double messagesInPerSec;
        private double failedFetchRequestsPerSec;
        private double failedProduceRequestsPerSec;
        private double bytesRejectedPerSec;

        private TopicMetricsAccumulator(String topic) {
            this.topic = Objects.requireNonNull(topic, "topic");
        }

        private void set(String metricName, double value) {
            switch (metricName) {
                case "BytesInPerSec" -> bytesInPerSec = value;
                case "BytesOutPerSec" -> bytesOutPerSec = value;
                case "MessagesInPerSec" -> messagesInPerSec = value;
                case "FailedFetchRequestsPerSec" -> failedFetchRequestsPerSec = value;
                case "FailedProduceRequestsPerSec" -> failedProduceRequestsPerSec = value;
                case "BytesRejectedPerSec" -> bytesRejectedPerSec = value;
                default -> {}
            }
        }

        private TopicMetricsSnapshot toSnapshot() {
            return new TopicMetricsSnapshot(
                    topic,
                    bytesInPerSec,
                    bytesOutPerSec,
                    messagesInPerSec,
                    failedFetchRequestsPerSec,
                    failedProduceRequestsPerSec,
                    bytesRejectedPerSec);
        }
    }
}
