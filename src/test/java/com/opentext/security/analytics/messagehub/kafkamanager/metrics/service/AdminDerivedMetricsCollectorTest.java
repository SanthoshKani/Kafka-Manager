package com.opentext.security.analytics.messagehub.kafkamanager.metrics.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiErrorCode;
import com.opentext.security.analytics.messagehub.kafkamanager.common.KafkaAdminException;
import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.BrokerLeaderCount;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
class AdminDerivedMetricsCollectorTest {

    private static final UUID CLUSTER_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    @Mock
    private Admin admin;

    @Mock
    private KafkaAdminExecutionService unavailableExecutionService;

    private AdminDerivedMetricsCollector collector;
    private AdminDerivedMetricsCollector unavailableCollector;

    @BeforeEach
    void setUp() {
        collector = new AdminDerivedMetricsCollector(
                new KafkaAdminExecutionService(admin, new SimpleMeterRegistry()),
                properties(List.of("^__.*", ".*-internal$")));
        unavailableCollector = new AdminDerivedMetricsCollector(unavailableExecutionService, properties(List.of()));
    }

    @Test
    void collectsSuccessfulSnapshot() {
        configureDescribeCluster(List.of(node(1), node(2)), node(1));
        configureListTopics(Map.of(
                "orders", listing("orders", false),
                "payments", listing("payments", false)));
        configureDescribeTopics(Map.of(
                "orders", topic("orders", false, List.of(partition(0, 1, 2, 2), partition(1, 2, 2, 2))),
                "payments", topic("payments", true, List.of(partition(0, 1, 2, 1)))));

        AdminClientMetricsSnapshot snapshot = collector.collect(CLUSTER_ID);

        assertThat(snapshot.clusterId()).isEqualTo(CLUSTER_ID);
        assertThat(snapshot.brokerCount()).isEqualTo(2);
        assertThat(snapshot.topicCount()).isEqualTo(2);
        assertThat(snapshot.partitionCount()).isEqualTo(3);
        assertThat(snapshot.offlinePartitionCount()).isZero();
        assertThat(snapshot.underReplicatedPartitionCount()).isEqualTo(1);
        assertThat(snapshot.controllerBrokerId()).isEqualTo(1);
        assertThat(snapshot.collectionStatus()).isEqualTo(CollectionStatus.SUCCESS);
        assertThat(snapshot.lastSuccessfulCollectionAt()).isEqualTo(snapshot.collectedAt());
        assertThat(snapshot.sanitizedFailureReason()).isNull();
        assertThat(snapshot.brokerLeaderCounts())
                .containsExactly(new BrokerLeaderCount(1, 2), new BrokerLeaderCount(2, 1));
    }

    @Test
    void collectsNoTopics() {
        configureDescribeCluster(List.of(node(1), node(2)), node(1));
        configureListTopics(Map.of());

        AdminClientMetricsSnapshot snapshot = collector.collect(CLUSTER_ID);

        assertThat(snapshot.topicCount()).isZero();
        assertThat(snapshot.partitionCount()).isZero();
        assertThat(snapshot.brokerCount()).isEqualTo(2);
        assertThat(snapshot.brokerLeaderCounts())
                .containsExactly(new BrokerLeaderCount(1, 0), new BrokerLeaderCount(2, 0));
        verify(admin, never()).describeTopics(anyList());
    }

    @Test
    void excludesConfiguredTopicsBeforeDescribe() {
        configureDescribeCluster(List.of(node(1)), node(1));
        configureListTopics(Map.of(
                "orders", listing("orders", false),
                "__consumer_offsets", listing("__consumer_offsets", true),
                "payments-internal", listing("payments-internal", false)));
        configureDescribeTopics(Map.of("orders", topic("orders", false, List.of(partition(0, 1, 1, 1)))));

        AdminClientMetricsSnapshot snapshot = collector.collect(CLUSTER_ID);

        verify(admin).describeTopics(List.of("orders"));
        assertThat(snapshot.topicCount()).isEqualTo(1);
        assertThat(snapshot.partitionCount()).isEqualTo(1);
    }

    @Test
    void propagatesDescribeClusterFailure() {
        DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
        KafkaFuture<Collection<Node>> nodesFuture = failingFuture(new ExecutionException(new RuntimeException("boom")));
        when(describeClusterResult.nodes()).thenReturn(nodesFuture);
        when(admin.describeCluster()).thenReturn(describeClusterResult);

        assertThatThrownBy(() -> collector.collect(CLUSTER_ID))
                .isInstanceOf(KafkaAdminException.class)
                .extracting(throwable -> ((KafkaAdminException) throwable).getErrorCode())
                .isEqualTo(ApiErrorCode.KAFKA_CONNECTIVITY_FAILURE);
        verify(admin, never()).listTopics(any(ListTopicsOptions.class));
    }

    @Test
    void propagatesListTopicsFailure() {
        configureDescribeCluster(List.of(node(1)), node(1));
        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        when(listTopicsResult.namesToListings())
                .thenReturn(failingFuture(new ExecutionException(new RuntimeException("boom"))));
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(listTopicsResult);

        assertThatThrownBy(() -> collector.collect(CLUSTER_ID))
                .isInstanceOf(KafkaAdminException.class)
                .extracting(throwable -> ((KafkaAdminException) throwable).getErrorCode())
                .isEqualTo(ApiErrorCode.KAFKA_CONNECTIVITY_FAILURE);
        verify(admin, never()).describeTopics(anyList());
    }

    @Test
    void propagatesDescribeTopicsFailure() {
        configureDescribeCluster(List.of(node(1)), node(1));
        configureListTopics(Map.of("orders", listing("orders", false)));
        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        when(describeTopicsResult.allTopicNames())
                .thenReturn(failingFuture(new ExecutionException(new RuntimeException("boom"))));
        when(admin.describeTopics(List.of("orders"))).thenReturn(describeTopicsResult);

        assertThatThrownBy(() -> collector.collect(CLUSTER_ID))
                .isInstanceOf(KafkaAdminException.class)
                .extracting(throwable -> ((KafkaAdminException) throwable).getErrorCode())
                .isEqualTo(ApiErrorCode.KAFKA_CONNECTIVITY_FAILURE);
    }

    @Test
    void propagatesTimeout() {
        DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
        when(describeClusterResult.nodes()).thenReturn(failingFuture(new TimeoutException("timed out")));
        when(admin.describeCluster()).thenReturn(describeClusterResult);

        assertThatThrownBy(() -> collector.collect(CLUSTER_ID))
                .isInstanceOf(KafkaAdminException.class)
                .extracting(throwable -> ((KafkaAdminException) throwable).getErrorCode())
                .isEqualTo(ApiErrorCode.KAFKA_TIMEOUT);
    }

    @Test
    void handlesControllerAbsent() {
        configureDescribeCluster(List.of(node(1)), Node.noNode());
        configureListTopics(Map.of("orders", listing("orders", false)));
        configureDescribeTopics(Map.of("orders", topic("orders", false, List.of(partition(0, 1, 1, 1)))));

        AdminClientMetricsSnapshot snapshot = collector.collect(CLUSTER_ID);

        assertThat(snapshot.controllerBrokerId()).isNull();
    }

    @Test
    void propagatesWhenAdminClientIsUnavailableForCluster() {
        KafkaAdminException exception = new KafkaAdminException(
                HttpStatus.NOT_FOUND, ApiErrorCode.NOT_FOUND, "CLUSTER_UNAVAILABLE", "Kafka cluster unavailable");
        doThrow(exception).when(unavailableExecutionService).execute(any(), any(), any(), any());

        assertThatThrownBy(() -> unavailableCollector.collect(CLUSTER_ID)).isSameAs(exception);
    }

    private void configureDescribeCluster(Collection<Node> brokers, Node controller) {
        DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
        when(describeClusterResult.nodes()).thenReturn(successfulFuture(brokers));
        when(describeClusterResult.controller()).thenReturn(successfulFuture(controller));
        when(admin.describeCluster()).thenReturn(describeClusterResult);
    }

    private void configureListTopics(Map<String, TopicListing> listings) {
        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        when(listTopicsResult.namesToListings()).thenReturn(successfulFuture(listings));
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(listTopicsResult);
    }

    private void configureDescribeTopics(Map<String, TopicDescription> descriptions) {
        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        when(describeTopicsResult.allTopicNames()).thenReturn(successfulFuture(descriptions));
        when(admin.describeTopics(anyList())).thenReturn(describeTopicsResult);
    }

    private static <T> KafkaFuture<T> successfulFuture(T value) {
        return KafkaFuture.completedFuture(value);
    }

    private static <T> KafkaFuture<T> failingFuture(Throwable exception) {
        KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
        future.completeExceptionally(exception);
        return future;
    }

    private TopicListing listing(String name, boolean internal) {
        return new TopicListing(name, Uuid.ZERO_UUID, internal);
    }

    private TopicDescription topic(String name, boolean internal, List<TopicPartitionInfo> partitions) {
        return new TopicDescription(name, internal, partitions);
    }

    private TopicPartitionInfo partition(int partition, Integer leaderId, int replicaCount, int isrCount) {
        Node leader = leaderId == null ? null : node(leaderId);
        return new TopicPartitionInfo(partition, leader, nodes(replicaCount), nodes(isrCount));
    }

    private List<Node> nodes(int count) {
        return java.util.stream.IntStream.range(0, count).mapToObj(this::node).toList();
    }

    private Node node(int id) {
        return new Node(id, "broker" + id, 9092);
    }

    private KafkaManagerProperties properties(List<String> exclusions) {
        KafkaManagerProperties.Admin admin = new KafkaManagerProperties.Admin(
                "localhost:9092", "PLAINTEXT", null, Duration.ofSeconds(30), Duration.ofSeconds(5));
        KafkaManagerProperties.AdminDerived adminDerived =
                new KafkaManagerProperties.AdminDerived(true, Duration.ofSeconds(5), Duration.ofSeconds(5), exclusions);
        return new KafkaManagerProperties(
                "kafka-manager", null, admin, new KafkaManagerProperties.Metrics(adminDerived), null);
    }
}
