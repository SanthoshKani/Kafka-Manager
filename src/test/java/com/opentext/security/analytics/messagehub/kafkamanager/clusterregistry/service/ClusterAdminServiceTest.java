package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api.LeaderElectionRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiException;
import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.service.AdminMutationRecorder;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.common.ElectionType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ClusterAdminServiceTest {

    private final KafkaManagerProperties properties = new KafkaManagerProperties(
            "test",
            new KafkaManagerProperties.Security(
                    "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
                    new KafkaManagerProperties.BasicAuth("testuser", "testpass"),
                    new KafkaManagerProperties.OAuth2ResourceServer("", "")),
            new KafkaManagerProperties.Admin(4, Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1)),
            new KafkaManagerProperties.Operations(Duration.ofSeconds(1), Duration.ofSeconds(1)),
            new KafkaManagerProperties.ClusterRegistry(50, 8),
            new KafkaManagerProperties.RateLimit(true, 100, Duration.ofMinutes(1), "X-Client-Id"));

    @Mock
    KafkaAdminExecutionService adminExecutionService;

    @Mock
    AdminMutationRecorder mutationRecorder;

    @Test
    void rejectsUncleanLeaderElectionWithoutExplicitPartitions() {
        ClusterAdminService service = new ClusterAdminService(adminExecutionService, mutationRecorder, properties);

        assertThatThrownBy(() -> service.electLeaders(
                        java.util.UUID.randomUUID(), new LeaderElectionRequest(ElectionType.UNCLEAN, List.of())))
                .isInstanceOf(ApiException.class);
    }
}
