package com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiErrorCode;
import com.opentext.security.analytics.messagehub.kafkamanager.common.KafkaAdminException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.errors.AuthenticationException;
import org.junit.jupiter.api.Test;

class KafkaAdminExecutionServiceTest {

    private final AdminClientRegistry registry = mock(AdminClientRegistry.class);
    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final KafkaAdminExecutionService service = new KafkaAdminExecutionService(registry, meterRegistry);

    @Test
    void executeTranslatesKafkaFailuresIntoKafkaAdminException() {
        UUID clusterId = UUID.randomUUID();
        Admin admin = mock(Admin.class);
        try {
            when(registry.get(clusterId))
                    .thenReturn(new AdminClientRegistry.AdminClientHandle(admin, "fingerprint", null));

            Function<AdminClientRegistry.AdminClientHandle, String> failingCallback = handle -> {
                throw new IllegalStateException("boom");
            };

            assertThatThrownBy(
                            () -> service.execute(clusterId, "failing-action", Duration.ofSeconds(1), failingCallback))
                    .isInstanceOfSatisfying(KafkaAdminException.class, exception -> assertThat(exception.getErrorCode())
                            .isEqualTo(ApiErrorCode.KAFKA_CONNECTIVITY_FAILURE));

            verify(registry).get(clusterId);
        } finally {
            admin.close();
        }
    }

    @Test
    void translateMapsAuthenticationFailures() {
        KafkaAdminException exception =
                service.translate(UUID.randomUUID(), "authenticate", new AuthenticationException("bad creds"));

        assertThat(exception.getErrorCode()).isEqualTo(ApiErrorCode.KAFKA_AUTHENTICATION_FAILURE);
    }
}
