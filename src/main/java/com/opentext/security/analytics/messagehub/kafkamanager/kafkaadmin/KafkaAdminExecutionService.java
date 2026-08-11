package com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin;

import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiErrorCode;
import com.opentext.security.analytics.messagehub.kafkamanager.common.KafkaAdminException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class KafkaAdminExecutionService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminExecutionService.class);
    private static final String CLUSTER_ID_TAG = "clusterId";

    private final AdminClientRegistry registry;
    private final MeterRegistry meterRegistry;

    public KafkaAdminExecutionService(AdminClientRegistry registry, MeterRegistry meterRegistry) {
        this.registry = registry;
        this.meterRegistry = meterRegistry;
    }

    @CircuitBreaker(name = "kafkaAdmin", fallbackMethod = "circuitBreakerFallback")
    public <T> T execute(
            UUID clusterId,
            String action,
            Duration timeout,
            Function<AdminClientRegistry.AdminClientHandle, T> callback) {
        Timer.Sample sample = Timer.start(meterRegistry);
        String clusterIdStr = clusterId.toString();
        MDC.put(CLUSTER_ID_TAG, clusterIdStr);
        MDC.put("adminAction", action);
        AdminClientRegistry.AdminClientHandle handle = registry.get(clusterId);
        try {
            return callback.apply(handle);
        } catch (KafkaAdminException exception) {
            meterRegistry
                    .counter(
                            "kafka.manager.admin.errors",
                            CLUSTER_ID_TAG,
                            clusterIdStr,
                            "action",
                            action,
                            "category",
                            exception.getKafkaCategory())
                    .increment();
            throw exception;
        } catch (RuntimeException exception) {
            throw translate(clusterId, action, exception);
        } finally {
            sample.stop(meterRegistry.timer(
                    "kafka.manager.admin.requests", CLUSTER_ID_TAG, clusterIdStr, "action", action));
            MDC.remove(CLUSTER_ID_TAG);
            MDC.remove("adminAction");
        }
    }

    public <T> T await(UUID clusterId, String action, Duration timeout, CompletableFuture<T> future) {
        try {
            return future.get(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (TimeoutException exception) {
            throw new KafkaAdminException(
                    HttpStatus.GATEWAY_TIMEOUT, ApiErrorCode.KAFKA_TIMEOUT, "TIMEOUT", "Kafka operation timed out");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new KafkaAdminException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    ApiErrorCode.KAFKA_CONNECTIVITY_FAILURE,
                    "INTERRUPTED",
                    "Kafka operation interrupted");
        } catch (ExecutionException exception) {
            throw translate(clusterId, action, exception.getCause() == null ? exception : exception.getCause());
        }
    }

    public <T> T await(UUID clusterId, String action, Duration timeout, KafkaFuture<T> future) {
        try {
            return future.get(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (TimeoutException exception) {
            throw new KafkaAdminException(
                    HttpStatus.GATEWAY_TIMEOUT, ApiErrorCode.KAFKA_TIMEOUT, "TIMEOUT", "Kafka operation timed out");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new KafkaAdminException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    ApiErrorCode.KAFKA_CONNECTIVITY_FAILURE,
                    "INTERRUPTED",
                    "Kafka operation interrupted");
        } catch (ExecutionException exception) {
            throw translate(clusterId, action, exception.getCause() == null ? exception : exception.getCause());
        }
    }

    private <T> T circuitBreakerFallback(
            UUID clusterId,
            String action,
            Duration timeout,
            Function<AdminClientRegistry.AdminClientHandle, T> callback,
            Throwable throwable) {
        log.warn(
                "Circuit breaker fallback triggered for clusterId={}, action={}: {}",
                clusterId,
                action,
                throwable.getMessage());
        throw translate(clusterId, action, throwable);
    }

    public KafkaAdminException translate(UUID clusterId, String action, Throwable throwable) {
        log.error(
                "Kafka admin action failed: clusterId={}, action={}, cause={}: {}",
                clusterId,
                action,
                throwable.getClass().getName(),
                throwable.getMessage(),
                throwable);
        if (throwable instanceof AuthenticationException) {
            return new KafkaAdminException(
                    HttpStatus.UNAUTHORIZED,
                    ApiErrorCode.KAFKA_AUTHENTICATION_FAILURE,
                    "AUTHENTICATION_FAILURE",
                    "Kafka authentication failed");
        }
        if (throwable instanceof AuthorizationException) {
            return new KafkaAdminException(
                    HttpStatus.FORBIDDEN,
                    ApiErrorCode.KAFKA_AUTHORIZATION_FAILURE,
                    "AUTHORIZATION_FAILURE",
                    "Kafka authorization failed");
        }
        if (throwable instanceof org.apache.kafka.common.errors.TimeoutException
                || throwable instanceof TimeoutException) {
            return new KafkaAdminException(
                    HttpStatus.GATEWAY_TIMEOUT, ApiErrorCode.KAFKA_TIMEOUT, "TIMEOUT", "Kafka operation timed out");
        }
        return new KafkaAdminException(
                HttpStatus.BAD_GATEWAY,
                ApiErrorCode.KAFKA_CONNECTIVITY_FAILURE,
                "CONNECTIVITY_FAILURE",
                "Kafka connectivity failure");
    }
}
