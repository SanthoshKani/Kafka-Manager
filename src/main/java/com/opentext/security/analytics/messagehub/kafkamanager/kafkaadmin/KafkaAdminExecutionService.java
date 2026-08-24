package com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin;

import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiErrorCode;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiException;
import com.opentext.security.analytics.messagehub.kafkamanager.common.KafkaAdminException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Wrapper around Kafka AdminClient interactions that provides resilience, timeouts, metrics and
 * error translation.
 *
 * <p>All AdminClient calls should use this service so that operations are executed with a
 * circuit-breaker, measured with Micrometer timers/counters, and converted into {@code
 * KafkaAdminException} instances that the API layer can map to RFC-9457 problem responses.
 */
@Service
public class KafkaAdminExecutionService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminExecutionService.class);
    private static final String CLUSTER_ID_TAG = "clusterId";

    private final Admin admin;
    private final MeterRegistry meterRegistry;

    public KafkaAdminExecutionService(Admin admin, MeterRegistry meterRegistry) {
        this.admin = admin;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Execute an AdminClient operation with circuit-breaker protection and instrumentation.
     *
     * @param <T> return type of the operation
     * @param clusterId cluster identifier used for tagging metrics and MDC
     * @param action human-readable action name for metrics and logs
     * @param timeout maximum duration to wait for the operation
     * @param callback function that receives an {@link AdminClientHandle} and performs AdminClient calls
     * @return result of the callback
     * @throws KafkaAdminException when underlying Kafka operations fail or are translated
     */
    @SuppressWarnings("unused")
    @CircuitBreaker(name = "kafkaAdmin", fallbackMethod = "circuitBreakerFallback")
    public <T> T execute(UUID clusterId, String action, Duration timeout, Function<AdminClientHandle, T> callback) {
        long timeoutMs = timeout.toMillis();
        Timer.Sample sample = Timer.start(meterRegistry);
        String clusterIdStr = clusterId.toString();
        MDC.put(CLUSTER_ID_TAG, clusterIdStr);
        MDC.put("adminAction", action);
        MDC.put("adminTimeoutMs", Long.toString(timeoutMs));
        AdminClientHandle handle = new AdminClientHandle(admin);
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
        } catch (ApiException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw translate(clusterId, action, exception);
        } finally {
            sample.stop(meterRegistry.timer(
                    "kafka.manager.admin.requests", CLUSTER_ID_TAG, clusterIdStr, "action", action));
            MDC.remove(CLUSTER_ID_TAG);
            MDC.remove("adminAction");
            MDC.remove("adminTimeoutMs");
        }
    }

    /**
     * Await a {@link CompletableFuture} result with a bounded timeout and translate failures.
     *
     * @param <T> result type
     * @param clusterId cluster id for logging/metrics
     * @param action action name for logs
     * @param timeout maximum wait duration
     * @param future future to await
     * @return future result
     * @throws KafkaAdminException on timeout, interruption or execution failure
     */
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

    /**
     * Await a {@link KafkaFuture} result with a bounded timeout and translate failures.
     *
     * @param <T> result type
     * @param clusterId cluster id for logging/metrics
     * @param action action name for logs
     * @param timeout maximum wait duration
     * @param future KafkaFuture to await
     * @return future result
     * @throws KafkaAdminException on timeout, interruption or execution failure
     */
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

    @SuppressWarnings("unused")
    private <T> T circuitBreakerFallback(
            UUID clusterId,
            String action,
            Duration timeout,
            Function<AdminClientHandle, T> callback,
            Throwable throwable) {
        long timeoutMs = timeout.toMillis();
        log.warn(
                "Circuit breaker fallback triggered for clusterId={}, action={}, timeoutMs={}, callbackType={}: {}",
                clusterId,
                action,
                timeoutMs,
                callback == null ? "null" : callback.getClass().getName(),
                throwable.getMessage());
        if (throwable instanceof ApiException apiException) {
            throw apiException;
        }
        throw translate(clusterId, action, throwable);
    }

    /**
     * Translate low-level Kafka or runtime throwables into {@link KafkaAdminException} with
     * appropriate HTTP status and error codes.
     *
     * @param clusterId cluster id used for logging context
     * @param action human readable action name
     * @param throwable the underlying throwable to translate
     * @return translated {@link KafkaAdminException}
     */
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
        if (throwable instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException
                || throwable instanceof org.apache.kafka.common.errors.UnknownTopicIdException
                || throwable instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) {
            return new KafkaAdminException(
                    HttpStatus.NOT_FOUND,
                    ApiErrorCode.NOT_FOUND,
                    "NOT_FOUND",
                    throwable.getMessage() != null ? throwable.getMessage() : "Kafka resource not found");
        }
        if (throwable instanceof org.apache.kafka.common.errors.TopicExistsException) {
            return new KafkaAdminException(
                    HttpStatus.CONFLICT,
                    ApiErrorCode.CONFLICT,
                    "CONFLICT",
                    throwable.getMessage() != null ? throwable.getMessage() : "Topic already exists");
        }
        if (throwable instanceof org.apache.kafka.common.errors.InvalidTopicException
                || throwable instanceof org.apache.kafka.common.errors.InvalidPartitionsException
                || throwable instanceof org.apache.kafka.common.errors.InvalidRequestException
                || throwable instanceof org.apache.kafka.common.errors.InvalidConfigurationException
                || throwable instanceof org.apache.kafka.common.errors.OffsetOutOfRangeException
                || throwable instanceof org.apache.kafka.common.errors.PolicyViolationException) {
            return new KafkaAdminException(
                    HttpStatus.BAD_REQUEST,
                    ApiErrorCode.VALIDATION_ERROR,
                    "INVALID_REQUEST",
                    throwable.getMessage() != null ? throwable.getMessage() : "Invalid Kafka request");
        }
        if (throwable instanceof org.apache.kafka.common.errors.UnsupportedVersionException) {
            return new KafkaAdminException(
                    HttpStatus.NOT_IMPLEMENTED,
                    ApiErrorCode.OPERATION_FAILED,
                    "UNSUPPORTED_VERSION",
                    throwable.getMessage() != null ? throwable.getMessage() : "Unsupported Kafka feature");
        }
        if (throwable instanceof org.apache.kafka.common.errors.SecurityDisabledException) {
            String guidance = "Kafka broker authorizer is not configured. ACL operations require an authorizer.\n"
                    + "Enable ACLs by setting the broker property authorizer.class.name.\n"
                    + "For example, in docker-compose set the environment variable: KAFKA_AUTHORIZER_CLASS_NAME=kafka.security.authorizer.AclAuthorizer\n"
                    + "Also configure super users as needed, e.g.: KAFKA_SUPER_USERS=User:ANONYMOUS;User:admin";
            String message = throwable.getMessage() != null ? throwable.getMessage() + ". " + guidance : guidance;
            return new KafkaAdminException(
                    HttpStatus.BAD_REQUEST, ApiErrorCode.KAFKA_SECURITY_DISABLED, "SECURITY_DISABLED", message);
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

    public record AdminClientHandle(Admin admin) implements AutoCloseable {
        @Override
        public void close() {
            // Singleton AdminClient is managed by Spring lifecycle.
        }
    }
}
