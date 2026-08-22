package com.opentext.security.analytics.messagehub.kafkamanager.operations.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ConflictException;
import com.opentext.security.analytics.messagehub.kafkamanager.common.InvalidOperationException;
import com.opentext.security.analytics.messagehub.kafkamanager.common.JsonSupport;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ResourceNotFoundException;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.api.OperationDetailResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.api.OperationEventResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.api.OperationSummaryResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.api.SubmitOperationRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.domain.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

/**
 * Service managing asynchronous operations persisted in the operations store.
 *
 * <p>Operations are stored as entities with lifecycle state (pending, validating, failed, etc.). This
 * service supports submission with idempotency, listing with pagination, retrieving details and events,
 * requesting cancellation and retrying failed operations.
 */
@Service
public class OperationService {

    private static final String OPERATION_NOT_FOUND = "Operation not found";

    private final OperationStore operationRepository;
    private final OperationEventStore eventRepository;
    private final ObjectMapper objectMapper;

    public OperationService(
            OperationStore operationRepository, OperationEventStore eventRepository, ObjectMapper objectMapper) {
        this.operationRepository = operationRepository;
        this.eventRepository = eventRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * List operations for a cluster with pagination.
     *
     * @param clusterId the target Kafka cluster id
     * @param page zero-based page index
     * @param size page size
     * @return a page of {@link OperationSummaryResponse}
     */
    public Page<OperationSummaryResponse> list(UUID clusterId, int page, int size) {
        return operationRepository
                .findByClusterIdOrderByCreatedAtDesc(clusterId, PageRequest.of(page, size))
                .map(this::summary);
    }

    /**
     * Get full operation details by operation id.
     *
     * @param id operation UUID
     * @return {@link OperationDetailResponse} containing operation fields and events
     * @throws ResourceNotFoundException when the operation does not exist
     */
    public OperationDetailResponse get(UUID id) {
        OperationEntity entity =
                operationRepository.findById(id).orElseThrow(() -> new ResourceNotFoundException(OPERATION_NOT_FOUND));
        return detail(entity);
    }

    /**
     * Return the chronological list of events for an operation.
     *
     * @param id operation UUID
     * @return list of {@link OperationEventResponse}
     * @throws ResourceNotFoundException when the operation does not exist
     */
    public List<OperationEventResponse> events(UUID id) {
        if (!operationRepository.existsById(id)) {
            throw new ResourceNotFoundException(OPERATION_NOT_FOUND);
        }
        return eventRepository.findAllByOperationIdOrderBySequenceNumberAsc(id).stream()
                .map(this::event)
                .toList();
    }

    /**
     * Submit a new asynchronous operation.
     *
     * <p>This creates an OperationEntity in the repository, records a SUBMITTED event and
     * returns the operation detail. If an idempotency key is provided and a matching operation
     * already exists for the cluster, a {@code ConflictException} is thrown.
     *
     * @param clusterId the target Kafka cluster id
     * @param request the submission request containing operation type, target and payload
     * @return created {@link OperationDetailResponse}
     * @throws ConflictException when a duplicate idempotency key is detected
     */
    public OperationDetailResponse submit(UUID clusterId, SubmitOperationRequest request) {
        if (request.idempotencyKey() != null
                && operationRepository
                        .findByClusterIdAndIdempotencyKey(clusterId, request.idempotencyKey())
                        .isPresent()) {
            throw new ConflictException("Duplicate idempotency key for this cluster");
        }
        OperationEntity entity = new OperationEntity();
        entity.setId(UUID.randomUUID());
        entity.setClusterId(clusterId);
        entity.setOperationType(request.operationType());
        entity.setTargetResourceName(request.resourceName());
        entity.setCurrentState(request.dryRun() ? OperationState.VALIDATING : OperationState.PENDING);
        entity.setRequestedBy(request.requestedBy());
        entity.setApprovedBy(request.approvedBy());
        entity.setIdempotencyKey(request.idempotencyKey());
        entity.setDryRun(request.dryRun());
        entity.setRequestedInputJson(
                JsonSupport.toJson(objectMapper, request.payload() == null ? Map.of() : request.payload()));
        entity.setProgressJson("{\"progress\":0}");
        operationRepository.save(entity);
        eventRepository.save(
                new OperationEventEntity(UUID.randomUUID(), entity.getId(), "SUBMITTED", "Operation submitted", 1));
        return detail(entity);
    }

    /**
     * Request cancellation of an in-progress operation.
     *
     * @param id operation UUID
     * @throws ResourceNotFoundException when the operation does not exist
     */
    public void cancel(UUID id) {
        OperationEntity entity =
                operationRepository.findById(id).orElseThrow(() -> new ResourceNotFoundException(OPERATION_NOT_FOUND));
        entity.setCancellationRequested(true);
        entity.setCurrentState(OperationState.CANCELLATION_REQUESTED);
        eventRepository.save(new OperationEventEntity(
                UUID.randomUUID(),
                entity.getId(),
                "CANCEL_REQUESTED",
                "Cancellation requested",
                eventRepository.countByOperationId(id) + 1));
    }

    /**
     * Retry a failed or cancelled operation by resetting its state to PENDING and clearing
     * failure/cancellation flags.
     *
     * @param id operation UUID
     * @throws ResourceNotFoundException when the operation does not exist
     * @throws InvalidOperationException when the operation is not in a retryable state
     */
    public void retry(UUID id) {
        OperationEntity entity =
                operationRepository.findById(id).orElseThrow(() -> new ResourceNotFoundException(OPERATION_NOT_FOUND));
        if (entity.getCurrentState() != OperationState.FAILED && entity.getCurrentState() != OperationState.CANCELLED) {
            throw new InvalidOperationException("Only failed or cancelled operations can be retried");
        }
        entity.setCurrentState(OperationState.PENDING);
        entity.setRetryCount(entity.getRetryCount() + 1);
        entity.setCancellationRequested(false);
        entity.setCancelled(false);
        entity.setFailureCategory(null);
        entity.setFailureDetails(null);
        entity.setStartedAt(null);
        entity.setCompletedAt(null);
    }

    private OperationSummaryResponse summary(OperationEntity entity) {
        return new OperationSummaryResponse(
                entity.getId(),
                entity.getClusterId(),
                entity.getOperationType(),
                entity.getTargetResourceName(),
                entity.getCurrentState(),
                entity.getRequestedBy(),
                entity.getApprovedBy(),
                entity.getIdempotencyKey(),
                entity.isDryRun(),
                entity.getRetryCount(),
                entity.isCancellationRequested(),
                entity.isCancelled(),
                entity.getCreatedAt(),
                entity.getUpdatedAt(),
                entity.getStartedAt(),
                entity.getCompletedAt());
    }

    private OperationDetailResponse detail(OperationEntity entity) {
        return new OperationDetailResponse(
                entity.getId(),
                entity.getClusterId(),
                entity.getOperationType(),
                entity.getTargetResourceName(),
                entity.getCurrentState(),
                entity.getRequestedBy(),
                entity.getApprovedBy(),
                entity.getIdempotencyKey(),
                entity.isDryRun(),
                entity.getRetryCount(),
                entity.isCancellationRequested(),
                entity.isCancelled(),
                entity.getFailureCategory(),
                entity.getFailureDetails(),
                entity.getCreatedAt(),
                entity.getUpdatedAt(),
                entity.getStartedAt(),
                entity.getCompletedAt(),
                events(entity.getId()));
    }

    private OperationEventResponse event(OperationEventEntity entity) {
        return new OperationEventResponse(
                entity.getId(),
                entity.getOperationId(),
                entity.getEventType(),
                entity.getMessage(),
                entity.getCreatedAt(),
                entity.getSequenceNumber());
    }
}
