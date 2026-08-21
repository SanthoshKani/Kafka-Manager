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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class OperationService {

    private static final String OPERATION_NOT_FOUND = "Operation not found";

    private final OperationStore operationRepository;
    private final OperationEventStore eventRepository;
    private final ObjectMapper objectMapper;

    public OperationService(
            OperationStore operationRepository,
            OperationEventStore eventRepository,
            ObjectMapper objectMapper) {
        this.operationRepository = operationRepository;
        this.eventRepository = eventRepository;
        this.objectMapper = objectMapper;
    }

    public Page<OperationSummaryResponse> list(UUID clusterId, int page, int size) {
        return operationRepository
                .findByClusterIdOrderByCreatedAtDesc(clusterId, PageRequest.of(page, size))
                .map(this::summary);
    }

    public OperationDetailResponse get(UUID id) {
        OperationEntity entity =
                operationRepository.findById(id).orElseThrow(() -> new ResourceNotFoundException(OPERATION_NOT_FOUND));
        return detail(entity);
    }

    public List<OperationEventResponse> events(UUID id) {
        if (!operationRepository.existsById(id)) {
            throw new ResourceNotFoundException(OPERATION_NOT_FOUND);
        }
        return eventRepository.findAllByOperationIdOrderBySequenceNumberAsc(id).stream()
                .map(this::event)
                .toList();
    }

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
