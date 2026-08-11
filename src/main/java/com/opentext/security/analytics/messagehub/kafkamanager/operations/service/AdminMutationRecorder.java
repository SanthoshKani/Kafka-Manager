package com.opentext.security.analytics.messagehub.kafkamanager.operations.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.common.JsonSupport;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.domain.*;
import java.util.UUID;
import java.util.function.Supplier;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

@Service
public class AdminMutationRecorder {

    private final OperationStore operationRepository;
    private final OperationEventStore eventRepository;
    private final ObjectMapper objectMapper;

    public AdminMutationRecorder(
            OperationStore operationRepository, OperationEventStore eventRepository, ObjectMapper objectMapper) {
        this.operationRepository = operationRepository;
        this.eventRepository = eventRepository;
        this.objectMapper = objectMapper;
    }

    public <T> T record(
            UUID clusterId,
            String operationType,
            String targetResourceName,
            boolean dryRun,
            Object requestPayload,
            Supplier<T> action) {
        OperationEntity entity = new OperationEntity();
        entity.setId(UUID.randomUUID());
        entity.setClusterId(clusterId);
        entity.setOperationType(operationType);
        entity.setTargetResourceName(targetResourceName);
        entity.setCurrentState(OperationState.RUNNING);
        entity.setRequestedBy(requestedBy());
        entity.setDryRun(dryRun);
        entity.setRequestedInputJson(JsonSupport.toJson(objectMapper, requestPayload));
        entity.setProgressJson("{\"progress\":0}");
        OperationEntity persisted = operationRepository.saveAndFlush(entity);
        eventRepository.saveAndFlush(new OperationEventEntity(
                UUID.randomUUID(), persisted.getId(), "RUNNING", "Admin mutation is running", 1));
        try {
            T result = action.get();
            persisted.setCurrentState(OperationState.SUCCEEDED);
            eventRepository.saveAndFlush(new OperationEventEntity(
                    UUID.randomUUID(),
                    persisted.getId(),
                    "SUCCEEDED",
                    "Admin mutation completed",
                    eventRepository.countByOperationId(persisted.getId()) + 1));
            return result;
        } catch (RuntimeException exception) {
            persisted.setCurrentState(OperationState.FAILED);
            persisted.setFailureCategory(exception.getClass().getSimpleName());
            persisted.setFailureDetails(exception.getMessage());
            eventRepository.saveAndFlush(new OperationEventEntity(
                    UUID.randomUUID(),
                    persisted.getId(),
                    "FAILED",
                    "Admin mutation failed: " + exception.getClass().getSimpleName(),
                    eventRepository.countByOperationId(persisted.getId()) + 1));
            throw exception;
        } finally {
            operationRepository.saveAndFlush(persisted);
        }
    }

    private String requestedBy() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return authentication == null ? null : authentication.getName();
    }
}
