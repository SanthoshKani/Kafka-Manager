package com.opentext.security.analytics.messagehub.kafkamanager.operations.service;

import com.opentext.security.analytics.messagehub.kafkamanager.operations.domain.*;
import com.opentext.security.analytics.messagehub.kafkamanager.topics.service.TopicMutationService;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OperationRunner {

    private final OperationStore operationRepository;
    private final OperationEventStore eventRepository;
    private final TopicMutationService topicMutationService;

    public OperationRunner(
            OperationStore operationRepository,
            OperationEventStore eventRepository,
            TopicMutationService topicMutationService) {
        this.operationRepository = operationRepository;
        this.eventRepository = eventRepository;
        this.topicMutationService = topicMutationService;
    }

    @Scheduled(fixedDelayString = "${app.operations.poll-interval:PT15S}")
    public void reconcile() {
        List<OperationEntity> operations = operationRepository.claimable(
                List.of(
                        OperationState.PENDING,
                        OperationState.VALIDATING,
                        OperationState.SCHEDULED,
                        OperationState.RUNNING),
                Instant.now());
        for (OperationEntity operation : operations) {
            if (operation.getLeaseExpiresAt() != null
                    && operation.getLeaseExpiresAt().isAfter(Instant.now())) {
                continue;
            }
            process(operation);
        }
    }

    private void process(OperationEntity operation) {
        operation.setCurrentState(OperationState.RUNNING);
        operation.setStartedAt(Instant.now());
        operation.setLeaseOwner("local-scheduler");
        operation.setLeaseExpiresAt(Instant.now().plusSeconds(60));
        eventRepository.save(new OperationEventEntity(
                UUID.randomUUID(),
                operation.getId(),
                "RUNNING",
                "Operation is running",
                eventRepository.countByOperationId(operation.getId()) + 1));
        try {
            switch (OperationType.valueOf(operation.getOperationType())) {
                case TOPIC_CREATE ->
                    topicMutationService.create(
                            operation.getClusterId(),
                            operation.getTargetResourceName(),
                            operation.getRequestedInputJson(),
                            operation.isDryRun());
                case TOPIC_DELETE ->
                    topicMutationService.delete(
                            operation.getClusterId(), operation.getTargetResourceName(), operation.isDryRun());
                default -> {}
            }
            operation.setCurrentState(OperationState.SUCCEEDED);
            operation.setCompletedAt(Instant.now());
            eventRepository.save(new OperationEventEntity(
                    UUID.randomUUID(),
                    operation.getId(),
                    "SUCCEEDED",
                    "Operation completed",
                    eventRepository.countByOperationId(operation.getId()) + 1));
        } catch (Exception exception) {
            operation.setCurrentState(OperationState.FAILED);
            operation.setFailureCategory(exception.getClass().getSimpleName());
            operation.setFailureDetails(exception.getMessage());
            operation.setCompletedAt(Instant.now());
            eventRepository.save(new OperationEventEntity(
                    UUID.randomUUID(),
                    operation.getId(),
                    "FAILED",
                    "Operation failed: " + exception.getClass().getSimpleName(),
                    eventRepository.countByOperationId(operation.getId()) + 1));
        } finally {
            operation.setLeaseExpiresAt(null);
            operation.setLeaseOwner(null);
        }
    }
}
