package com.opentext.security.analytics.messagehub.kafkamanager.operations.domain;

import java.time.Instant;
import java.util.UUID;

public class OperationEntity {

    private UUID id;

    private UUID clusterId;

    private String operationType;

    private String targetResourceName;

    private OperationState currentState;

    private String requestedBy;

    private String approvedBy;

    private String idempotencyKey;

    private boolean dryRun;

    private String requestedInputJson;

    private String normalizedPlanJson;

    private String dryRunReportJson;

    private String progressJson;

    private String failureCategory;

    private String failureDetails;

    private int retryCount;

    private boolean cancellationRequested;

    private boolean cancelled;

    private Instant createdAt;

    private Instant updatedAt;

    private Instant startedAt;

    private Instant completedAt;

    private String leaseOwner;

    private Instant leaseExpiresAt;

    private long version;

    public OperationEntity() {}

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getClusterId() {
        return clusterId;
    }

    public void setClusterId(UUID clusterId) {
        this.clusterId = clusterId;
    }

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    public String getTargetResourceName() {
        return targetResourceName;
    }

    public void setTargetResourceName(String targetResourceName) {
        this.targetResourceName = targetResourceName;
    }

    public OperationState getCurrentState() {
        return currentState;
    }

    public void setCurrentState(OperationState currentState) {
        this.currentState = currentState;
    }

    public String getRequestedBy() {
        return requestedBy;
    }

    public void setRequestedBy(String requestedBy) {
        this.requestedBy = requestedBy;
    }

    public String getApprovedBy() {
        return approvedBy;
    }

    public void setApprovedBy(String approvedBy) {
        this.approvedBy = approvedBy;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public String getRequestedInputJson() {
        return requestedInputJson;
    }

    public void setRequestedInputJson(String requestedInputJson) {
        this.requestedInputJson = requestedInputJson;
    }

    public String getNormalizedPlanJson() {
        return normalizedPlanJson;
    }

    public void setNormalizedPlanJson(String normalizedPlanJson) {
        this.normalizedPlanJson = normalizedPlanJson;
    }

    public String getDryRunReportJson() {
        return dryRunReportJson;
    }

    public void setDryRunReportJson(String dryRunReportJson) {
        this.dryRunReportJson = dryRunReportJson;
    }

    public String getProgressJson() {
        return progressJson;
    }

    public void setProgressJson(String progressJson) {
        this.progressJson = progressJson;
    }

    public String getFailureCategory() {
        return failureCategory;
    }

    public void setFailureCategory(String failureCategory) {
        this.failureCategory = failureCategory;
    }

    public String getFailureDetails() {
        return failureDetails;
    }

    public void setFailureDetails(String failureDetails) {
        this.failureDetails = failureDetails;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public boolean isCancellationRequested() {
        return cancellationRequested;
    }

    public void setCancellationRequested(boolean cancellationRequested) {
        this.cancellationRequested = cancellationRequested;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    // Allow in-memory stores to set timestamps without using reflection
    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    public String getLeaseOwner() {
        return leaseOwner;
    }

    public void setLeaseOwner(String leaseOwner) {
        this.leaseOwner = leaseOwner;
    }

    public Instant getLeaseExpiresAt() {
        return leaseExpiresAt;
    }

    public void setLeaseExpiresAt(Instant leaseExpiresAt) {
        this.leaseExpiresAt = leaseExpiresAt;
    }

    public long getVersion() {
        return version;
    }
}
