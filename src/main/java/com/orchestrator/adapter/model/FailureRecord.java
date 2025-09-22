package com.orchestrator.adapter.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "failure_records")
public class FailureRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "message_key")
    private String messageKey;

    @Column(name = "message_value", columnDefinition = "TEXT")
    private String messageValue;

    @Column(name = "source_topic")
    private String sourceTopic;

    @Column(name = "source_partition")
    private Integer sourcePartition;

    @Column(name = "source_offset")
    private Long sourceOffset;

    @Column(name = "failure_type")
    @Enumerated(EnumType.STRING)
    private FailureType failureType;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "retry_count")
    private Integer retryCount;

    @Column(name = "created_at")
    private Instant createdAt;

    @Column(name = "last_retry_at")
    private Instant lastRetryAt;

    public FailureRecord() {}

    public FailureRecord(String messageKey, String messageValue, String sourceTopic,
                        Integer sourcePartition, Long sourceOffset, FailureType failureType,
                        String errorMessage, Integer retryCount) {
        this.messageKey = messageKey;
        this.messageValue = messageValue;
        this.sourceTopic = sourceTopic;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.failureType = failureType;
        this.errorMessage = errorMessage;
        this.retryCount = retryCount;
        this.createdAt = Instant.now();
        this.lastRetryAt = Instant.now();
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getMessageKey() { return messageKey; }
    public void setMessageKey(String messageKey) { this.messageKey = messageKey; }

    public String getMessageValue() { return messageValue; }
    public void setMessageValue(String messageValue) { this.messageValue = messageValue; }

    public String getSourceTopic() { return sourceTopic; }
    public void setSourceTopic(String sourceTopic) { this.sourceTopic = sourceTopic; }

    public Integer getSourcePartition() { return sourcePartition; }
    public void setSourcePartition(Integer sourcePartition) { this.sourcePartition = sourcePartition; }

    public Long getSourceOffset() { return sourceOffset; }
    public void setSourceOffset(Long sourceOffset) { this.sourceOffset = sourceOffset; }

    public FailureType getFailureType() { return failureType; }
    public void setFailureType(FailureType failureType) { this.failureType = failureType; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public Integer getRetryCount() { return retryCount; }
    public void setRetryCount(Integer retryCount) { this.retryCount = retryCount; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getLastRetryAt() { return lastRetryAt; }
    public void setLastRetryAt(Instant lastRetryAt) { this.lastRetryAt = lastRetryAt; }

    public enum FailureType {
        RETRYABLE, NON_RETRYABLE
    }
}