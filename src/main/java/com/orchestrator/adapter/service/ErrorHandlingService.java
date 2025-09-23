package com.orchestrator.adapter.service;

import com.orchestrator.adapter.config.AdapterProperties;
import com.orchestrator.adapter.model.FailureRecord;
import com.orchestrator.adapter.repository.FailureRecordRepository;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Service
public class ErrorHandlingService {

    private static final Logger logger = LoggerFactory.getLogger(ErrorHandlingService.class);

    private final AdapterProperties properties;
    private final FailureRecordRepository failureRepository;
    private final Optional<KafkaTemplate<String, String>> kafkaTemplate;
    private final CircuitBreaker dbCircuitBreaker;
    private final RetryService retryService;

    public ErrorHandlingService(AdapterProperties properties,
                               FailureRecordRepository failureRepository,
                               @Autowired(required = false) KafkaTemplate<String, String> kafkaTemplate,
                               CircuitBreakerRegistry circuitBreakerRegistry,
                               RetryService retryService) {
        this.properties = properties;
        this.failureRepository = failureRepository;
        this.kafkaTemplate = Optional.ofNullable(kafkaTemplate);
        this.dbCircuitBreaker = circuitBreakerRegistry.circuitBreaker("dbCircuit");
        this.retryService = retryService;
    }

    public CompletableFuture<Void> handleFailure(ConsumerRecord<String, String> record, Throwable error, int retryCount) {
        RetryService.RetryResult retryResult = retryService.classifyFailure(error);

        FailureRecord.FailureType failureType = retryResult.isRetriable()
            ? FailureRecord.FailureType.RETRYABLE
            : FailureRecord.FailureType.NON_RETRYABLE;

        FailureRecord failureRecord = new FailureRecord(
            record.key(),
            record.value(),
            record.topic(),
            record.partition(),
            record.offset(),
            failureType,
            retryResult.getErrorMessage(),
            retryCount
        );

        return handleFailureByStrategy(failureRecord);
    }

    public CompletableFuture<Void> handleBatchFailures(List<FailureRecord> failures) {
        return switch (properties.error().strategy()) {
            case DB -> handleDbBatchFailures(failures);
            case KAFKA -> handleKafkaBatchFailures(failures);
            case HYBRID -> handleHybridBatchFailures(failures);
        };
    }

    private CompletableFuture<Void> handleFailureByStrategy(FailureRecord failure) {
        return switch (properties.error().strategy()) {
            case DB -> handleDbFailure(failure);
            case KAFKA -> handleKafkaFailure(failure);
            case HYBRID -> handleHybridFailure(failure);
        };
    }

    private CompletableFuture<Void> handleDbFailure(FailureRecord failure) {
        if (!properties.db().enabled()) {
            logger.warn("DB strategy configured but DB is disabled");
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                if (dbCircuitBreaker.getState() == CircuitBreaker.State.OPEN) {
                    logger.error("DB circuit breaker is OPEN, cannot store failure record");
                    return null;
                }

                return dbCircuitBreaker.executeSupplier(() -> {
                    failureRepository.save(failure);
                    logger.debug("Stored failure record to DB: {}", failure.getId());
                    return null;
                });
            } catch (Exception e) {
                logger.error("Failed to store failure record to DB: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }

    private CompletableFuture<Void> handleKafkaFailure(FailureRecord failure) {
        if (!isDltEnabled()) {
            logger.warn("DLT strategy configured but DLT is disabled");
            return CompletableFuture.completedFuture(null);
        }

        if (kafkaTemplate.isEmpty()) {
            logger.warn("DLT enabled but KafkaTemplate not available");
            return CompletableFuture.completedFuture(null);
        }

        String topic = failure.getFailureType() == FailureRecord.FailureType.RETRYABLE
            ? properties.dlt().retryableTopic()
            : properties.dlt().nonRetryableTopic();

        return kafkaTemplate.get().send(topic, failure.getMessageKey(), failure.getMessageValue())
            .thenAccept(result -> {
                logger.debug("Sent failure record to DLT topic '{}': offset {}",
                    topic, result.getRecordMetadata().offset());
            })
            .exceptionally(throwable -> {
                logger.error("Failed to send failure record to DLT topic '{}': {}", topic, throwable.getMessage());
                return null;
            });
    }

    private CompletableFuture<Void> handleHybridFailure(FailureRecord failure) {
        if (properties.db().enabled() && dbCircuitBreaker.getState() != CircuitBreaker.State.OPEN) {
            return handleDbFailure(failure);
        } else if (isDltEnabled()) {
            logger.warn("DB unavailable, falling back to Kafka DLT");
            return handleKafkaFailure(failure);
        } else {
            logger.warn("Both DB and DLT are disabled, cannot handle failure");
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> handleDbBatchFailures(List<FailureRecord> failures) {
        if (!properties.db().enabled()) {
            logger.warn("DB strategy configured but DB is disabled");
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                if (dbCircuitBreaker.getState() == CircuitBreaker.State.OPEN) {
                    logger.error("DB circuit breaker is OPEN, cannot store {} failure records", failures.size());
                    return null;
                }

                return dbCircuitBreaker.executeSupplier(() -> {
                    List<FailureRecord> savedRecords = failureRepository.saveAll(failures);
                    logger.debug("Bulk saved {} failure records to DB", savedRecords.size());
                    return null;
                });
            } catch (Exception e) {
                logger.error("Failed to bulk save {} failure records to DB: {}", failures.size(), e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }

    private CompletableFuture<Void> handleKafkaBatchFailures(List<FailureRecord> failures) {
        if (!isDltEnabled()) {
            logger.warn("DLT strategy configured but DLT is disabled");
            return CompletableFuture.completedFuture(null);
        }

        if (kafkaTemplate.isEmpty()) {
            logger.warn("DLT enabled but KafkaTemplate not available");
            return CompletableFuture.completedFuture(null);
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (FailureRecord failure : failures) {
            String topic = failure.getFailureType() == FailureRecord.FailureType.RETRYABLE
                ? properties.dlt().retryableTopic()
                : properties.dlt().nonRetryableTopic();

            CompletableFuture<Void> future = kafkaTemplate.get().send(topic, failure.getMessageKey(), failure.getMessageValue())
                .thenAccept(result -> {
                    logger.debug("Sent failure record to DLT topic '{}': offset {}",
                        topic, result.getRecordMetadata().offset());
                })
                .exceptionally(throwable -> {
                    logger.error("Failed to send failure record to DLT topic '{}': {}", topic, throwable.getMessage());
                    return null;
                });

            futures.add(future);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    private CompletableFuture<Void> handleHybridBatchFailures(List<FailureRecord> failures) {
        if (properties.db().enabled() && dbCircuitBreaker.getState() != CircuitBreaker.State.OPEN) {
            return handleDbBatchFailures(failures);
        } else if (isDltEnabled()) {
            logger.warn("DB unavailable, falling back to Kafka DLT for {} failures", failures.size());
            return handleKafkaBatchFailures(failures);
        } else {
            logger.warn("Both DB and DLT are disabled, cannot handle {} failures", failures.size());
            return CompletableFuture.completedFuture(null);
        }
    }

    private boolean isDltEnabled() {
        return properties.dlt() != null && properties.dlt().enabled();
    }

    public boolean isDbCircuitBreakerOpen() {
        return dbCircuitBreaker.getState() == CircuitBreaker.State.OPEN;
    }

    public long getRetryableFailureCount() {
        try {
            return failureRepository.countByFailureType(FailureRecord.FailureType.RETRYABLE);
        } catch (Exception e) {
            logger.error("Failed to count retryable failures: {}", e.getMessage());
            return 0;
        }
    }

    public long getNonRetryableFailureCount() {
        try {
            return failureRepository.countByFailureType(FailureRecord.FailureType.NON_RETRYABLE);
        } catch (Exception e) {
            logger.error("Failed to count non-retryable failures: {}", e.getMessage());
            return 0;
        }
    }
}