package com.orchestrator.adapter.service;

import com.orchestrator.adapter.config.AdapterProperties;
import com.orchestrator.adapter.model.FailureRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@ConditionalOnProperty(name = "adapter.consumer.mode", havingValue = "batch", matchIfMissing = true)
public class BatchMessageConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(BatchMessageConsumerService.class);

    private final AdapterProperties properties;
    private final MessageTransformer messageTransformer;
    private final RetryService retryService;
    private final ErrorHandlingService errorHandlingService;
    private final KafkaTemplate<String, String> targetKafkaTemplate;

    public BatchMessageConsumerService(AdapterProperties properties,
                                      MessageTransformer messageTransformer,
                                      RetryService retryService,
                                      ErrorHandlingService errorHandlingService,
                                      @Qualifier("targetKafkaTemplate") KafkaTemplate<String, String> targetKafkaTemplate) {
        this.properties = properties;
        this.messageTransformer = messageTransformer;
        this.retryService = retryService;
        this.errorHandlingService = errorHandlingService;
        this.targetKafkaTemplate = targetKafkaTemplate;

        logger.info("BatchMessageConsumerService initialized - BATCH mode enabled");
    }

    @KafkaListener(
        topics = "${adapter.consumer.topic}",
        containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void consumeBatchMessages(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        logger.info("BATCH MODE: Received batch of {} records", records.size());
        processBatchMode(records, acknowledgment);
    }

    private void processBatchMode(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        logger.debug("Processing in BATCH mode - {} records", records.size());

        CountDownLatch latch = new CountDownLatch(records.size());
        List<FailureRecord> failures = new ArrayList<>();
        AtomicInteger processed = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);

        for (ConsumerRecord<String, String> record : records) {
            processRecordInBatch(record, failures)
                .thenAccept(success -> {
                    if (success) {
                        processed.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                    }
                    latch.countDown();
                })
                .exceptionally(throwable -> {
                    failed.incrementAndGet();
                    logger.error("Unexpected error processing record in batch: {}", throwable.getMessage());
                    latch.countDown();
                    return null;
                });
        }

        try {
            if (latch.await(properties.db().timeoutMs(), TimeUnit.MILLISECONDS)) {
                logger.info("Batch processing complete: {} succeeded, {} failed", processed.get(), failed.get());

                if (!failures.isEmpty()) {
                    errorHandlingService.handleBatchFailures(failures)
                        .thenRun(() -> {
                            logger.info("Batch failure handling complete, acknowledging offset");
                            acknowledgment.acknowledge();
                        })
                        .exceptionally(throwable -> {
                            logger.error("Failed to handle batch failures: {}", throwable.getMessage());
                            acknowledgment.acknowledge();
                            return null;
                        });
                } else {
                    acknowledgment.acknowledge();
                }
            } else {
                logger.error("Batch processing timeout after {}ms", properties.db().timeoutMs());
                acknowledgment.acknowledge();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Batch processing interrupted");
            acknowledgment.acknowledge();
        }
    }

    private CompletableFuture<Boolean> processRecordInBatch(ConsumerRecord<String, String> record, List<FailureRecord> failures) {
        return retryService.executeWithRetry(
            () -> transformAndSend(record),
            "processRecordInBatch"
        )
        .thenApply(result -> {
            logger.debug("Batch record processed successfully: topic={}, partition={}, offset={}",
                        record.topic(), record.partition(), record.offset());
            return true;
        })
        .exceptionally(throwable -> {
            logger.error("Batch record processing failed after retries: topic={}, partition={}, offset={}, error={}",
                        record.topic(), record.partition(), record.offset(), throwable.getMessage());

            RetryService.RetryResult retryResult = retryService.classifyFailure(throwable);

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
                properties.retry().maxAttempts()
            );

            synchronized (failures) {
                failures.add(failureRecord);
            }

            return false;
        });
    }

    private CompletableFuture<Void> transformAndSend(ConsumerRecord<String, String> record) {
        return CompletableFuture.supplyAsync(() -> {
            String transformedMessage = messageTransformer.transform(record.value());
            return targetKafkaTemplate.send(getTargetTopic(), record.key(), transformedMessage);
        })
        .thenCompose(sendResult -> sendResult)
        .thenAccept(result -> {
            logger.debug("Successfully sent message to topic '{}': partition={}, offset={}",
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
        });
    }

    private String getTargetTopic() {
        String targetTopic = System.getProperty("target.kafka.topic");
        return targetTopic != null ? targetTopic : "output-topic";
    }
}