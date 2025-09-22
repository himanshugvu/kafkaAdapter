package com.orchestrator.adapter.service;

import com.orchestrator.adapter.config.AdapterProperties;
import com.orchestrator.adapter.model.FailureRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class MessageConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerService.class);

    private final AdapterProperties properties;
    private final MessageTransformer messageTransformer;
    private final RetryService retryService;
    private final ErrorHandlingService errorHandlingService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public MessageConsumerService(AdapterProperties properties,
                                 MessageTransformer messageTransformer,
                                 RetryService retryService,
                                 ErrorHandlingService errorHandlingService,
                                 KafkaTemplate<String, String> kafkaTemplate) {
        this.properties = properties;
        this.messageTransformer = messageTransformer;
        this.retryService = retryService;
        this.errorHandlingService = errorHandlingService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${adapter.consumer.topic}",
                   containerFactory = "kafkaListenerContainerFactory")
    public void consumeMessages(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        logger.info("Received batch of {} records", records.size());

        if (properties.consumer().mode() == AdapterProperties.ConsumerMode.RECORD) {
            processRecordMode(records, acknowledgment);
        } else {
            processBatchMode(records, acknowledgment);
        }
    }

    private void processRecordMode(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        logger.debug("Processing in RECORD mode - {} records", records.size());

        AtomicInteger processed = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);

        for (ConsumerRecord<String, String> record : records) {
            processRecord(record)
                .thenAccept(success -> {
                    if (success) {
                        processed.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                    }

                    int totalProcessed = processed.get() + failed.get();
                    if (totalProcessed == records.size()) {
                        logger.info("Record mode processing complete: {} succeeded, {} failed", processed.get(), failed.get());
                        acknowledgment.acknowledge();
                    }
                })
                .exceptionally(throwable -> {
                    failed.incrementAndGet();
                    logger.error("Unexpected error processing record: {}", throwable.getMessage());

                    int totalProcessed = processed.get() + failed.get();
                    if (totalProcessed == records.size()) {
                        acknowledgment.acknowledge();
                    }
                    return null;
                });
        }
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

    private CompletableFuture<Boolean> processRecord(ConsumerRecord<String, String> record) {
        return retryService.executeWithRetry(
            () -> transformAndSend(record),
            "processRecord"
        )
        .thenApply(result -> true)
        .exceptionally(throwable -> {
            errorHandlingService.handleFailure(record, throwable, properties.retry().maxAttempts());
            return false;
        });
    }

    private CompletableFuture<Boolean> processRecordInBatch(ConsumerRecord<String, String> record, List<FailureRecord> failures) {
        return retryService.executeWithRetry(
            () -> transformAndSend(record),
            "processRecordInBatch"
        )
        .thenApply(result -> true)
        .exceptionally(throwable -> {
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

            return kafkaTemplate.send(getTargetTopic(), record.key(), transformedMessage);
        })
        .thenCompose(sendResult -> sendResult)
        .thenAccept(result -> {
            logger.debug("Successfully sent message to topic '{}': offset {}",
                result.getRecordMetadata().topic(), result.getRecordMetadata().offset());
        });
    }

    private String getTargetTopic() {
        return System.getProperty("target.kafka.topic", "output-topic");
    }
}