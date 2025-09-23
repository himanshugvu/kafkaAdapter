package com.orchestrator.adapter.service;

import com.orchestrator.adapter.config.AdapterProperties;
import com.orchestrator.adapter.config.TargetKafkaProperties;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Service
@ConditionalOnProperty(name = "adapter.consumer.mode", havingValue = "batch", matchIfMissing = true)
public class BatchMessageConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(BatchMessageConsumerService.class);
    private static final long DEFAULT_BATCH_COMPLETION_TIMEOUT_MS = 5_000L;
    private static final long DEFAULT_FAILURE_HANDLING_TIMEOUT_MS = 5_000L;

    private final AdapterProperties properties;
    private final MessageTransformer messageTransformer;
    private final RetryService retryService;
    private final ErrorHandlingService errorHandlingService;
    private final KafkaTemplate<String, String> targetKafkaTemplate;
    private final TargetKafkaProperties targetKafkaProperties;
    private final Executor processingExecutor;
    private final long batchCompletionTimeoutMs;
    private final long failureHandlingTimeoutMs;

    public BatchMessageConsumerService(AdapterProperties properties,
                                       MessageTransformer messageTransformer,
                                       RetryService retryService,
                                       ErrorHandlingService errorHandlingService,
                                       @Qualifier("targetKafkaTemplate") KafkaTemplate<String, String> targetKafkaTemplate,
                                       TargetKafkaProperties targetKafkaProperties,
                                       @Qualifier("adapterAsyncExecutor") Executor processingExecutor) {
        this.properties = properties;
        this.messageTransformer = messageTransformer;
        this.retryService = retryService;
        this.errorHandlingService = errorHandlingService;
        this.targetKafkaTemplate = targetKafkaTemplate;
        this.targetKafkaProperties = targetKafkaProperties;
        this.processingExecutor = processingExecutor;
        this.batchCompletionTimeoutMs = resolveTimeout(properties);
        this.failureHandlingTimeoutMs = resolveFailureHandlingTimeout(properties);

        logger.info("BatchMessageConsumerService initialized - BATCH mode enabled");
    }

    @KafkaListener(
        topics = "${adapter.consumer.topic}",
        containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void consumeBatchMessages(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        if (records == null || records.isEmpty()) {
            logger.debug("BATCH MODE: Received empty batch");
            acknowledgment.acknowledge();
            return;
        }

        logger.info("BATCH MODE: Received batch of {} records", records.size());
        processBatchMode(records, acknowledgment);
    }

    private void processBatchMode(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        logger.debug("Processing in BATCH mode - {} records", records.size());

        CountDownLatch latch = new CountDownLatch(records.size());
        List<FailureRecord> failures = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger processed = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);
        record RecordProcessingContext(ConsumerRecord<String, String> record, CompletableFuture<Boolean> future) {}
        List<RecordProcessingContext> contexts = new ArrayList<>(records.size());

        for (ConsumerRecord<String, String> record : records) {
            CompletableFuture<Boolean> future = processRecordInBatch(record, failures);
            contexts.add(new RecordProcessingContext(record, future));
            future.whenComplete((success, throwable) -> {
                if (throwable != null) {
                    failed.incrementAndGet();
                    logger.error("Unexpected error processing record in batch: {}", throwable.getMessage(), throwable);
                } else if (Boolean.TRUE.equals(success)) {
                    processed.incrementAndGet();
                } else {
                    failed.incrementAndGet();
                }
                latch.countDown();
            });
        }

        boolean completedOnTime;
        try {
            completedOnTime = latch.await(batchCompletionTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Batch processing interrupted", e);
            acknowledgment.acknowledge();
            return;
        }

        if (!completedOnTime) {
            logger.error("Batch processing timeout after {} ms", batchCompletionTimeoutMs);
            for (RecordProcessingContext context : contexts) {
                if (!context.future().isDone()) {
                    context.future().cancel(true);
                    ConsumerRecord<String, String> record = context.record();
                    logger.error("Record timed out during batch processing: topic={}, partition={}, offset={}",
                        record.topic(), record.partition(), record.offset());
                    FailureRecord timeoutRecord = new FailureRecord(
                        record.key(),
                        record.value(),
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        FailureRecord.FailureType.RETRYABLE,
                        "Processing timed out after " + batchCompletionTimeoutMs + " ms",
                        properties.retry().maxAttempts()
                    );
                    failures.add(timeoutRecord);
                }
            }
        }

        logger.info("Batch processing complete: {} succeeded, {} failed", processed.get(), failed.get());

        CompletableFuture<Void> failureHandlingFuture = failures.isEmpty()
            ? CompletableFuture.completedFuture(null)
            : errorHandlingService.handleBatchFailures(List.copyOf(failures));

        try {
            failureHandlingFuture
                .orTimeout(failureHandlingTimeoutMs, TimeUnit.MILLISECONDS)
                .join();
            if (!failures.isEmpty()) {
                logger.info("Batch failure handling complete for {} record(s)", failures.size());
            }
        } catch (CompletionException ex) {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            logger.error("Failed to handle batch failures: {}", cause.getMessage(), cause);
        } finally {
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
                Throwable cause = throwable instanceof CompletionException && throwable.getCause() != null
                    ? throwable.getCause()
                    : throwable;

                logger.error("Batch record processing failed after retries: topic={}, partition={}, offset={}, error={}",
                    record.topic(), record.partition(), record.offset(), cause.getMessage(), cause);

                RetryService.RetryResult retryResult = retryService.classifyFailure(cause);

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

                failures.add(failureRecord);
                return false;
            });
    }

    private CompletableFuture<Void> transformAndSend(ConsumerRecord<String, String> record) {
        return CompletableFuture.supplyAsync(() -> {
                String transformedMessage = messageTransformer.transform(record.value());
                return targetKafkaTemplate.send(targetKafkaProperties.topic(), record.key(), transformedMessage);
            }, processingExecutor)
            .thenCompose(Function.identity())
            .thenAccept(result -> {
                logger.debug("Successfully sent message to topic '{}': partition={}, offset={}",
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            });
    }

    private long resolveTimeout(AdapterProperties adapterProperties) {
        if (adapterProperties.db() != null && adapterProperties.db().enabled() && adapterProperties.db().timeoutMs() > 0) {
            return adapterProperties.db().timeoutMs();
        }
        return DEFAULT_BATCH_COMPLETION_TIMEOUT_MS;
    }

    private long resolveFailureHandlingTimeout(AdapterProperties adapterProperties) {
        if (adapterProperties.db() != null && adapterProperties.db().timeoutMs() > 0) {
            return Math.max(adapterProperties.db().timeoutMs(), DEFAULT_FAILURE_HANDLING_TIMEOUT_MS);
        }
        return DEFAULT_FAILURE_HANDLING_TIMEOUT_MS;
    }
}