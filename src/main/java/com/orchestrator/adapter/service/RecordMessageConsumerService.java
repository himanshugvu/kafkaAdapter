package com.orchestrator.adapter.service;

import com.orchestrator.adapter.config.AdapterProperties;
import com.orchestrator.adapter.config.TargetKafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@Service
@ConditionalOnProperty(name = "adapter.consumer.mode", havingValue = "record")
public class RecordMessageConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(RecordMessageConsumerService.class);
    private static final long DEFAULT_RECORD_PROCESSING_TIMEOUT_MS = 5_000L;

    private final AdapterProperties properties;
    private final MessageTransformer messageTransformer;
    private final RetryService retryService;
    private final ErrorHandlingService errorHandlingService;
    private final KafkaTemplate<String, String> targetKafkaTemplate;
    private final TargetKafkaProperties targetKafkaProperties;
    private final Executor processingExecutor;
    private final long recordProcessingTimeoutMs;

    public RecordMessageConsumerService(AdapterProperties properties,
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
        this.recordProcessingTimeoutMs = resolveProcessingTimeout(properties);

        logger.info("RecordMessageConsumerService initialized - RECORD mode enabled");
    }

    @KafkaListener(
        topics = "${adapter.consumer.topic}",
        containerFactory = "recordKafkaListenerContainerFactory"
    )
    public void consumeRecordMessages(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        logger.debug("RECORD MODE: Received single record from partition {} offset {}",
            record.partition(), record.offset());
        processRecordMode(record, acknowledgment);
    }

    private void processRecordMode(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        logger.debug("Processing in RECORD mode - topic: {}, partition: {}, offset: {}",
            record.topic(), record.partition(), record.offset());

        boolean success = false;
        CompletableFuture<Boolean> processingFuture = processRecord(record);

        try {
            success = processingFuture.get(recordProcessingTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            processingFuture.cancel(true);
            logger.error("Record processing timed out after {} ms: topic={}, partition={}, offset={}",
                recordProcessingTimeoutMs, record.topic(), record.partition(), record.offset(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Record processing interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            logger.error("Unexpected failure while processing record: {}", cause.getMessage(), cause);
        } finally {
            acknowledgment.acknowledge();
        }

        if (success) {
            logger.debug("Record processed successfully, acknowledging offset");
        } else {
            logger.warn("Record processed with failures, offset acknowledged to avoid re-delivery");
        }
    }

    private CompletableFuture<Boolean> processRecord(ConsumerRecord<String, String> record) {
        return retryService.executeWithRetry(
                () -> transformAndSend(record),
                "processRecord"
            )
            .handle((result, throwable) -> {
                if (throwable == null) {
                    logger.debug("Record processed successfully: topic={}, partition={}, offset={}",
                        record.topic(), record.partition(), record.offset());
                    return CompletableFuture.completedFuture(true);
                }

                Throwable cause = throwable instanceof CompletionException && throwable.getCause() != null
                    ? throwable.getCause()
                    : throwable;

                logger.error("Record processing failed after retries: topic={}, partition={}, offset={}, error={}",
                    record.topic(), record.partition(), record.offset(), cause.getMessage(), cause);

                return errorHandlingService.handleFailure(record, cause, properties.retry().maxAttempts())
                    .handle((ignored, handlingError) -> {
                        if (handlingError != null) {
                            Throwable handlingCause = handlingError.getCause() != null ? handlingError.getCause() : handlingError;
                            logger.error("Failure handling encountered an error: {}", handlingCause.getMessage(), handlingCause);
                        }
                        return false;
                    });
            })
            .thenCompose(Function.identity());
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

    private long resolveProcessingTimeout(AdapterProperties adapterProperties) {
        long configuredTimeout = adapterProperties.consumer().pollTimeoutMs();
        if (configuredTimeout > 0) {
            return configuredTimeout;
        }
        return DEFAULT_RECORD_PROCESSING_TIMEOUT_MS;
    }
}