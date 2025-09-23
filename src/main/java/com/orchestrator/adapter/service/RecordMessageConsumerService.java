package com.orchestrator.adapter.service;

import com.orchestrator.adapter.config.AdapterProperties;
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

@Service
@ConditionalOnProperty(name = "adapter.consumer.mode", havingValue = "record")
public class RecordMessageConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(RecordMessageConsumerService.class);

    private final AdapterProperties properties;
    private final MessageTransformer messageTransformer;
    private final RetryService retryService;
    private final ErrorHandlingService errorHandlingService;
    private final KafkaTemplate<String, String> targetKafkaTemplate;

    public RecordMessageConsumerService(AdapterProperties properties,
                                       MessageTransformer messageTransformer,
                                       RetryService retryService,
                                       ErrorHandlingService errorHandlingService,
                                       @Qualifier("targetKafkaTemplate") KafkaTemplate<String, String> targetKafkaTemplate) {
        this.properties = properties;
        this.messageTransformer = messageTransformer;
        this.retryService = retryService;
        this.errorHandlingService = errorHandlingService;
        this.targetKafkaTemplate = targetKafkaTemplate;

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

        processRecord(record)
            .thenAccept(success -> {
                if (success) {
                    logger.debug("Record processed successfully, acknowledging offset");
                } else {
                    logger.warn("Record processing failed, but acknowledging offset");
                }
                acknowledgment.acknowledge();
            })
            .exceptionally(throwable -> {
                logger.error("Unexpected error processing record: {}", throwable.getMessage());
                acknowledgment.acknowledge();
                return null;
            });
    }

    private CompletableFuture<Boolean> processRecord(ConsumerRecord<String, String> record) {
        return retryService.executeWithRetry(
            () -> transformAndSend(record),
            "processRecord"
        )
        .thenApply(result -> {
            logger.debug("Record processed successfully: topic={}, partition={}, offset={}",
                        record.topic(), record.partition(), record.offset());
            return true;
        })
        .exceptionally(throwable -> {
            logger.error("Record processing failed after retries: topic={}, partition={}, offset={}, error={}",
                        record.topic(), record.partition(), record.offset(), throwable.getMessage());
            errorHandlingService.handleFailure(record, throwable, properties.retry().maxAttempts());
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