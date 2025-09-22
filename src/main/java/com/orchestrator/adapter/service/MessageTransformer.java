package com.orchestrator.adapter.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MessageTransformer {

    private static final Logger logger = LoggerFactory.getLogger(MessageTransformer.class);
    private final JsonProcessor jsonProcessor;

    public MessageTransformer(JsonProcessor jsonProcessor) {
        this.jsonProcessor = jsonProcessor;
    }

    public String transform(String payload) {
        if (payload == null || payload.isEmpty()) {
            return payload;
        }

        try {
            return performTransformation(payload);
        } catch (Exception e) {
            logger.error("Failed to transform payload: {}", e.getMessage());
            return payload;
        }
    }

    private String performTransformation(String payload) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        return jsonProcessor.lightStringEnrichment(payload, "processedAt", timestamp);
    }

    public String transformWithEnrichment(String payload, String key, String value) {
        if (payload == null || payload.isEmpty()) {
            return payload;
        }

        try {
            return jsonProcessor.lightStringEnrichment(payload, key, value);
        } catch (Exception e) {
            logger.error("Failed to enrich payload with key {} and value {}: {}", key, value, e.getMessage());
            return payload;
        }
    }

    public String extractAndTransform(String payload, String extractField, String enrichField, String enrichValue) {
        if (payload == null || payload.isEmpty()) {
            return payload;
        }

        try {
            String extractedValue = jsonProcessor.extractFieldValue(payload, extractField);
            if (extractedValue != null) {
                logger.debug("Extracted field {}: {}", extractField, extractedValue);
            }

            return jsonProcessor.lightStringEnrichment(payload, enrichField, enrichValue);
        } catch (Exception e) {
            logger.error("Failed to extract and transform payload: {}", e.getMessage());
            return payload;
        }
    }

    public String wrapTransform(String payload, String wrapperKey) {
        if (payload == null || payload.isEmpty()) {
            return payload;
        }

        try {
            return jsonProcessor.wrapPayload(payload, wrapperKey);
        } catch (Exception e) {
            logger.error("Failed to wrap payload: {}", e.getMessage());
            return payload;
        }
    }
}