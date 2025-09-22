package com.orchestrator.adapter.service;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Service
public class JsonProcessor {

    private static final Logger logger = LoggerFactory.getLogger(JsonProcessor.class);
    private final ObjectMapper objectMapper;
    private final ObjectReader objectReader;
    private final JsonFactory jsonFactory;

    public JsonProcessor() {
        this.objectMapper = new ObjectMapper();
        this.objectReader = objectMapper.reader();
        this.jsonFactory = new JsonFactory();
    }

    public String processStringPassThrough(String payload) {
        return payload;
    }

    public String lightStringEnrichment(String payload, String enrichmentKey, String enrichmentValue) {
        if (payload == null || payload.isEmpty()) {
            return payload;
        }

        if (!payload.trim().startsWith("{") || !payload.trim().endsWith("}")) {
            return payload;
        }

        if (payload.length() > 1 && payload.charAt(payload.length() - 1) == '}') {
            StringBuilder sb = new StringBuilder(payload.length() + enrichmentKey.length() + enrichmentValue.length() + 10);
            sb.append(payload, 0, payload.length() - 1);
            sb.append(",\"").append(enrichmentKey).append("\":\"").append(enrichmentValue).append("\"}");
            return sb.toString();
        }

        return payload;
    }

    public String extractFieldValue(String payload, String fieldName) {
        if (payload == null || payload.isEmpty()) {
            return null;
        }

        try {
            String searchPattern = "\"" + fieldName + "\":";
            int startIndex = payload.indexOf(searchPattern);
            if (startIndex == -1) {
                return null;
            }

            startIndex += searchPattern.length();
            while (startIndex < payload.length() && Character.isWhitespace(payload.charAt(startIndex))) {
                startIndex++;
            }

            if (startIndex >= payload.length()) {
                return null;
            }

            char firstChar = payload.charAt(startIndex);
            if (firstChar == '"') {
                int endIndex = payload.indexOf('"', startIndex + 1);
                if (endIndex != -1) {
                    return payload.substring(startIndex + 1, endIndex);
                }
            } else {
                int endIndex = startIndex;
                while (endIndex < payload.length() &&
                       payload.charAt(endIndex) != ',' &&
                       payload.charAt(endIndex) != '}' &&
                       payload.charAt(endIndex) != ']') {
                    endIndex++;
                }
                return payload.substring(startIndex, endIndex).trim();
            }
        } catch (Exception e) {
            logger.debug("Failed to extract field {} from payload using string method: {}", fieldName, e.getMessage());
        }

        return null;
    }

    public String prefixPayload(String payload, String prefix) {
        if (payload == null || payload.isEmpty()) {
            return payload;
        }

        return prefix + payload;
    }

    public String suffixPayload(String payload, String suffix) {
        if (payload == null || payload.isEmpty()) {
            return payload;
        }

        return payload + suffix;
    }

    public String wrapPayload(String payload, String wrapper) {
        if (payload == null || payload.isEmpty()) {
            return payload;
        }

        return "{\"" + wrapper + "\":" + payload + "}";
    }

    public String transformWithFullJsonParsing(String payload, JsonTransformer transformer) {
        if (payload == null || payload.isEmpty()) {
            return payload;
        }

        try {
            JsonNode rootNode = objectReader.readTree(payload);
            JsonNode transformedNode = transformer.transform(rootNode);
            return objectMapper.writeValueAsString(transformedNode);
        } catch (IOException e) {
            logger.error("Failed to parse JSON payload for full transformation: {}", e.getMessage());
            return payload;
        }
    }

    public Map<String, String> extractMultipleFields(String payload, String... fieldNames) {
        Map<String, String> result = new HashMap<>();

        if (payload == null || payload.isEmpty() || fieldNames == null) {
            return result;
        }

        for (String fieldName : fieldNames) {
            String value = extractFieldValue(payload, fieldName);
            if (value != null) {
                result.put(fieldName, value);
            }
        }

        return result;
    }

    public boolean isValidJson(String payload) {
        if (payload == null || payload.isEmpty()) {
            return false;
        }

        try (JsonParser parser = jsonFactory.createParser(payload)) {
            while (parser.nextToken() != null) {
                // Just parse through to validate
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public String streamingExtractField(String payload, String fieldName) {
        if (payload == null || payload.isEmpty()) {
            return null;
        }

        try (JsonParser parser = jsonFactory.createParser(payload)) {
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.getCurrentToken() == JsonToken.FIELD_NAME &&
                    fieldName.equals(parser.getCurrentName())) {
                    parser.nextToken();
                    return parser.getValueAsString();
                }
            }
        } catch (IOException e) {
            logger.debug("Failed to extract field {} using streaming parser: {}", fieldName, e.getMessage());
        }

        return null;
    }

    @FunctionalInterface
    public interface JsonTransformer {
        JsonNode transform(JsonNode input);
    }
}