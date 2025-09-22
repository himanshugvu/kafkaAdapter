package com.orchestrator.adapter.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonProcessorTest {

    private JsonProcessor jsonProcessor;

    @BeforeEach
    void setUp() {
        jsonProcessor = new JsonProcessor();
    }

    @Test
    void testPassThrough() {
        String payload = "{\"test\":\"value\"}";
        String result = jsonProcessor.processStringPassThrough(payload);
        assertEquals(payload, result);
    }

    @Test
    void testLightStringEnrichment() {
        String payload = "{\"existing\":\"value\"}";
        String result = jsonProcessor.lightStringEnrichment(payload, "new", "added");
        assertEquals("{\"existing\":\"value\",\"new\":\"added\"}", result);
    }

    @Test
    void testExtractFieldValue() {
        String payload = "{\"name\":\"John\",\"age\":30}";
        String name = jsonProcessor.extractFieldValue(payload, "name");
        assertEquals("John", name);
    }

    @Test
    void testExtractFieldValueNotFound() {
        String payload = "{\"name\":\"John\"}";
        String result = jsonProcessor.extractFieldValue(payload, "nonexistent");
        assertNull(result);
    }

    @Test
    void testPrefixPayload() {
        String payload = "test";
        String result = jsonProcessor.prefixPayload(payload, "prefix:");
        assertEquals("prefix:test", result);
    }

    @Test
    void testSuffixPayload() {
        String payload = "test";
        String result = jsonProcessor.suffixPayload(payload, ":suffix");
        assertEquals("test:suffix", result);
    }

    @Test
    void testWrapPayload() {
        String payload = "{\"inner\":\"value\"}";
        String result = jsonProcessor.wrapPayload(payload, "wrapper");
        assertEquals("{\"wrapper\":{\"inner\":\"value\"}}", result);
    }

    @Test
    void testIsValidJson() {
        assertTrue(jsonProcessor.isValidJson("{\"valid\":\"json\"}"));
        assertFalse(jsonProcessor.isValidJson("invalid json"));
        assertFalse(jsonProcessor.isValidJson(""));
        assertFalse(jsonProcessor.isValidJson(null));
    }
}