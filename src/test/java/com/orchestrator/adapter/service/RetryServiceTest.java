package com.orchestrator.adapter.service;

import com.orchestrator.adapter.config.AdapterProperties;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RetryServiceTest {

    private RetryService retryService;

    @BeforeEach
    void setUp() {
        AdapterProperties.RetryConfig retryConfig = new AdapterProperties.RetryConfig(3);
        AdapterProperties properties = new AdapterProperties(
            null, retryConfig, null, null, null
        );
        retryService = new RetryService(properties);
    }

    @Test
    void testIsRetriableException() {
        assertTrue(retryService.isRetriableException(new TimeoutException("test")));
        assertFalse(retryService.isRetriableException(new IllegalArgumentException("test")));
        assertTrue(retryService.isRetriableException(new RuntimeException("Connection timeout")));
        assertTrue(retryService.isRetriableException(new RuntimeException("Network unavailable")));
    }

    @Test
    void testClassifyFailure() {
        RetryService.RetryResult retriableResult = retryService.classifyFailure(new TimeoutException("test"));
        assertTrue(retriableResult.isRetriable());
        assertEquals("test", retriableResult.getErrorMessage());

        RetryService.RetryResult nonRetriableResult = retryService.classifyFailure(new IllegalArgumentException("test"));
        assertFalse(nonRetriableResult.isRetriable());
        assertEquals("test", nonRetriableResult.getErrorMessage());
    }
}