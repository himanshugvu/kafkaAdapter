package com.orchestrator.adapter.service;

import com.orchestrator.adapter.config.AdapterProperties;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

@Service
public class RetryService {

    private static final Logger logger = LoggerFactory.getLogger(RetryService.class);
    private final AdapterProperties properties;

    public RetryService(AdapterProperties properties) {
        this.properties = properties;
    }

    public <T> CompletableFuture<T> executeWithRetry(Supplier<CompletableFuture<T>> operation, String operationName) {
        int maxAttempts = Math.max(properties.retry().maxAttempts(), 1);
        return executeWithRetry(operation, operationName, 1, maxAttempts);
    }

    private <T> CompletableFuture<T> executeWithRetry(Supplier<CompletableFuture<T>> operation,
                                                      String operationName,
                                                      int attempt,
                                                      int maxAttempts) {
        return operation.get()
            .handle((result, throwable) -> {
                if (throwable == null) {
                    if (attempt > 1) {
                        logger.info("Operation '{}' succeeded after {} retry attempt(s)", operationName, attempt - 1);
                    }
                    return CompletableFuture.completedFuture(result);
                }

                Throwable cause = throwable.getCause() != null ? throwable.getCause() : throwable;

                if (!isRetriableException(cause)) {
                    logger.error("Non-retryable exception in operation '{}': {}", operationName, cause.getMessage());
                    return CompletableFuture.<T>failedFuture(cause);
                }

                if (attempt >= maxAttempts) {
                    logger.error("Operation '{}' failed after {} attempt(s): {}", operationName, attempt, cause.getMessage());
                    return CompletableFuture.<T>failedFuture(cause);
                }

                logger.warn("Retryable exception in operation '{}' (attempt {}/{}): {}",
                    operationName, attempt, maxAttempts, cause.getMessage());
                return executeWithRetry(operation, operationName, attempt + 1, maxAttempts);
            })
            .thenCompose(Function.identity());
    }

    public boolean isRetriableException(Throwable throwable) {
        if (throwable instanceof RetriableException) {
            return true;
        }

        if (throwable.getCause() instanceof RetriableException) {
            return true;
        }

        String message = throwable.getMessage();
        if (message != null) {
            String lowerMessage = message.toLowerCase();
            return lowerMessage.contains("timeout") ||
                   lowerMessage.contains("connection") ||
                   lowerMessage.contains("network") ||
                   lowerMessage.contains("unavailable") ||
                   lowerMessage.contains("retriable");
        }

        return false;
    }

    public RetryResult classifyFailure(Throwable throwable) {
        boolean isRetriable = isRetriableException(throwable);
        return new RetryResult(isRetriable, throwable.getMessage());
    }

    public static class RetryResult {
        private final boolean retriable;
        private final String errorMessage;

        public RetryResult(boolean retriable, String errorMessage) {
            this.retriable = retriable;
            this.errorMessage = errorMessage;
        }

        public boolean isRetriable() { return retriable; }
        public String getErrorMessage() { return errorMessage; }
    }
}