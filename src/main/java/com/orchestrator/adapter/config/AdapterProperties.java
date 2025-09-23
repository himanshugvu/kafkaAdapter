package com.orchestrator.adapter.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "adapter")
@Validated
public record AdapterProperties(
    @NotNull @Valid ConsumerConfig consumer,
    @NotNull @Valid RetryConfig retry,
    @NotNull @Valid ErrorConfig error,
    @Valid DltConfig dlt,
    @Valid DbConfig db
) {

    public record ConsumerConfig(
        @NotBlank String topic,
        @NotBlank String groupId,
        @NotBlank String bootstrapServers,
        @NotNull ConsumerMode mode,
        @Positive int maxPollRecords,
        long pollTimeoutMs,
        @Positive int concurrency
    ) {}

    public record RetryConfig(
        @PositiveOrZero int maxAttempts
    ) {}

    public record ErrorConfig(
        @NotNull ErrorStrategy strategy
    ) {}

    public record DltConfig(
        boolean enabled,
        String retryableTopic,
        String nonRetryableTopic,
        String bootstrapServers
    ) {

        @AssertTrue(message = "Retryable and non-retryable DLT topics must be configured when DLT is enabled")
        public boolean isTopicConfigurationValid() {
            if (!enabled) {
                return true;
            }
            return StringUtils.hasText(retryableTopic) && StringUtils.hasText(nonRetryableTopic);
        }

        @AssertTrue(message = "DLT bootstrap servers must be configured when DLT is enabled")
        public boolean isBootstrapConfigurationValid() {
            return !enabled || StringUtils.hasText(bootstrapServers);
        }
    }

    public record DbConfig(
        boolean enabled,
        boolean circuitBreaker,
        @Positive int bulkSize,
        @Positive long timeoutMs
    ) {

        @AssertTrue(message = "Circuit breaker cannot be enabled when the DB is disabled")
        public boolean isCircuitBreakerConfigurationValid() {
            return enabled || !circuitBreaker;
        }
    }

    public enum ConsumerMode {
        RECORD, BATCH
    }

    public enum ErrorStrategy {
        DB, KAFKA, HYBRID
    }
}