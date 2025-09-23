package com.orchestrator.adapter.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.boot.context.properties.ConfigurationProperties;
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
    ) {}

    public record DbConfig(
        boolean enabled,
        boolean circuitBreaker,
        @Positive int bulkSize,
        long timeoutMs
    ) {}

    public enum ConsumerMode {
        RECORD, BATCH
    }

    public enum ErrorStrategy {
        DB, KAFKA, HYBRID
    }
}