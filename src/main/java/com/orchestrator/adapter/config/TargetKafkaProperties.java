package com.orchestrator.adapter.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "target.kafka")
@Validated
public record TargetKafkaProperties(
    @NotBlank String topic,
    @NotBlank String bootstrapServers
) {
}