package com.orchestrator.adapter.controller;

import com.orchestrator.adapter.service.ErrorHandlingService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/health")
public class HealthController {

    private final ErrorHandlingService errorHandlingService;

    public HealthController(ErrorHandlingService errorHandlingService) {
        this.errorHandlingService = errorHandlingService;
    }

    @GetMapping
    public Map<String, Object> health() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("dbCircuitBreakerOpen", errorHandlingService.isDbCircuitBreakerOpen());
        health.put("retryableFailures", errorHandlingService.getRetryableFailureCount());
        health.put("nonRetryableFailures", errorHandlingService.getNonRetryableFailureCount());
        return health;
    }
}