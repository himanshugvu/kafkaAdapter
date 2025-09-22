package com.orchestrator.adapter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

@SpringBootApplication
@EnableRetry
public class OrchestratorAdapterApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrchestratorAdapterApplication.class, args);
    }
}