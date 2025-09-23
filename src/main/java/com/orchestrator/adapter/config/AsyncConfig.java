package com.orchestrator.adapter.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
public class AsyncConfig {

    @Bean(name = "adapterAsyncExecutor")
    public Executor adapterAsyncExecutor(AdapterProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        int concurrency = Math.max(properties.consumer().concurrency(), 1);
        executor.setCorePoolSize(concurrency);
        executor.setMaxPoolSize(Math.max(concurrency * 2, concurrency + 1));
        executor.setQueueCapacity(properties.consumer().maxPollRecords() * Math.max(concurrency, 1));
        executor.setThreadNamePrefix("adapter-async-");
        executor.initialize();
        return executor;
    }
}