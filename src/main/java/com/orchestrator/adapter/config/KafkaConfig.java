package com.orchestrator.adapter.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableConfigurationProperties({AdapterProperties.class, TargetKafkaProperties.class})
public class KafkaConfig {

    private final AdapterProperties properties;
    private final TargetKafkaProperties targetProperties;

    public KafkaConfig(AdapterProperties properties, TargetKafkaProperties targetProperties) {
        this.properties = properties;
        this.targetProperties = targetProperties;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // Basic Configuration
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.consumer().bootstrapServers());
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, properties.consumer().groupId());
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Offset and Commit Configuration
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // Performance Configuration
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.consumer().maxPollRecords());
        configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 minutes
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45000);
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 15000);

        // Fetch Configuration
        configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        configProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        configProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 52428800); // 50MB
        configProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576); // 1MB

        // Network Configuration
        configProps.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 540000);
        configProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        configProps.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
        configProps.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
        configProps.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

        // Partition Assignment Strategy
        configProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            "org.apache.kafka.clients.consumer.RangeAssignor,org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        // Metadata Configuration
        configProps.put("metadata.max.age.ms", 300000);

        // Client Configuration
        configProps.put(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");
        configProps.put(ConsumerConfig.CHECK_CRCS_CONFIG, true);

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> batchKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(properties.consumer().concurrency());
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(properties.consumer().pollTimeoutMs());
        factory.getContainerProperties().setMicrometerEnabled(true);

        // Batch-specific configuration
        factory.getContainerProperties().setSyncCommits(true);

        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> recordConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // Basic Configuration
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.consumer().bootstrapServers());
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, properties.consumer().groupId());
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Offset and Commit Configuration
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // Record Mode Performance Configuration - Optimized for single record processing
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1); // Always 1 for record mode
        configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60000); // Shorter for faster processing
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // Shorter timeout
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // More frequent heartbeats

        // Fetch Configuration - Optimized for low latency
        configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1); // Get data immediately
        configProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100); // Low wait time
        configProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1048576); // Smaller fetch size
        configProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 65536); // Smaller partition fetch

        // Network Configuration - Optimized for responsiveness
        configProps.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 300000);
        configProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000); // Shorter timeout
        configProps.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 25);
        configProps.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 500);
        configProps.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 50);

        // Partition Assignment Strategy - Single assignor for simplicity
        configProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            "org.apache.kafka.clients.consumer.RangeAssignor");

        // Metadata Configuration
        configProps.put("metadata.max.age.ms", 300000);

        // Client Configuration
        configProps.put(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");
        configProps.put(ConsumerConfig.CHECK_CRCS_CONFIG, true);

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> recordKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(recordConsumerFactory()); // Use dedicated record consumer factory
        factory.setConcurrency(Math.max(properties.consumer().concurrency(), 1));
        factory.setBatchListener(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(properties.consumer().pollTimeoutMs());
        factory.getContainerProperties().setMicrometerEnabled(true);

        factory.getContainerProperties().setSyncCommits(false);
        factory.getContainerProperties().setCommitRetries(1);

        return factory;
    }

    @Bean
    @ConditionalOnProperty(name = "adapter.dlt.enabled", havingValue = "true")
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // Basic Configuration
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getDltBootstrapServers());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Reliability Configuration
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        // Timeout Configuration
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

        // Performance Configuration
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432L); // 32MB
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // Network Configuration
        configProps.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 540000);
        configProps.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
        configProps.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);

        // Metadata Configuration
        configProps.put("metadata.max.age.ms", 300000);

        // Client Configuration
        configProps.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    @ConditionalOnProperty(name = "adapter.dlt.enabled", havingValue = "true")
    public KafkaTemplate<String, String> kafkaTemplate() {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory());
        return template;
    }

    @Bean
    public ProducerFactory<String, String> targetProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // Basic Configuration
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, targetProperties.bootstrapServers());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Reliability Configuration
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // Timeout Configuration
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

        // High Throughput Configuration
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L); // 64MB
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // Network Configuration
        configProps.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 540000);
        configProps.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
        configProps.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);

        // Metadata Configuration
        configProps.put("metadata.max.age.ms", 300000);

        // Client Configuration
        configProps.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean("targetKafkaTemplate")
    public KafkaTemplate<String, String> targetKafkaTemplate() {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(targetProducerFactory());
        template.setDefaultTopic(targetProperties.topic());
        return template;
    }

    private String getDltBootstrapServers() {
        return properties.dlt() != null && properties.dlt().bootstrapServers() != null
            ? properties.dlt().bootstrapServers()
            : properties.consumer().bootstrapServers();
    }
}