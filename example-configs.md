# Orchestrator Adapter - Consumer Mode Examples

## 1. Batch Mode Configuration (Default)

```yaml
adapter:
  consumer:
    mode: batch                    # Uses BatchMessageConsumerService
    topic: input-topic
    group-id: orchestrator-adapter-group
    bootstrap-servers: localhost:9092
    max-poll-records: 500          # Processes up to 500 records per batch
    poll-timeout-ms: 3000
    concurrency: 1
  retry:
    max-attempts: 3
  error:
    strategy: db
  dlt:
    enabled: true                  # Enable DLT functionality
    retryable-topic: my-retry-dlt
    non-retryable-topic: my-nonretry-dlt
  db:
    enabled: true
    circuit-breaker: true
```

**Container Factory**: `batchKafkaListenerContainerFactory`
- Batch listener: `true`
- Sync commits: `true`
- Better for high throughput scenarios

## 2. Record Mode Configuration

```yaml
adapter:
  consumer:
    mode: record                   # Uses RecordMessageConsumerService
    topic: input-topic
    group-id: orchestrator-adapter-group
    bootstrap-servers: localhost:9092
    max-poll-records: 500          # Batch config value (not used in record mode)
    poll-timeout-ms: 1000
    concurrency: 3                 # Higher concurrency for record mode
  retry:
    max-attempts: 3
  error:
    strategy: kafka
  dlt:
    enabled: true                  # Enable DLT functionality
    retryable-topic: my-retry-dlt
    non-retryable-topic: my-nonretry-dlt
```

**Container Factory**: `recordKafkaListenerContainerFactory`
- Uses dedicated `recordConsumerFactory()` with optimized settings
- Batch listener: `false`
- Sync commits: `false` (async for speed)
- Better for low latency scenarios
- **Note**: Record mode uses a separate consumer factory optimized for single record processing (max-poll-records=1), regardless of application configuration

## Enhanced Kafka Configuration

### Consumer Configuration

#### Batch Consumer Factory (`consumerFactory`)
- **Basic**: Bootstrap servers, group ID, deserializers
- **Offset Management**: Manual commits, read committed isolation
- **Performance**: Configurable max poll records (default 500), 5-minute poll interval
- **Fetch**: 1KB min bytes, 500ms wait, 50MB max bytes, 1MB partition fetch
- **Network**: Connection pooling, retry backoffs
- **Partition Assignment**: Range + Cooperative sticky assignors

#### Record Consumer Factory (`recordConsumerFactory`)
- **Basic**: Same as batch consumer
- **Performance**: Fixed max poll records (1), 1-minute poll interval, frequent heartbeats
- **Fetch**: Immediate (1 byte min), 100ms wait, smaller fetch sizes (1MB/64KB)
- **Network**: Optimized for responsiveness with shorter timeouts
- **Partition Assignment**: Range assignor only for simplicity

### Producer Configuration
- **DLT Producer**: For error handling (snappy compression, conservative settings)
- **Target Producer**: For main output (lz4 compression, high throughput)
- **Reliability**: All acks, idempotence, infinite retries
- **Performance**: Optimized batch sizes, linger times, buffer memory

## Mode-Specific Behaviors

### Batch Mode (`BatchMessageConsumerService`)
- Processes multiple records simultaneously
- Collects failures across the batch
- Bulk error handling (DB inserts or DLT sends)
- Commits offsets only after entire batch completion
- Uses `@ConditionalOnProperty(name = "adapter.consumer.mode", havingValue = "batch")`

### Record Mode (`RecordMessageConsumerService`)
- Processes one record at a time
- Immediate error handling per record
- Individual offset commits
- Lower latency, higher overhead
- Uses `@ConditionalOnProperty(name = "adapter.consumer.mode", havingValue = "record")`

## Architecture Notes

### Consumer Factory Selection
- **Batch Mode**: Uses `consumerFactory()` with configurable `max-poll-records` from properties
- **Record Mode**: Uses dedicated `recordConsumerFactory()` with fixed `max-poll-records=1` and low-latency optimizations
- The record mode consumer factory is specifically designed for single record processing efficiency, independent of application configuration

### Container Factory Configuration
- **Batch Container**: Higher concurrency (configurable), sync commits, batch processing
- **Record Container**: Higher minimum concurrency (3+), async commits, single record processing

## Runtime Switching

To switch modes, update the configuration and restart:

```bash
# Batch Mode (uses consumerFactory)
java -jar orchestrator-adapter-1.0.0.jar --adapter.consumer.mode=batch

# Record Mode (uses recordConsumerFactory)
java -jar orchestrator-adapter-1.0.0.jar --adapter.consumer.mode=record
```

Only the appropriate consumer service will be instantiated based on the configuration.

## DLT Configuration Options

### Enabling DLT (Dead Letter Topics)

```yaml
adapter:
  error:
    strategy: kafka                # or hybrid
  dlt:
    enabled: true                  # Enable DLT functionality
    retryable-topic: my-retry-dlt
    non-retryable-topic: my-nonretry-dlt
    bootstrap-servers: localhost:9092
```

### Disabling DLT (DB-only error handling)

```yaml
adapter:
  error:
    strategy: db                   # DB-only strategy
  dlt:
    enabled: false                 # Disable DLT functionality
  db:
    enabled: true
    circuit-breaker: true
```

When DLT is disabled:
- The DLT producer and KafkaTemplate beans are not created
- Kafka/Hybrid error strategies gracefully fall back to DB-only or no-op
- Zero DLT-related overhead or dependencies
- Easy to remove DLT code by setting `adapter.dlt.enabled: false`