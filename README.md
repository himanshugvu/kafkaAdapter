# Orchestrator Adapter

A high-performance Kafka message orchestrator adapter that implements configurable consumer modes, minimal JSON handling, retry policies, and comprehensive error handling strategies.

## Features

### Consumer Modes
- **Non-Batch Consumer**: Processes one record at a time with immediate transformation and publishing
- **Batch Consumer**: Processes multiple records in a single poll cycle with bulk error handling

### JSON Handling
- **Default Mode**: String pass-through for maximum performance
- **Light Enrichment**: String concatenation/slicing without full JSON parsing
- **Streaming Extraction**: Field extraction using Jackson streaming parser
- **Full Parsing**: Complete JSON transformation when strictly necessary

### Retry Policy
- Configurable retry attempts (default: 3)
- Retries only for retryable Kafka exceptions
- Non-retryable exceptions immediately treated as permanent failures
- Exponential backoff with circuit breaker pattern

### Error Handling Strategies

#### DB-Backed Strategy
- Insert failure events into database table
- Circuit breaker protection during DB outages
- Bulk insert optimization for batch processing

#### Kafka DLT Strategy
- Route failures to Dead Letter Topics (DLT)
- Separate topics for retryable vs non-retryable errors
- Configurable topic names and bootstrap servers

#### Hybrid Strategy
- Primary: DB insert with circuit breaker
- Fallback: Kafka DLT when DB unavailable

### Failure Routing
| Failure Type | Destination |
|--------------|-------------|
| Retryable error | Retry N attempts → Retryable DLT / DB |
| Non-retryable | Immediately → Non-retryable DLT / DB |

## Configuration

```yaml
adapter:
  consumer:
    mode: batch                    # batch | record
    topic: input-topic
    group-id: orchestrator-adapter-group
    bootstrap-servers: localhost:9092
    max-poll-records: 500
    poll-timeout-ms: 3000
    concurrency: 1
  retry:
    max-attempts: 3
  error:
    strategy: db                   # db | kafka | hybrid
  dlt:
    retryable-topic: my-retry-dlt
    non-retryable-topic: my-nonretry-dlt
    bootstrap-servers: localhost:9092
  db:
    enabled: true
    circuit-breaker: true
    bulk-size: 100
    timeout-ms: 5000
```

## Building and Running

### Prerequisites
- Java 21+
- Maven 3.8+
- Kafka cluster
- Database (H2, PostgreSQL)

### Build
```bash
mvn clean package
```

### Run
```bash
java -jar target/orchestrator-adapter-1.0.0.jar
```

### Environment Variables
- `KAFKA_BROKERS`: Kafka bootstrap servers
- `CONSUMER_TOPIC`: Input topic name
- `CONSUMER_GROUP`: Consumer group ID
- `TARGET_TOPIC`: Output topic name
- `DLT_RETRY_TOPIC`: Retryable DLT topic
- `DLT_NONRETRY_TOPIC`: Non-retryable DLT topic
- `DB_URL`: Database connection URL
- `DB_USERNAME`: Database username
- `DB_PASSWORD`: Database password

## Processing Flows

### Non-Batch Mode
1. Consume one record
2. Transform payload (minimal JSON handling)
3. Send to target Kafka topic
4. Retry on retryable failures (up to max attempts)
5. Route unrecoverable failures via error handler
6. Commit offset after completion

### Batch Mode
1. Consume list of records
2. Process each record:
   - Transform payload
   - Send to Kafka
   - Retry on retryable failures
   - Collect unrecoverable failures
3. Bulk flush failures (DB insert or DLT send)
4. Commit offsets for entire batch

## Health Monitoring

Access health endpoint at `/health`:
```json
{
  "status": "UP",
  "dbCircuitBreakerOpen": false,
  "retryableFailures": 0,
  "nonRetryableFailures": 0
}
```

## Architecture

The adapter follows a clean architecture pattern with separation of concerns:

- **Config Layer**: Configuration properties and Kafka setup
- **Service Layer**: Business logic for processing, retry, and error handling
- **Repository Layer**: Data access for failure records
- **Controller Layer**: Health and monitoring endpoints

## Performance Optimizations

- Virtual threads for concurrent processing
- Minimal JSON parsing by default
- Bulk database operations
- Circuit breaker for resilience
- Configurable batch sizes and timeouts
- Connection pooling and producer reuse

## Testing

Run tests:
```bash
mvn test
```

Integration tests use TestContainers for Kafka and database testing.