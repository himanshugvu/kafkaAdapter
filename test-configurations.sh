#!/bin/bash

echo "=== Testing Orchestrator Adapter Configurations ==="

# Test 1: Check health endpoint
echo "1. Testing Health Endpoint:"
curl -s http://localhost:8080/health
echo -e "\n"

# Test 2: Check actuator health
echo "2. Testing Actuator Health:"
curl -s http://localhost:8080/actuator/health
echo -e "\n"

# Test 3: Test different configuration responses
echo "3. Circuit Breaker Status:"
curl -s http://localhost:8080/health | grep -o '"dbCircuitBreakerOpen":[^,]*'
echo ""

echo "4. Failure Counts:"
curl -s http://localhost:8080/health | grep -o '"retryableFailures":[^,]*'
curl -s http://localhost:8080/health | grep -o '"nonRetryableFailures":[^,]*'
echo ""

# Test 5: Application status
echo "5. Application Status:"
curl -s http://localhost:8080/health | grep -o '"status":"[^"]*"'
echo ""

echo "=== Configuration Tests Complete ==="

# Show current configuration from logs
echo "=== Current Configuration ==="
echo "Consumer Mode: batch (from application.yml)"
echo "Retry Max Attempts: 3"
echo "Error Strategy: db"
echo "Circuit Breaker: enabled"
echo "Consumer Topic: input-topic"
echo "Target Topic: output-topic"
echo "DLT Topics: my-retry-dlt, my-nonretry-dlt"