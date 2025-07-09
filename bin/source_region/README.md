# Source Region Component

This is the Source Region component of the S3 Cross-Region Compressor system. It's responsible for:

1. Listening to SQS messages for S3 object creation events
2. Downloading created objects from S3
3. Compressing the objects using adaptive ZSTD compression
4. Creating a manifest file with object metadata
5. Uploading the compressed archive to an outbound bucket for cross-region replication

## Environment Variables

The application requires the following environment variables:

- `SQS_QUEUE_URL`: URL of the SQS queue to poll for messages
- `BUCKET`: Name of the outbound S3 bucket
- `AWS_DEFAULT_REGION`: AWS region
- `STACK_NAME`: CloudFormation stack name
- `MONITORED_PREFIX`: (Optional) The root prefix being monitored (can be empty for bucket root)
- `LOG_LEVEL`: (Optional, default: INFO) Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `COMPRESSION_SETTINGS_TABLE`: DynamoDB table for compression settings
- `REPLICATION_PARAMETERS_TABLE`: DynamoDB table for replication parameters

## Runtime Dependencies

The application depends on:

- boto3/botocore for AWS service access
- pyzstd for high-performance compression
- cachetools for caching DynamoDB lookups
- psutil for memory management
- python-json-logger for structured logging

## Running the Application

The application is designed to be run in a container:

```bash
docker build -t source-region .
docker run -d \
  -e SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/my-queue \
  -e BUCKET=my-outbound-bucket \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -e STACK_NAME=my-stack \
  source-region
```

## Running the Tests

This component includes a comprehensive test suite using pytest, pytest-mock, and moto for AWS service mocking.

### Install Test Dependencies

```bash
pip install -e ".[test]"
```

### Run All Tests

```bash
pytest
```

### Run with Coverage Report

```bash
pytest --cov=utils tests/
```

See the [tests/README.md](tests/README.md) file for more detailed information on the test suite structure and running specific tests.

## Features

### Adaptive Compression

The component uses an adaptive compression system that:
- Monitors compression performance metrics
- Dynamically adjusts compression level for optimal throughput and cost
- Stores and retrieves compression settings in DynamoDB
- Makes data-driven decisions to balance compression ratio and processing time

### Efficient Resource Usage

- Memory-efficient streaming compression using zstd
- Parallel S3 object processing using ThreadPoolExecutor
- Automatic buffer size tuning based on available memory
- CPU benchmarking for normalized performance metrics

### Robust Error Handling

- Graceful handling of S3 download/upload failures
- Retries with exponential backoff for DynamoDB operations
- Protection against SQS message processing failures
- Handling of S3 test events and invalid messages
