# Source Region Tests

This directory contains unit tests for the Source Region component of the S3 Cross-Region Compressor.

## Test Structure

- `conftest.py`: Contains shared pytest fixtures for AWS service mocking, environment setup, and test data
- `test_aws_utils.py`: Tests for AWS utility functions (S3, SQS, DynamoDB operations)
- `test_compression.py`: Tests for compression operations (TAR archive creation, ZSTD compression)
- `test_compression_manager.py`: Tests for adaptive compression level management
- `test_manifest.py`: Tests for manifest file creation and manipulation
- `test_parameters_repository.py`: Tests for DynamoDB parameter storage and retrieval
- `test_server.py`: Tests for the main server application flow

## Running the Tests

### Install Dependencies

First, install the test dependencies:

```bash
# From the bin/source_region directory
pip install -e ".[test]"  # Install the package with test dependencies
```

### Running All Tests

To run all tests:

```bash
# From the bin/source_region directory
pytest
```

### Running Specific Test Files

To run tests from a specific file:

```bash
pytest tests/test_aws_utils.py
```

### Running Specific Tests

To run a specific test or test class:

```bash
pytest tests/test_aws_utils.py::TestSQSFunctions
pytest tests/test_aws_utils.py::TestSQSFunctions::test_get_sqs_messages
```

### Run with Coverage

To run tests with coverage report:

```bash
pytest --cov=utils tests/
```

For a more detailed coverage report:

```bash
pytest --cov=utils --cov-report=term-missing tests/
```

## Mocking Strategy

The tests use several mocking strategies:

1. **AWS Services**: We use the `moto` library to mock AWS services (S3, SQS, DynamoDB, CloudWatch)
2. **Function Patching**: We use `unittest.mock` for patching functions and methods
3. **Fixtures**: Pytest fixtures are used for setting up test environments and dependencies

## Test Data

Test data is set up in `conftest.py` and includes:
- Sample S3 objects and buckets
- SQS messages and queues
- DynamoDB tables with test parameters and settings

## Testing Best Practices

- Each test should be independent and not rely on the state from other tests
- Use fixtures for shared setup and teardown
- Test both success and failure paths
- Use descriptive test names that explain what is being tested
