# Target Region Tests

This directory contains the test suite for the target_region application.

## Test Structure

- `conftest.py`: Shared pytest fixtures for use across all test files
- `test_aws_utils.py`: Tests for AWS service interactions
- `test_decompression.py`: Tests for decompression utilities
- `test_manifest.py`: Tests for manifest handling
- `test_metrics.py`: Tests for metrics reporting
- `test_server.py`: Tests for main application logic

## Running Tests

Due to the project's structure with multiple test directories, there are several ways to run tests:

### Using VS Code Tasks (Recommended)

The project includes VS Code tasks for easy test execution:

1. Press `Ctrl+Shift+P` (or `Cmd+Shift+P` on macOS) 
2. Type "Tasks: Run Task"
3. Select one of:
   - "Run Target Region Tests" - to run only target region tests
   - "Run Source Region Tests" - to run only source region tests
   - "Run All Tests" - to run all tests sequentially

### Using VS Code Launch Configurations

Launch configurations are set up for debugging tests:

1. Open the Run and Debug panel (`Ctrl+Shift+D` or `Cmd+Shift+D`)
2. Select "Python: Target Region Tests" from the dropdown
3. Click the play button or press F5

### Using Command Line

To run from the command line:

```bash
# From project root directory
python -m pytest bin/target_region/tests

# Run specific test file
python -m pytest bin/target_region/tests/test_aws_utils.py

# Run specific test
python -m pytest bin/target_region/tests/test_server.py::TestMessageBatchProcessing::test_process_message_batch_full_flow

# Run tests with coverage
python -m pytest bin/target_region/tests --cov=bin.target_region
```

## Test Environment

Tests use:
- `pytest` as the test framework
- `pytest-mock` for mocking 
- `moto` for AWS service mocking

The tests are designed to run without actual AWS resources or credentials.
