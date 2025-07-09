"""
CPU Benchmark utility for normalizing compression performance across different CPU generations.

This module provides a benchmark that measures CPU compression performance using pyzstd,
the same library used for the actual compression tasks.
"""

import logging
import time
import random
from pyzstd import compress

# Configure logging
logger = logging.getLogger(__name__)

# Reference system performance (measured on reference AWS Fargate configuration)
REFERENCE_COMPRESSION_OPS = 100.0  # Operations per second on reference system


def run_cpu_benchmark(max_duration=10):
	"""
	Run CPU benchmark to determine performance normalization factor specifically
	for compression workloads.

	Args:
	    max_duration: Maximum benchmark duration in seconds

	Returns:
	    CPU factor (higher means slower CPU)
	"""
	logger.info(f'Running CPU compression benchmark (max {max_duration}s)...')

	# Generate test data (4MB of pseudorandom data)
	random.seed(42)  # Fixed seed for consistency
	test_data = bytes([random.randrange(0, 256) for _ in range(4 * 1024 * 1024)])
	random.seed(None)  # Unfixed seed for other random tasks

	# Run multiple compression tests at level 10 (medium level)
	iterations = 0
	total_time = 0
	start_time = time.time()
	elapsed = 0

	while elapsed < max_duration and iterations < 20:
		# Time compression operation
		compress_start = time.time()
		compress(test_data, 10)
		compress_time = time.time() - compress_start

		total_time += compress_time
		iterations += 1
		elapsed = time.time() - start_time

		# If we've run for at least half the max duration and have at least 3 iterations,
		# we have enough data for a reliable estimate
		if elapsed >= (max_duration / 2) and iterations >= 3:
			break

	# Calculate operations per second
	if iterations > 0 and total_time > 0:
		ops_per_second = iterations / total_time

		# CPU factor is the ratio of reference performance to measured performance
		# Higher factor means slower CPU relative to reference
		cpu_factor = REFERENCE_COMPRESSION_OPS / ops_per_second
	else:
		# Default to 1.0 if we couldn't measure properly
		ops_per_second = 0
		cpu_factor = 1.0

	logger.info(
		f'CPU benchmark results: {iterations} compressions in {elapsed:.2f}s, '
		f'{ops_per_second:.2f} ops/sec, normalization factor: {cpu_factor:.2f}'
	)

	return cpu_factor
