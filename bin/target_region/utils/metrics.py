"""
Metrics Utilities for Target Region Container

This module provides utilities for tracking and reporting metrics:
- Decompression ratio calculation
- Processing time tracking
- CloudWatch metrics reporting using Embedded Metric Format (EMF)
"""

import functools
import logging
import os
import time
from typing import Callable

from aws_embedded_metrics import metric_scope, MetricsLogger
from aws_embedded_metrics.config import get_config

# Configure logging
logger = logging.getLogger(__name__)

# Configure EMF
get_config().namespace = os.environ.get('STACK_NAME')


def calculate_decompression_ratio(compressed_size: int, decompressed_size: int) -> float:
	"""
	Calculate the decompression ratio.

	Args:
	    compressed_size: Compressed size in bytes
	    decompressed_size: Decompressed size in bytes

	Returns:
	    Decompression ratio (decompressed_size / compressed_size)
	"""
	if compressed_size <= 0:
		return 0.0

	return decompressed_size / compressed_size


@metric_scope
def report_decompression_metrics(
	target_bucket: str, compressed_size: int, decompressed_size: int, metrics: MetricsLogger = None
) -> bool:
	"""
	Report decompression metrics to CloudWatch using EMF.

	Args:
	    target_bucket: Target bucket name
	    compressed_size: Compressed size in bytes
	    decompressed_size: Decompressed size in bytes
	    metrics: MetricsLogger automatically injected by the decorator

	Returns:
	    True if all metrics were reported successfully, False otherwise
	"""
	try:
		# Calculate metrics
		decompression_ratio = calculate_decompression_ratio(compressed_size, decompressed_size)

		# Set dimensions and report metrics
		metrics.set_dimensions({'TargetBucket': target_bucket})

		# Put all metrics at once
		metrics.put_metric('DecompressionRatio', decompression_ratio)
		metrics.put_metric('CompressedSize', compressed_size, 'Bytes')
		metrics.put_metric('DecompressedSize', decompressed_size, 'Bytes')

		logger.debug(
			f'Reported decompression metrics for bucket {target_bucket}: ratio={decompression_ratio:.2f}, compressed={compressed_size}, decompressed={decompressed_size}'
		)
		return True
	except Exception as e:
		logger.error(f'Error reporting decompression metrics: {e}')
		return False


def track_processing_time(func: Callable) -> Callable:
	"""
	Decorator to track processing time for a function and report it using EMF.

	Args:
	    func: Function to track

	Returns:
	    Wrapped function
	"""

	@functools.wraps(func)
	def wrapper(*args, **kwargs):
		@metric_scope
		def _metric_wrapper(metrics: MetricsLogger):
			start_time = time.time()
			result = func(*args, **kwargs)
			end_time = time.time()

			processing_time = end_time - start_time

			# Set dimensions and record processing time
			metrics.set_dimensions({'Function': func.__name__})
			metrics.put_metric('ProcessingTime', processing_time, 'Seconds')

			logger.debug(f'Function {func.__name__} execution time: {processing_time:.2f} seconds')

			return result

		return _metric_wrapper()

	return wrapper
