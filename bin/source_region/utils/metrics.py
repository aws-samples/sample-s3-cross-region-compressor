"""
Metrics Utilities for Source Region Container

This module provides utilities for tracking and reporting metrics:
- Compression ratio calculation
- Bytes saved tracking
- Compression throughput calculation
- CloudWatch metrics reporting using Embedded Metric Format (EMF)
"""

import logging
import os
from typing import Optional, List, Dict

from aws_embedded_metrics import metric_scope, MetricsLogger
from aws_embedded_metrics.config import get_config

# Configure logging
logger = logging.getLogger(__name__)

# Configure EMF
get_config().namespace = os.environ.get('STACK_NAME')


def calculate_compression_ratio(original_size: int, compressed_size: int) -> float:
	"""
	Calculate the compression ratio.

	Args:
	    original_size: Original size in bytes
	    compressed_size: Compressed size in bytes

	Returns:
	    Compression ratio (original_size / compressed_size)
	"""
	if compressed_size <= 0:
		return 0.0

	return original_size / compressed_size


def calculate_bytes_saved(original_size: int, compressed_size: int) -> int:
	"""
	Calculate the number of bytes saved by compression.

	Args:
	    original_size: Original size in bytes
	    compressed_size: Compressed size in bytes

	Returns:
	    Bytes saved (original_size - compressed_size)
	"""
	return max(0, original_size - compressed_size)


def calculate_transfer_efficiency(original_size: int, bytes_saved: int) -> float:
	"""
	Calculate the transfer efficiency as a percentage.

	Args:
	    original_size: Original size in bytes
	    bytes_saved: Bytes saved

	Returns:
	    Transfer efficiency as a percentage (bytes_saved / original_size * 100)
	"""
	if original_size <= 0:
		return 0.0

	return (bytes_saved / original_size) * 100


def calculate_compression_throughput(original_size: int, compression_time: float) -> float:
	"""
	Calculate compression throughput in MB/s.

	Args:
	    original_size: Original size in bytes
	    compression_time: Time taken for ZSTD compression operation in seconds

	Returns:
	    Throughput in MB/s
	"""
	if compression_time <= 0:
		return 0.0

	# Convert bytes to MB and divide by seconds
	return (original_size / 1024 / 1024) / compression_time


@metric_scope
def report_compression_metrics(
	source_bucket: str,
	source_prefix: Optional[str],
	original_size: int,
	compressed_size: int,
	compression_time: Optional[float] = None,
	processing_time: Optional[float] = None,
	targets: Optional[List[Dict]] = None,
	monitored_prefix: Optional[str] = None,
	metrics: MetricsLogger = None,  # Will be injected by the decorator
) -> bool:
	"""
	Report compression metrics to CloudWatch using EMF according to two configurations:

	Config 1:
	Dimensions: Source Bucket, Source Prefix (if exists), Target Region
	Metrics: OriginalSize, CompressedSize, BytesSaved

	Config 2:
	Dimensions: Source Bucket, Source Prefix (if exists)
	Metrics: CompressionRatio, TransferEfficiency, CompressionThroughput

	Args:
	    metrics: MetricsLogger injected by the decorator
	    source_bucket: Source bucket name
	    source_prefix: Source prefix (individual object's prefix)
	    original_size: Original size in bytes
	    compressed_size: Compressed size in bytes
	    compression_time: Time taken just for ZSTD compression in seconds (used for throughput metrics)
	    processing_time: Total time from after SQS to completion (used for cost benefit calculations)
	    targets: List of target region dictionaries
	    monitored_prefix: The root prefix being monitored from environment variable (takes precedence over source_prefix for dimensions)

	Returns:
	    True if all metrics were reported successfully, False otherwise
	"""
	try:
		# Calculate metrics
		compression_ratio = calculate_compression_ratio(original_size, compressed_size)
		bytes_saved = calculate_bytes_saved(original_size, compressed_size)
		transfer_efficiency = calculate_transfer_efficiency(original_size, bytes_saved)

		# Config 2: Report metrics with source bucket and prefix dimensions
		# If monitored_prefix is provided, use it instead of source_prefix
		prefix_to_use = monitored_prefix if monitored_prefix is not None else source_prefix
		config2_dimensions = {'SourceBucket': source_bucket, 'SourcePrefix': prefix_to_use if prefix_to_use else 'root'}

		# Set dimensions for Config 2
		metrics.set_dimensions(config2_dimensions)

		# Report Config 2 metrics (CompressionRatio, TransferEfficiency, CompressionThroughput)
		metrics.put_metric('CompressionRatio', compression_ratio)
		metrics.put_metric('TransferEfficiency', transfer_efficiency, 'Percent')

		# For throughput calculation, prefer compression_time if available, otherwise fall back to processing_time
		if compression_time is not None and compression_time > 0:
			throughput = calculate_compression_throughput(original_size, compression_time)
			metrics.put_metric('CompressionThroughput', throughput, 'Megabytes/Second')
		elif processing_time is not None and processing_time > 0:
			# Fall back to processing_time if that's all we have
			logger.debug('Using processing_time for throughput calculation as compression_time is not available')
			throughput = calculate_compression_throughput(original_size, processing_time)
			metrics.put_metric('CompressionThroughput', throughput, 'Megabytes/Second')

		# Config 1: If we have target information, report metrics per target region
		if targets:
			logger.debug(f'Reporting metrics for {len(targets)} target regions')

			for target in targets:
				if 'region' in target:
					target_region = target['region']

					# Call a separate metric_scope function for each region
					# to ensure each gets its own EMF log line
					report_region_metrics(
						source_bucket=source_bucket,
						source_prefix=source_prefix,
						target_region=target_region,
						original_size=original_size,
						compressed_size=compressed_size,
						bytes_saved=bytes_saved,
						monitored_prefix=monitored_prefix,
					)
				else:
					logger.warning('Target missing region information')

		return True
	except Exception as e:
		logger.error(f'Error reporting compression metrics: {e}')
		return False


@metric_scope
def report_region_metrics(
	source_bucket: str,
	source_prefix: Optional[str],
	target_region: str,
	original_size: int,
	compressed_size: int,
	bytes_saved: int,
	monitored_prefix: Optional[str] = None,
	metrics: MetricsLogger = None,  # Will be injected by the decorator
) -> None:
	"""
	Report Config 1 metrics for a specific target region using a separate metrics context.

	Config 1:
	Dimensions: Source Bucket, Source Prefix (if exists), Target Region
	Metrics: OriginalSize, CompressedSize, BytesSaved

	This function is decorated with @metric_scope to ensure each region
	gets its own independent metrics context that's flushed separately.

	Args:
	    metrics: MetricsLogger injected by the decorator
	    source_bucket: Source bucket name
	    source_prefix: Source prefix
	    target_region: Target region name
	    original_size: Original size in bytes
	    compressed_size: Compressed size in bytes
	    bytes_saved: Bytes saved value
	"""
	# Create dimensions for Config 1
	# If monitored_prefix is provided, use it instead of source_prefix
	prefix_to_use = monitored_prefix if monitored_prefix is not None else source_prefix
	config1_dimensions = {
		'SourceBucket': source_bucket,
		'TargetRegion': target_region,
		'SourcePrefix': prefix_to_use if prefix_to_use else 'root',
	}

	# Set dimensions for Config 1
	metrics.set_dimensions(config1_dimensions)

	# Report Config 1 metrics (OriginalSize, CompressedSize, BytesSaved)
	metrics.put_metric('OriginalSize', original_size, 'Bytes')
	metrics.put_metric('CompressedSize', compressed_size, 'Bytes')
	metrics.put_metric('BytesSaved', bytes_saved, 'Bytes')
