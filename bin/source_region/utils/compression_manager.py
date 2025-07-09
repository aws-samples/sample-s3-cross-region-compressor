"""
Compression Manager for adaptive ZSTD compression levels.

This module provides a facade for determining optimal compression levels
based on S3 bucket/prefix patterns and aggregated compression metrics.
"""

import logging
import os

from utils.compression_settings_repository import CompressionSettingsRepository
from utils.compression_optimizer import CompressionOptimizer
from utils.cost_benefit_calculator import CostBenefitCalculator

# Configure logging
logger = logging.getLogger(__name__)

# Constants
DEFAULT_COMPRESSION_LEVEL = 12


class CompressionManager:
	"""
	Manages optimal compression levels based on S3 bucket/prefix patterns.
	Implements the Singleton pattern to ensure a single instance across the application.
	"""

	# Class variable to hold the singleton instance
	_instance = None

	@classmethod
	def initialize(cls, dynamodb_client=None, cpu_factor=1.0):
		"""
		Initialize or re-initialize the singleton instance.

		Args:
		    dynamodb_client: Optional boto3 DynamoDB client
		    cpu_factor: CPU performance factor

		Returns:
		    The singleton instance
		"""
		cls._instance = cls(dynamodb_client, cpu_factor)
		logger.info(f'CompressionManager initialized with cpu_factor={cpu_factor}')
		return cls._instance

	@classmethod
	def get_instance(cls):
		"""
		Get the singleton instance. Initialize with defaults if not yet created.

		Returns:
		    The singleton instance
		"""
		if cls._instance is None:
			logger.warning('CompressionManager.get_instance() called before initialization')
			cls._instance = cls()
		return cls._instance

	def __init__(self, dynamodb_client=None, cpu_factor=1.0):
		"""
		Initialize the compression manager.

		Args:
		    dynamodb_client: Optional boto3 DynamoDB client
		    cpu_factor: CPU performance factor
		"""
		self.repository = CompressionSettingsRepository(
			dynamodb_client=dynamodb_client, table_name=os.environ.get('COMPRESSION_SETTINGS_TABLE')
		)
		self.optimizer = CompressionOptimizer(default_level=DEFAULT_COMPRESSION_LEVEL)
		self.calculator = CostBenefitCalculator(cpu_factor=cpu_factor)
		self.default_level = DEFAULT_COMPRESSION_LEVEL
		self.cpu_factor = cpu_factor

	def get_bucket_prefix_key(self, bucket: str, prefix: str) -> str:
		"""
		Generate the DynamoDB key from bucket and prefix.

		Args:
		    bucket: S3 bucket name
		    prefix: S3 prefix

		Returns:
		    Formatted key string
		"""
		norm_prefix = prefix.rstrip('/') + '/' if prefix else ''
		return f'{bucket}/{norm_prefix}'

	def get_compression_level(self, bucket: str, prefix: str, ddb_key_name: str = None) -> int:
		"""
		Get the optimal compression level using the simplified level selection strategy.

		Args:
		    bucket: S3 bucket name
		    prefix: S3 prefix
		    ddb_key_name: DDB key name (overrides bucket/prefix if provided)

		Returns:
		    Compression level to use
		"""
		# Use DDB Item key name if provided, otherwise build from bucket/prefix
		key = ddb_key_name if ddb_key_name else self.get_bucket_prefix_key(bucket, prefix)

		# Get settings from repository
		settings = self.repository.get_settings(key)

		if not settings or not settings.get('level_stats'):
			# No data yet - create entry and use default level
			self.repository.create_settings(key, self.default_level)
			logger.debug(f'No existing data found for {key}, using default level {self.default_level}')
			return self.default_level

		# Calculate avg_cpu_factor
		version = settings.get('version', 0)
		sum_cpu_factor = settings.get('sum_cpu_factor', 0)

		# Avoid division by zero
		avg_cpu_factor = sum_cpu_factor / version if version > 0 else 1.0

		# Get the single best level based on historical benefit
		level_stats = settings.get('level_stats', {})
		best_level = self.optimizer.get_best_level(level_stats)

		# Adjust level based on relative CPU performance
		chosen_level = self.optimizer.select_level_based_on_cpu(best_level, self.cpu_factor, avg_cpu_factor)

		# Occasionally explore adjacent levels with version-based decay
		final_level = self.optimizer.explore_adjacent_level(
			chosen_level,
			version,  # Pass version count for decay calculation
		)

		# Calculate relative performance for logging
		relative_performance = self.cpu_factor / avg_cpu_factor if avg_cpu_factor > 0 else 1.0

		logger.debug(
			f'Level selection for {key}: '
			f'best_level={best_level}, cpu_adjusted={chosen_level}, '
			f'with exploration={final_level}, '
			f'relative_performance={relative_performance:.2f}'
		)

		return final_level

	def update_compression_metrics(
		self,
		bucket: str = None,
		prefix: str = None,
		level: int = None,
		original_size: int = None,
		compressed_size: int = None,
		compression_time: float = None,
		processing_time: float = None,
		num_regions: int = 1,
		ddb_key_name: str = None,
		file_count: int = 1,
	) -> bool:
		"""
		Update compression metrics using the new aggregated approach.

		Args:
		    bucket: S3 bucket name
		    prefix: S3 prefix
		    level: Compression level used
		    original_size: Original size in bytes
		    compressed_size: Compressed size in bytes
		    compression_time: Time taken just for ZSTD compression in seconds
		    processing_time: Total processing time in seconds
		    num_regions: Number of target regions (defaults to 1)
		    ddb_key_name: DDB item key (overrides bucket/prefix)
		    file_count: Number of files in the TAR archive

		Returns:
		    True if successful, False otherwise
		"""
		# Use DDB Item key if provided, otherwise use bucket/prefix
		key = ddb_key_name if ddb_key_name else self.get_bucket_prefix_key(bucket, prefix)

		# For cost benefit calculations, use processing_time if available
		time_for_cost_benefit = processing_time if processing_time is not None else compression_time

		if time_for_cost_benefit is None:
			logger.warning('No timing information available for cost benefit calculations')
			return False

		# Calculate benefit metrics using raw processing time (no normalization)
		metrics = self.calculator.calculate_metrics(
			level, original_size, compressed_size, time_for_cost_benefit, num_regions, file_count
		)

		# Update repository with atomic updates for the aggregated level stats
		success = self.repository.update_metrics(key, level, metrics['benefit_score'], self.cpu_factor, file_count)

		if success:
			logger.debug(
				f'Updated metrics for {key}, level={level}, '
				f'benefit_score={metrics["benefit_score"]:.5f}, '
				f'file_count={file_count}'
			)

		return success
