"""
Unit tests for the metrics module.
"""

from unittest.mock import patch, MagicMock

# Import the module under test
from bin.source_region.utils.metrics import (
	report_compression_metrics,
)


class TestMetricsReporting:
	"""Tests for compression metrics reporting."""

	def test_report_compression_metrics(self):
		"""Test reporting compression metrics to CloudWatch."""
		# Given: Compression metrics data
		source_bucket = 'test-bucket'
		source_prefix = 'test/prefix'
		original_size = 1000
		compressed_size = 500
		processing_time = 2.5
		targets = [
			{'region': 'us-west-2', 'bucket': 'target-bucket-1'},
			{'region': 'eu-west-1', 'bucket': 'target-bucket-2'},
		]

		# When: We report metrics with multiple targets
		with patch('bin.source_region.utils.metrics.report_region_metrics') as mock_report_region:
			# Call the function
			result = report_compression_metrics(
				source_bucket,
				source_prefix,
				original_size,
				compressed_size,
				processing_time=processing_time,
				targets=targets,
			)

			# Then: The function should succeed
			assert result is True

			# Verify region metrics were called for each target
			assert mock_report_region.call_count == 2, 'Expected report_region_metrics to be called for each target'

	def test_report_compression_metrics_empty_prefix(self):
		"""Test reporting compression metrics with an empty prefix."""
		# Given: Compression metrics data with empty prefix
		source_bucket = 'test-bucket'
		source_prefix = ''
		original_size = 1000
		compressed_size = 500
		processing_time = 2.5
		targets = [{'region': 'us-west-2', 'bucket': 'target-bucket'}]

		# When: We report metrics with empty prefix
		with patch('bin.source_region.utils.metrics.report_region_metrics') as mock_report_region:
			# Call the function
			result = report_compression_metrics(
				source_bucket,
				source_prefix,
				original_size,
				compressed_size,
				processing_time=processing_time,
				targets=targets,
			)

			# Then: The function should succeed
			assert result is True

			# Verify report_region_metrics is called with empty prefix
			mock_report_region.assert_called_once()

			# Check the args to ensure empty prefix is passed correctly
			call_args = mock_report_region.call_args
			args, kwargs = call_args
			assert kwargs.get('source_prefix') == '', 'Empty source_prefix should be preserved'

	def test_report_compression_metrics_no_targets(self):
		"""Test reporting compression metrics with no targets."""
		# Given: Compression metrics data with no targets
		source_bucket = 'test-bucket'
		source_prefix = 'test/prefix'
		original_size = 1000
		compressed_size = 500
		processing_time = 2.5
		targets = []

		# When: We report metrics with no targets
		with patch('bin.source_region.utils.metrics.report_region_metrics') as mock_report_region:
			# Call the function
			result = report_compression_metrics(
				source_bucket,
				source_prefix,
				original_size,
				compressed_size,
				processing_time=processing_time,
				targets=targets,
			)

			# Then: The function should succeed
			assert result is True

			# Verify the region-specific metrics function wasn't called
			mock_report_region.assert_not_called()

	def test_report_compression_metrics_with_monitored_prefix(self):
		"""Test reporting compression metrics with a monitored_prefix that overrides source_prefix."""
		# Given: Compression metrics data with both source_prefix and monitored_prefix
		source_bucket = 'test-bucket'
		source_prefix = 'test/source/prefix'
		monitored_prefix = 'test/monitored/prefix'
		original_size = 1000
		compressed_size = 500
		processing_time = 2.5
		targets = [{'region': 'us-west-2', 'bucket': 'target-bucket'}]

		# When: We report metrics with both prefixes
		with patch('bin.source_region.utils.metrics.report_region_metrics') as mock_report_region:
			# Call the function
			result = report_compression_metrics(
				source_bucket,
				source_prefix,
				original_size,
				compressed_size,
				processing_time=processing_time,
				targets=targets,
				monitored_prefix=monitored_prefix,
			)

			# Then: The function should succeed
			assert result is True

			# Verify report_region_metrics is called with the monitored_prefix
			mock_report_region.assert_called_once()

			# Check that monitored_prefix was passed correctly to report_region_metrics
			call_args = mock_report_region.call_args
			args, kwargs = call_args
			assert kwargs.get('monitored_prefix') == monitored_prefix, (
				'monitored_prefix should be passed to report_region_metrics'
			)

	def test_report_compression_metrics_cloudwatch_error(self):
		"""Test handling CloudWatch errors when reporting metrics."""
		# Given: Compression metrics data
		source_bucket = 'test-bucket'
		source_prefix = 'test/prefix'
		original_size = 1000
		compressed_size = 500
		processing_time = 2.5
		targets = [{'region': 'us-west-2', 'bucket': 'target-bucket'}]

		# When: CloudWatch metrics logging raises an exception
		with patch('aws_embedded_metrics.metric_scope') as mock_metrics_scope:
			# Make the decorator raise an exception
			def side_effect(func):
				def wrapper(*args, **kwargs):
					mock_metrics = MagicMock()
					mock_metrics.put_metric.side_effect = Exception('Test metric error')
					return func(mock_metrics, *args, **kwargs)

				return wrapper

			mock_metrics_scope.return_value = side_effect

			# Call the function - should handle errors gracefully
			# This test passes if no exception is raised
			report_compression_metrics(
				source_bucket, source_prefix, original_size, compressed_size, processing_time, targets
			)
