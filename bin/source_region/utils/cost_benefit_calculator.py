"""
Cost-benefit calculator for compression operations.

This module provides utilities for calculating the cost-benefit tradeoffs
of different compression levels based on compute costs vs. transfer savings.
"""

import logging
import os
from typing import Dict

logger = logging.getLogger(__name__)


class CostBenefitCalculator:
	"""Calculator for compression cost-benefit analysis."""

	def __init__(self, cpu_factor=1.0):
		"""
		Initialize the calculator with cost factors.

		Args:
		    cpu_factor: CPU performance factor (stored but not used for normalization)
		              Will be used for level selection instead
		"""
		self.TRANSFER_COST_FACTOR = float(os.environ.get('DATA_TRANSFER_COST', 0.02))
		self.COMPUTE_COST_FACTOR = float(os.environ.get('FARGATE_COST_PER_MINUTE', 0.000395))
		self.CPU_FACTOR = cpu_factor

	def calculate_metrics(
		self,
		level: int,
		original_size: int,
		compressed_size: int,
		processing_time: float,
		num_regions: int,
		file_count: int,
	) -> Dict:
		"""
		Calculate cost-benefit metrics using raw processing times.

		Args:
		    level: Compression level used
		    original_size: Original size in bytes
		    compressed_size: Compressed size in bytes
		    processing_time: Total time taken for processing in seconds (downloading, compressing, preparing)
		    num_regions: Number of target regions
		    file_count: Number of files compressed

		Returns:
		    Dictionary of metric values
		"""
		# Calculate bytes saved
		bytes_saved = max(0, original_size - compressed_size)

		# Calculate actual dollar costs/savings using raw processing time
		compute_cost = self._calculate_compute_cost(processing_time)
		transfer_savings = self._calculate_transfer_savings(bytes_saved, num_regions)

		# Net benefit (transfer savings minus compute cost)
		net_benefit = transfer_savings - compute_cost
		benefit_score = net_benefit

		# Return only the essential metrics needed (simplified)
		return {'level': level, 'benefit_score': benefit_score, 'file_count': file_count}

	def _calculate_compute_cost(self, processing_time: float) -> float:
		"""Calculate the cost of computation time in dollars."""
		return (
			processing_time * 1.025 * self.COMPUTE_COST_FACTOR / 60
		)  # Convert seconds to minutes and adding 2.5% Compute Overhead

	def _calculate_transfer_savings(self, bytes_saved: int, num_regions: int) -> float:
		"""Calculate the savings from reduced transfer costs in dollars."""
		return bytes_saved * self.TRANSFER_COST_FACTOR / (1024 * 1024 * 1024) * num_regions  # Convert bytes to GB
