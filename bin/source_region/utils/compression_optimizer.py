"""
Compression level optimizer.

This module provides algorithms for determining optimal compression levels
based on aggregated level statistics and CPU performance.
"""

import logging
import random
from typing import Dict

logger = logging.getLogger(__name__)


class CompressionOptimizer:
	"""Optimizer for compression levels based on aggregated statistics."""

	def __init__(self, default_level=12):
		"""
		Initialize the optimizer.

		Args:
		    default_level: Default compression level
		"""
		self.default_level = default_level
		# Min and max compression levels
		self.MIN_LEVEL = 1
		self.MAX_LEVEL = 22

	def get_best_level(self, level_stats: Dict) -> int:
		"""
		Get the single best compression level based on average benefit per object.

		Args:
		    level_stats: Dictionary of level statistics from DynamoDB
		                 {level: {'sum_benefit': float, 'trials': int, 'objects': int}, ...}

		Returns:
		    Best performing compression level
		"""
		if not level_stats:
			logger.info('No level statistics available, using default level')
			return self.default_level

		# Find best level based on average benefit per object
		best_level = self.default_level
		best_avg_benefit = 0

		for level_str, stats in level_stats.items():
			# Still require minimum number of trials for statistical significance
			if stats['trials'] >= 10:
				# Use objects instead of trials for average calculation
				objects = stats.get('objects', stats['trials'])  # Fallback to trials if objects not present

				# Calculate per-object average benefit
				avg_benefit = stats['sum_benefit'] / objects

				if avg_benefit > best_avg_benefit:
					best_avg_benefit = avg_benefit
					best_level = int(level_str)

		logger.debug(
			f'Best compression level selected: {best_level} with avg_benefit_per_object={best_avg_benefit:.5f}'
		)
		return best_level

	def select_level_based_on_cpu(self, best_level: int, cpu_factor: float, avg_cpu_factor: float) -> int:
		"""
		Adjust the best level based on relative CPU performance.

		Args:
		    best_level: Best performing level based on average benefit
		    cpu_factor: Current container's CPU factor
		    avg_cpu_factor: Average CPU factor across all containers

		Returns:
		    Selected compression level
		"""
		# Calculate relative performance
		# Remember: Lower CPU factor = higher performance, higher = slower
		if avg_cpu_factor <= 0:  # Avoid division by zero
			relative_performance = 1.0
		else:
			relative_performance = cpu_factor / avg_cpu_factor

		# Fast CPU - pick best level + 1 (capped at MAX_LEVEL)
		if relative_performance < 0.9:  # 10% better than average
			return min(best_level + 1, self.MAX_LEVEL)

		# Slow CPU - pick best level - 1 (minimum MIN_LEVEL)
		elif relative_performance > 1.1:  # 10% worse than average
			return max(best_level - 1, self.MIN_LEVEL)

		# Average CPU - use best level as-is
		else:
			return best_level

	def explore_adjacent_level(self, chosen_level: int, version_count: int = 0) -> int:
		"""
		Implement multi-tier exploration strategy with version-based decay.

		Exploration tiers with adaptive decay:
		- Base rate: 25% total exploration
		- Decays 2% per 1000 versions (max 50% decay)
		- Distribution: 60% for ±1, 25% for ±2, 15% for ±3
		- Minimum floor: 12.5% exploration (at 25000+ versions)

		Args:
		    chosen_level: The level selected as best
		    version_count: Total version count for decay calculation

		Returns:
		    Level to use (may be the same or a different level)
		"""
		# Calculate decay factor (2% per 1000 versions, max 50% decay)
		decay_per_1000 = 0.02  # 2% decay per 1000 versions
		max_decay = 0.5  # Maximum 50% decay

		# Calculate actual decay based on version count
		decay_factor = min(max_decay, (version_count / 1000) * decay_per_1000)

		# Base exploration rate is 25%
		base_exploration = 0.25

		# Apply decay to get actual exploration rate
		exploration_rate = base_exploration * (1 - decay_factor)

		# Calculate tier thresholds while maintaining proportions
		# Current proportions: 60% for tier 1, 25% for tier 2, 15% for tier 3
		tier1_threshold = exploration_rate * 0.6  # 60% of exploration budget
		tier2_threshold = tier1_threshold + exploration_rate * 0.25  # 25% of budget
		tier3_threshold = exploration_rate  # 100% of exploration budget

		# Debug logging for visibility (only when decay is significant)
		if version_count > 1000:
			logger.debug(
				f'Exploration rate: {exploration_rate:.4f} (decay: {decay_factor:.2f} based on {version_count} versions). '
				f'Thresholds: ±1:{tier1_threshold:.4f}, ±2:{tier2_threshold:.4f}, ±3:{tier3_threshold:.4f}'
			)

		rand = random.random()

		# Tier 1: ±1 level exploration (60% of exploration budget)
		if rand < tier1_threshold:
			# 50% chance to go up or down by 1
			if random.random() < 0.5:
				new_level = max(self.MIN_LEVEL, chosen_level - 1)
				logger.debug(f'Exploring level -1: {chosen_level} -> {new_level}')
				return new_level
			else:
				new_level = min(self.MAX_LEVEL, chosen_level + 1)
				logger.debug(f'Exploring level +1: {chosen_level} -> {new_level}')
				return new_level

		# Tier 2: ±2 level exploration (25% of exploration budget)
		elif rand < tier2_threshold:
			# 50% chance to go up or down by 2
			if random.random() < 0.5:
				new_level = max(self.MIN_LEVEL, chosen_level - 2)
				logger.debug(f'Exploring level -2: {chosen_level} -> {new_level}')
				return new_level
			else:
				new_level = min(self.MAX_LEVEL, chosen_level + 2)
				logger.debug(f'Exploring level +2: {chosen_level} -> {new_level}')
				return new_level

		# Tier 3: ±3 level exploration (15% of exploration budget)
		elif rand < tier3_threshold:
			# 50% chance to go up or down by 3
			if random.random() < 0.5:
				new_level = max(self.MIN_LEVEL, chosen_level - 3)
				logger.debug(f'Exploring level -3: {chosen_level} -> {new_level}')
				return new_level
			else:
				new_level = min(self.MAX_LEVEL, chosen_level + 3)
				logger.debug(f'Exploring level +3: {chosen_level} -> {new_level}')
				return new_level

		# Remainder of the time, use the chosen level (exploitation)
		return chosen_level
