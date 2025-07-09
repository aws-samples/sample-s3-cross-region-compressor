"""
CloudWatch Dashboard creation for S3 Cross-Region Compressor.

This module provides functions for creating a consolidated CloudWatch Dashboard
that displays metrics for all source prefixes in a tabular format.

Key features:
- One consolidated dashboard per source region
- Tabular view of metrics by source bucket and prefix
- Overview of regional performance metrics
- Time series analysis of compression performance
"""

from constructs import Construct
from aws_cdk import (
	aws_cloudwatch as cw,
	Stack,
)


def create_compression_dashboard(scope: Construct, stack_name: str) -> cw.Dashboard:
	"""
	Create a consolidated CloudWatch Dashboard for visualizing compression metrics.

	Creates a single dashboard per region that displays metrics for all source buckets
	and prefixes in a tabular format, along with overview metrics and time series graphs.

	Args:
	    scope: The CDK construct scope
	    stack_name: Name of the stack for resource naming

	Returns:
	    A CloudWatch Dashboard displaying compression metrics
	"""
	# Get current region from the stack
	region = Stack.of(scope).region

	dashboard = cw.Dashboard(
		scope,
		f'{stack_name}-Compression-Dashboard',
		dashboard_name=f'{stack_name}-{region}-Compression-Metrics',
		variables=[
			cw.DashboardVariable(
				id='SourceBucket',
				type=cw.VariableType.PROPERTY,
				label='SourceBucket',
				input_type=cw.VariableInputType.SELECT,
				visible=True,
				value='SourceBucket',
				values=cw.Values.from_search(
					expression='{' + stack_name + ',SourceBucket,SourcePrefix,TargetRegion}',
					populate_from='SourceBucket',
				),
			),
			cw.DashboardVariable(
				id='SourcePrefix',
				type=cw.VariableType.PROPERTY,
				label='SourcePrefix',
				input_type=cw.VariableInputType.SELECT,
				visible=True,
				value='SourcePrefix',
				values=cw.Values.from_search(
					expression='{' + stack_name + ',SourceBucket,SourcePrefix,TargetRegion}',
					populate_from='SourcePrefix',
				),
			),
			cw.DashboardVariable(
				id='TargetRegion',
				type=cw.VariableType.PROPERTY,
				label='TargetRegion',
				input_type=cw.VariableInputType.SELECT,
				visible=True,
				value='TargetRegion',
				values=cw.Values.from_search(
					expression='{' + stack_name + ',SourceBucket,SourcePrefix,TargetRegion}',
					populate_from='TargetRegion',
				),
			),
		],
	)
	# Add header with title
	dashboard.add_widgets(cw.TextWidget(markdown='# S3 Cross-Region Compression Dashboard', width=24, height=1))

	# Add widgets in logical groups

	_add_overview_section(
		dashboard,
		stack_name,
		filter_by_variables=False,
		section_title='compression performance metrics across all sources',
	)
	_add_overview_section(
		dashboard,
		stack_name,
		filter_by_variables=True,
		section_title='compression performance metrics per source location',
	)

	return dashboard


def _add_overview_section(
	dashboard: cw.Dashboard, stack_name: str, filter_by_variables: bool, section_title: str
) -> None:
	"""
	Add overview metrics section to the dashboard.

	Args:
	    dashboard: The CloudWatch Dashboard to add widgets to
	    stack_name: Name of the stack for metric namespace
	    filter_by_variables: If True, filter metrics by dashboard variables
	    section_title: Title to display in the section header
	"""
	# Variable filter string to add to search expressions if needed
	var_filter = ''
	var_filter_no_region = ''
	if filter_by_variables:
		var_filter = ' SourceBucket=SourceBucket SourcePrefix=SourcePrefix TargetRegion=TargetRegion'
		# For metrics that don't have TargetRegion dimension, we need to remove that part from var_filter
		var_filter_no_region = var_filter.replace(' TargetRegion=TargetRegion', '')

	# For metrics that have dimensions, we need to use a search expression to aggregate across all dimension values
	# Original Size - Using search expression since it uses SourceBucket, SourcePrefix, TargetRegion dimensions
	original_size_sum = cw.MathExpression(
		expression="SUM(SEARCH('{"
		+ stack_name
		+ ',SourceBucket,SourcePrefix,TargetRegion}'
		+ var_filter
		+ " MetricName=\"OriginalSize\"', 'Sum'))",
		label='Total Original Size',
	)

	# Compressed Size - Using search expression since it uses SourceBucket, SourcePrefix, TargetRegion dimensions
	compressed_size_sum = cw.MathExpression(
		expression="SUM(SEARCH('{"
		+ stack_name
		+ ',SourceBucket,SourcePrefix,TargetRegion}'
		+ var_filter
		+ " MetricName=\"CompressedSize\"', 'Sum'))",
		label='Total Compressed Size',
	)

	# Bytes Saved - Using search expression since it uses SourceBucket, SourcePrefix, TargetRegion dimensions
	bytes_saved_sum = cw.MathExpression(
		expression="SUM(SEARCH('{"
		+ stack_name
		+ ',SourceBucket,SourcePrefix,TargetRegion}'
		+ var_filter
		+ " MetricName=\"BytesSaved\"', 'Sum'))",
		label='Total Bytes Saved',
	)

	# Compression Ratio - Using search expression since it uses SourceBucket, SourcePrefix dimensions
	compression_ratio_avg = cw.MathExpression(
		expression="AVG(REMOVE_EMPTY(SEARCH('{"
		+ stack_name
		+ ',SourceBucket,SourcePrefix}'
		+ var_filter_no_region
		+ " MetricName=\"CompressionRatio\"', 'Average')))",
		label='Average Compression Ratio',
	)

	# Transfer Efficiency - Using search expression since it uses SourceBucket, SourcePrefix dimensions
	transfer_efficiency_avg = cw.MathExpression(
		expression="AVG(REMOVE_EMPTY(SEARCH('{"
		+ stack_name
		+ ',SourceBucket,SourcePrefix}'
		+ var_filter_no_region
		+ " MetricName=\"TransferEfficiency\"', 'Average')))",
		label='Average Transfer Efficiency (%)',
	)

	# Compression Throughput - Using search expression since it uses SourceBucket, SourcePrefix dimensions
	compression_throughput_avg = cw.MathExpression(
		expression="AVG(REMOVE_EMPTY(SEARCH('{"
		+ stack_name
		+ ',SourceBucket,SourcePrefix}'
		+ var_filter_no_region
		+ " MetricName=\"CompressionThroughput\"', 'Average')))",
		label='Average Compression Throughput (MB/s)',
	)

	compression_throughput_sum = cw.MathExpression(
		expression="SUM(SEARCH('{"
		+ stack_name
		+ ',SourceBucket,SourcePrefix}'
		+ var_filter_no_region
		+ " MetricName=\"CompressionThroughput\"', 'Sum')) / 1024",
		label='Aggregated Compression Throughput (GB/s)',
	)

	# Add header with title
	dashboard.add_widgets(cw.TextWidget(markdown=f'## Overview of {section_title}', width=24, height=1))

	# Add summary metrics as single value widgets
	dashboard.add_widgets(
		cw.SingleValueWidget(
			title='Total Original Data Size',
			metrics=[original_size_sum],
			width=8,
			height=4,
			set_period_to_time_range=True,
		),
		cw.SingleValueWidget(
			title='Total Compressed Data',
			metrics=[compressed_size_sum],
			width=8,
			height=4,
			set_period_to_time_range=True,
		),
		cw.SingleValueWidget(
			title='Total Bytes Saved', metrics=[bytes_saved_sum], width=8, height=4, set_period_to_time_range=True
		),
	)
	dashboard.add_widgets(
		cw.SingleValueWidget(
			title='Average Compression Ratio',
			metrics=[compression_ratio_avg],
			width=8,
			height=3,
			set_period_to_time_range=True,
		),
		cw.SingleValueWidget(
			title='Average Transfer Efficiency (%)',
			metrics=[transfer_efficiency_avg],
			width=8,
			height=3,
			set_period_to_time_range=True,
		),
		cw.SingleValueWidget(
			title='Average Compression Throughput (MB/s)',
			metrics=[compression_throughput_avg],
			width=8,
			height=3,
			set_period_to_time_range=True,
		),
	)

	# Add header with title
	dashboard.add_widgets(cw.TextWidget(markdown=f'## Detail {section_title}', width=24, height=1))

	# Keep the graph widgets for detailed time series visualization
	dashboard.add_widgets(
		cw.GraphWidget(
			title='Compression Throughput Over Time',
			left=[compression_throughput_avg],
			right=[compression_throughput_sum],
			width=24,
			height=6,
			left_y_axis=cw.YAxisProps(label='Average Throughput per task (MB/s)', show_units=True),
			right_y_axis=cw.YAxisProps(label='Aggregated Throughput (GB/s)', show_units=True),
		)
	)
