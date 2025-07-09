#!/usr/bin/env python3
"""
Main CDK application for S3 Cross-Region Compressor

This module serves as the entry point for deploying the S3 Cross-Region Compressor solution.
It reads configuration files, validates the replication configuration to prevent loops,
and creates the necessary stacks in the specified AWS regions.

The application creates two main types of stacks:
1. BaselineRegionCompressorStack - A stack of baseline resources in each enabled region
2. S3ReplicationStack - A stack for S3 replication in each source region
"""

import os
import cdk_nag
import aws_cdk as cdk

from s3_cross_region_compressor.baseline_resources_stack import (
	BaselineRegionCompressorProps,
	BaselineRegionCompressorStack,
)
from s3_cross_region_compressor.s3_replication_stack import (
	S3ReplicationStack,
	S3ReplicationProps,
)
from s3_cross_region_compressor.utils.config_utils import (
	get_config,
	detect_replication_loops,
	group_configurations_by_source_region,
)

from s3_cross_region_compressor.utils.log_retention import LogGroupRetentionAspect
from s3_cross_region_compressor.utils.capacity_provider_aspect import HotfixCapacityProviderDependencies

LOG_RETENTION_DAYS = 30

settings = get_config('./configuration/settings.json')
replication_config = get_config('./configuration/replication_config.json')
stack_name = settings['stack_name']

if detect_replication_loops(replication_config):
	raise ValueError('Replication loop detected in the configuration! Please review the replication_config.json file')
print('No replication loops found.')
replication_config = replication_config['replication_configuration']

tags = settings['tags']

app = cdk.App()

for item in settings['enabled_regions']:
	region = item['region']
	env = cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=region)

	# Create the baseline stack with shared resources
	baseline_props = BaselineRegionCompressorProps(
		region_config=item,
		tags=tags,
		stack_name=stack_name,
		replication_config=replication_config,
		notification_emails=settings['notification_emails'],
		# Auto-scaling configuration for ECS tasks based on SQS queue depth
		min_capacity=0,  # Scale to zero when no messages
		max_capacity=20,  # Can scale up to 20 tasks
		scaling_target_backlog_per_task=60,
		scale_out_cooldown=60,  # 1 minute cooldown before scaling out again
		scale_in_cooldown=90,  # 1:30 minutes cooldown before scaling in
	)

	baseline_stack = BaselineRegionCompressorStack(
		app,
		f'{stack_name}-{region}-BaselineStack',
		props=baseline_props,
		env=env,
	)

# Group replication configurations by source region
grouped_configs = group_configurations_by_source_region(replication_config)

# Implement S3 replication between source and target regions
for region, value in grouped_configs.items():
	env = cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=region)

	s3_replication_props = S3ReplicationProps(
		stack_name=stack_name,
		replication_config=value,
	)

	s3_replication_stack = S3ReplicationStack(
		app,
		f'{stack_name}-{region}-S3Replication',
		props=s3_replication_props,
		env=env,
	).add_dependency(baseline_stack)

# Apply log retention policy (ONE_MONTH) to all CloudWatch Log Groups
cdk.Aspects.of(app).add(LogGroupRetentionAspect(LOG_RETENTION_DAYS))

# Apply workaround for ECS capacity provider dependency issue
cdk.Aspects.of(app).add(HotfixCapacityProviderDependencies())

# Adding cdk-nag checks
cdk.Aspects.of(app).add(cdk_nag.AwsSolutionsChecks())

app.synth()
