#!/usr/bin/env python3
"""
Source Region Container for S3 Cross-Region Compressor

This application processes SQS messages for S3 object creation events,
compresses the objects using zstd, and uploads them to a staging S3 bucket
for cross-region replication.

Environment Variables:
    SQS_QUEUE_URL: URL of the SQS queue to poll for messages
    AWS_DEFAULT_REGION: AWS region
    OUTBOUND_BUCKET: Name of the outbound S3 bucket
    MONITORED_PREFIX: The root prefix being monitored (can be empty for bucket root)
"""

import logging
import os
import signal
import sys
import time
import uuid
from pyzstd import zstd_version
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Dict, Optional, Tuple
from utils.cpu_benchmark import run_cpu_benchmark

from pythonjsonlogger import jsonlogger

# Import utility modules
from utils.aws_utils import (
	delete_sqs_messages_batch,
	extract_s3_event_info,
	get_s3_object,
	get_s3_object_metadata,
	get_sqs_messages,
	get_target_info_from_dynamodb,
	is_s3_test_event,
	upload_to_s3,
)
from utils.compression_manager import CompressionManager
from utils.compression import (
	cleanup_temp_directory,
	compress_objects,
	create_temp_directory,
)
from utils.manifest import create_object_manifest
from utils.metrics import report_compression_metrics

log_level_str = os.environ.get('LOG_LEVEL', 'INFO')
log_level_map = {
	'DEBUG': logging.DEBUG,
	'INFO': logging.INFO,
	'WARNING': logging.WARNING,
	'ERROR': logging.ERROR,
	'CRITICAL': logging.CRITICAL,
}
log_level = log_level_map.get(log_level_str.upper(), logging.INFO)

# Configure logging
logger = logging.getLogger()
logger.setLevel(log_level)

# Add JSON formatter for structured logging
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(name)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

# Constants
MAX_MESSAGES_PER_BATCH = 10
MAX_WORKERS = max(1, os.cpu_count() or 1)
POLL_INTERVAL = 20  # seconds

# Global variables
running = True


def get_env_var(name: str, required: bool = True) -> Optional[str]:
	"""
	Get an environment variable.

	Args:
	    name: Name of the environment variable
	    required: Whether the variable is required

	Returns:
	    Value of the environment variable or None if not found and not required
	"""
	value = os.environ.get(name)

	if required and not value:
		logger.error(f'Required environment variable {name} not set')
		sys.exit(1)

	return value


def signal_handler(sig, frame):
	"""
	Handle termination signals.

	Args:
	    sig: Signal number
	    frame: Current stack frame
	"""
	global running
	logger.info(f'Received signal {sig}, shutting down gracefully...')
	running = False


def process_s3_object(s3_object: Dict, temp_dir: str, monitored_prefix: str = '') -> Tuple[Dict, str]:
	"""
	Process an S3 object: download it and get its metadata.

	Args:
	    s3_object: Dictionary with bucket and key information
	    temp_dir: Temporary directory for downloaded files
	    monitored_prefix: Root prefix being monitored

	Returns:
	    Tuple of (object metadata, local file path)
	"""
	bucket = s3_object['bucket']
	key = s3_object['key']

	def _process_s3_object() -> Tuple[Dict, str]:
		# Generate a unique filename
		filename = f'{uuid.uuid4().hex}_{os.path.basename(key)}'
		local_path = os.path.join(temp_dir, filename)

		# Download the object
		if not get_s3_object(bucket, key, local_path):
			logger.error(f'Failed to download object {bucket}/{key}')
			return {}, ''

		# Get object metadata
		metadata = get_s3_object_metadata(bucket, key)
		if not metadata:
			logger.error(f'Failed to get metadata for object {bucket}/{key}')
			return {}, ''

		# Add full key to metadata
		metadata['full_key'] = key

		# Calculate relative path from monitored prefix
		if monitored_prefix and key.startswith(monitored_prefix):
			# Remove the monitored prefix and any leading slashes
			relative_key = key[len(monitored_prefix) :]
			relative_key = relative_key.lstrip('/')
		else:
			relative_key = key

		metadata['relative_key'] = relative_key

		return metadata, local_path

	return _process_s3_object()


def process_message_batch(queue_url: str, outbound_bucket: str, stack_name: str, monitored_prefix: str = '') -> int:
	"""
	Process a batch of SQS messages.

	Args:
	    queue_url: URL of the SQS queue
	    outbound_bucket: Name of the outbound S3 bucket
	    stack_name: Stack name for DynamoDB parameter lookup

	Returns:
	    Number of successfully processed messages
	"""
	# Retrieve messages from SQS
	messages = get_sqs_messages(
		queue_url=queue_url,
		max_messages=MAX_MESSAGES_PER_BATCH,
	)

	if not messages:
		return 0

	logger.info(f'Retrieved {len(messages)} messages from SQS')

	# Process test events first
	test_event_receipt_handles = []
	regular_messages = []

	for message in messages:
		if is_s3_test_event(message):
			logger.debug('Detected S3 test event - will delete without processing')
			test_event_receipt_handles.append(message['ReceiptHandle'])
		else:
			regular_messages.append(message)

	# Delete test events immediately
	if test_event_receipt_handles:
		logger.debug(f'Deleting {len(test_event_receipt_handles)} S3 test event messages')
		delete_sqs_messages_batch(queue_url, test_event_receipt_handles)
		logger.debug(f'Successfully deleted {len(test_event_receipt_handles)} test event messages')

	# If all messages were test events, return now
	if not regular_messages:
		logger.debug('All messages were test events, no further processing needed')
		return len(messages)

	# Continue processing with regular messages only
	messages = regular_messages
	logger.debug(f'Processing {len(messages)} non-test event messages')

	# Create temporary directory
	temp_dir = create_temp_directory()

	# Track start time for processing metrics - AFTER getting SQS messages
	start_processing_time = time.time()

	try:
		# Process each message
		receipt_handles = []
		s3_objects = []

		for message in messages:
			receipt_handles.append(message['ReceiptHandle'])
			s3_objects.extend(extract_s3_event_info(message))

		if not s3_objects:
			logger.warning('No S3 objects found in messages')

			return len(messages)

		# Process S3 objects
		objects_metadata = []
		object_paths = []

		# Use ThreadPoolExecutor for parallel processing
		with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
			process_func = partial(process_s3_object, temp_dir=temp_dir, monitored_prefix=monitored_prefix)
			results = list(executor.map(process_func, s3_objects))

		# Collect results
		for metadata, local_path in results:
			if metadata and local_path:
				objects_metadata.append(metadata)
				object_paths.append(
					{
						'local_path': local_path,
						'object_name': metadata['object_name'],
						'relative_key': metadata['relative_key'],
					}
				)

		if not objects_metadata:
			logger.warning('No valid objects to process')

			return len(messages)

		# Get source information (all objects in a batch come from the same source bucket via dedicated queue)
		source_bucket = objects_metadata[0]['source_bucket']
		source_prefix = objects_metadata[0]['source_prefix']

		# Get target information from DynamoDB
		# Use the monitored_prefix from environment variable instead of source_prefix
		# This reduces multiple DynamoDB lookups with different prefix components
		ddb_key_name, targets = get_target_info_from_dynamodb(stack_name, source_bucket, monitored_prefix)
		if not targets:
			logger.error(f'No target information found for {source_bucket}/{monitored_prefix or ""}')

			return len(messages)

		manifest_path = os.path.join(temp_dir, 'manifest.json')
		if not create_object_manifest(objects_metadata, targets, manifest_path):
			logger.error('Failed to create manifest file')

			return len(messages)

		# Get file count for weighting the compression metrics
		file_count = len(objects_metadata)
		logger.debug(f'Compressing {file_count} files with DDB Item Key: {ddb_key_name}')

		# Compress objects and manifest with source bucket/prefix for adaptive compression
		# Pass the DDB Item Key, targets, and file count to enable optimized compression settings
		success, compressed_path, original_size, compressed_size, compression_level = compress_objects(
			object_paths,
			manifest_path,
			temp_dir,
			source_bucket,
			source_prefix,
			ddb_key_name=ddb_key_name,
			targets=targets,
			file_count=file_count,
		)

		if not success:
			logger.error('Failed to compress objects')

			return len(messages)

		# Upload compressed file to outbound bucket
		# Use monitored prefix for upload if available, otherwise use source_prefix
		# This ensures all objects get compressed together but are uploaded to correct path
		if monitored_prefix:
			# Normalize path to avoid double slashes
			normalized_prefix = monitored_prefix.rstrip('/')
			s3_key = f'{source_bucket}/{normalized_prefix}/{uuid.uuid4().hex}.tar.zst'
			logger.info(f'Using monitored prefix for upload path: {normalized_prefix}')
		elif not source_prefix:
			s3_key = f'{source_bucket}/{uuid.uuid4().hex}.tar.zst'
		else:
			s3_key = f'{source_bucket}/{source_prefix}/{uuid.uuid4().hex}.tar.zst'

		if not upload_to_s3(compressed_path, outbound_bucket, s3_key):
			logger.error(f'Failed to upload compressed file to {outbound_bucket}/{s3_key}')
			return 0

		# Delete the compressed file after successful upload to save disk space
		try:
			os.remove(compressed_path)
			logger.debug(f'Deleted compressed file after S3 upload: {compressed_path}')
		except Exception as e:
			logger.debug(f'Could not delete compressed file {compressed_path}: {e}')

		# Calculate full processing time from after SQS message retrieval through object download, compression, and upload prep
		processing_time = time.time() - start_processing_time

		# Update metrics in both systems now that processing_time is available
		# 1. Update compression metrics in the manager for adaptive compression optimization
		num_regions = len(targets) if targets else 1
		CompressionManager.get_instance().update_compression_metrics(
			bucket=source_bucket,
			prefix=source_prefix or '',
			level=compression_level,
			original_size=original_size,
			compressed_size=compressed_size,
			compression_time=None,  # We don't have this value here, but processing_time is more important
			processing_time=processing_time,
			num_regions=num_regions,
			ddb_key_name=ddb_key_name,
			file_count=file_count,
		)

		# 2. Report metrics to CloudWatch with the updated metrics module for monitoring
		# Pass the monitored_prefix from environment variable to consolidate metrics under a single prefix dimension
		report_compression_metrics(
			source_bucket=source_bucket,
			source_prefix=source_prefix,
			original_size=original_size,
			compressed_size=compressed_size,
			processing_time=processing_time,
			targets=targets,
			monitored_prefix=monitored_prefix,
		)

		# Delete processed messages
		delete_sqs_messages_batch(queue_url, receipt_handles)

		logger.info(
			f'Successfully processed {len(messages)} messages, '
			f'compressed {len(objects_metadata)} objects from {original_size} to {compressed_size} bytes '
			f'({(compressed_size / original_size * 100):.2f}% of original size) '
			f'using compression level {compression_level}'
		)

		return len(messages)

	finally:
		# Clean up temporary directory
		cleanup_temp_directory(temp_dir)


def main():
	"""
	Main function to run the application.
	"""
	# Register signal handlers
	signal.signal(signal.SIGTERM, signal_handler)
	signal.signal(signal.SIGINT, signal_handler)

	# Get environment variables
	queue_url = get_env_var('SQS_QUEUE_URL')
	outbound_bucket = get_env_var('BUCKET')
	region = get_env_var('AWS_DEFAULT_REGION')
	stack_name = get_env_var('STACK_NAME')
	# Get monitored prefix (optional - can be empty for bucket root)
	monitored_prefix = get_env_var('MONITORED_PREFIX', required=False) or ''

	logger.info(f'Starting Source Region Container: {region}')
	logger.info(f'Queue URL: {queue_url}')
	logger.info(f'Outbound Bucket: {outbound_bucket}')
	logger.info(f'Monitored Prefix: {monitored_prefix or "(bucket root)"}')

	# Run CPU benchmark once at startup (max 10 seconds)
	logger.info('Running CPU benchmark to normalize performance metrics...')
	cpu_factor = run_cpu_benchmark(max_duration=10)
	logger.info(f'Using ZSTD version {zstd_version}')

	# Initialize compression_manager with the CPU factor
	CompressionManager.initialize(cpu_factor=cpu_factor)

	# Main processing loop
	while running:
		try:
			# Process batch of messages
			processed = process_message_batch(
				queue_url=queue_url,
				outbound_bucket=outbound_bucket,
				stack_name=stack_name,
				monitored_prefix=monitored_prefix,
			)

			if processed == 0:
				# No messages processed, wait before polling again
				time.sleep(1)

		except Exception as e:
			logger.exception(f'Error in processing loop: {e}')
			time.sleep(5)

	logger.info('Shutting down')


if __name__ == '__main__':
	main()
