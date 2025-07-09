#!/usr/bin/env python3
"""
Target Region Container for S3 Cross-Region Compressor

This application processes SQS messages for S3 object creation events in the target staging bucket,
decompresses the objects using zstd, and uploads them to the target S3 buckets
according to the manifest information.

Environment Variables:
    SQS_QUEUE_URL: URL of the SQS queue to poll for messages
    AWS_DEFAULT_REGION: AWS region
"""

import logging
import os
import signal
import time
import uuid
from typing import Dict, Tuple

import traceback
from pythonjsonlogger import jsonlogger

# Import utility modules
from utils.aws_utils import (
	delete_sqs_messages_batch,
	extract_s3_event_info,
	get_s3_object,
	get_sqs_messages,
	is_s3_test_event,
	upload_to_s3,
	delete_s3_object,
	get_env_var,
	get_current_region,
)
from utils.decompression import (
	cleanup_temp_directory,
	create_temp_directory,
	decompress_and_extract,
	stream_extract_file,
	get_tar_members,
)
from utils.manifest import (
	read_manifest_from_file,
	get_object_paths_from_manifest,
	prepare_object_tags,
)
from utils.metrics import report_decompression_metrics, track_processing_time

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Add JSON formatter for structured logging
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(name)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

# Constants
MAX_MESSAGES_PER_BATCH = 1
MAX_WORKERS = max(1, os.cpu_count() or 4)
POLL_INTERVAL = 20  # seconds

# Global variables
running = True


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


@track_processing_time
def process_s3_object(s3_object: Dict, temp_dir: str) -> Tuple[bool, str, Dict]:
	"""
	Process an S3 object: download it and prepare for decompression.

	Args:
	    s3_object: Dictionary with bucket and key information
	    temp_dir: Temporary directory for downloaded files

	Returns:
	    Tuple of (success, local_path, object_info)
	"""
	try:
		bucket = s3_object['bucket']
		key = s3_object['key']

		logger.debug(f'Processing S3 object from {bucket}/{key}')

		# Generate a unique filename
		filename = f'{uuid.uuid4().hex}_{os.path.basename(key)}'
		local_path = os.path.join(temp_dir, filename)

		# Download the object
		logger.debug(f'Downloading object to {local_path}')
		if not get_s3_object(bucket, key, local_path):
			logger.error(f'Download failed for {bucket}/{key}')
			return False, '', {}

		logger.debug(f'Successfully downloaded {bucket}/{key}')
		return True, local_path, {'bucket': bucket, 'key': key}
	except Exception as e:
		logger.exception(f'Exception in process_s3_object: {e}')
		return False, '', {}


@track_processing_time
def upload_object_to_targets(object_info: Dict) -> bool:
	"""
	Upload an object to its target buckets.

	Args:
	    object_info: Dictionary with object information

	Returns:
	    True if successful, False otherwise
	"""
	try:
		object_name = object_info.get('object_name', 'unknown')
		logger.info(f'Starting upload for object: {object_name}')

		local_path = object_info.get('local_path')
		if not local_path or not os.path.exists(local_path):
			logger.error(f'Object file not found: {local_path}')
			return False

		# Prepare tags
		tags = prepare_object_tags(object_info)

		# Get targets
		targets = object_info.get('targets', [])
		if not targets:
			logger.error(f'No targets specified for object: {object_name}')
			return False

		# Get current region
		current_region = get_current_region()
		logger.debug(f'Current AWS region: {current_region}')

		# Find targets for current region
		current_region_targets = []
		other_region_targets = []

		for target in targets:
			target_region = target.get('region')
			if target_region == current_region:
				current_region_targets.append(target)
			else:
				other_region_targets.append(target)

		logger.debug(
			f'Object {object_name} has {len(current_region_targets)} targets in current region and {len(other_region_targets)} in other regions'
		)

		# Skip if no targets for this region
		if not current_region_targets:
			logger.debug(f'No targets in current region ({current_region}) for object {object_name}, skipping')
			return True

		# Upload to each target in current region
		success = True
		for target in current_region_targets:
			try:
				target_bucket = target.get('bucket')
				target_region = target.get('region')

				logger.debug(f'Processing target in region {target_region}: bucket={target_bucket}')

				if not target_bucket:
					logger.warning(f'Target missing bucket name for {object_name}')
					success = False
					continue

				# Construct target key
				object_name = object_info.get('object_name', '')
				source_prefix = object_info.get('source_prefix', '')

				# Use source prefix if available, otherwise use the object name directly
				if source_prefix:
					target_key = f'{source_prefix}/{object_name}'
				else:
					target_key = object_name

				logger.debug(f'Uploading to {target_bucket}/{target_key}')

				# Get original storage class from object info
				original_storage_class = object_info.get('storage_class', 'STANDARD')

				# Check if there's a storage class override in the target configuration
				# This handles storage_class from target configuration and manifest targets
				target_storage_class = target.get('storage_class')

				# Use target storage class if specified, otherwise use the original
				storage_class = target_storage_class if target_storage_class else original_storage_class

				# Log storage class decision with more details
				if target_storage_class:
					logger.debug(
						f'Overriding storage class to {target_storage_class} (from target config) for {target_bucket}/{target_key}'
					)
					logger.debug(f'Target configuration: {target}')
				else:
					logger.debug(
						f'Using original storage class: {original_storage_class} for {target_bucket}/{target_key}'
					)
					logger.debug(f'Target region: {target_region}, bucket: {target_bucket}')

				# Get KMS key ARN from target configuration if available
				kms_key_arn = target.get('kms_key_arn')
				if kms_key_arn:
					logger.debug(f'Using KMS encryption with key from target config for {target_bucket}/{target_key}')

				# Upload to target bucket with selected storage class and KMS key if provided
				if not upload_to_s3(local_path, target_bucket, target_key, tags, storage_class, kms_key_arn):
					logger.error(f'Failed to upload to target bucket: {target_bucket}/{target_key}')
					success = False
				else:
					logger.debug(
						f'Successfully uploaded to target bucket: {target_bucket}/{target_key} with storage class {storage_class}'
					)
			except Exception as e:
				logger.exception(f'Exception during upload to target: {e}')
				success = False

		# Log information about skipped targets
		if other_region_targets:
			other_regions = set(t.get('region') for t in other_region_targets)
			logger.debug(f'Skipped {len(other_region_targets)} targets in other regions: {", ".join(other_regions)}')

		logger.info(f'Upload complete for object {object_name}, success={success}')
		return success
	except Exception as e:
		logger.exception(f'Unhandled exception in upload_object_to_targets: {e}')
		return False


@track_processing_time
def process_message_batch(queue_url: str) -> int:
	"""
	Process a batch of SQS messages.

	Args:
	    queue_url: URL of the SQS queue

	Returns:
	    Number of successfully processed messages
	"""
	try:
		logger.info('Starting to process message batch')

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
			logger.info('All messages were test events, no further processing needed')
			return len(messages)

		# Continue processing with regular messages only
		messages = regular_messages
		logger.info(f'Processing {len(messages)} non-test event messages')

		# Create temporary directory
		temp_dir = create_temp_directory()
		logger.debug(f'Created temporary directory: {temp_dir}')

		try:
			# Process each message
			receipt_handles = []
			s3_objects = []

			for message in messages:
				receipt_handles.append(message['ReceiptHandle'])
				extracted_objects = extract_s3_event_info(message)
				logger.debug(f'Extracted {len(extracted_objects)} S3 objects from message')
				s3_objects.extend(extracted_objects)

			logger.info(f'Total S3 objects to process: {len(s3_objects)}')

			if not s3_objects:
				logger.warning('No S3 objects found in messages')
				# delete_sqs_messages_batch(queue_url, receipt_handles)
				return len(messages)

			# Process each S3 object (compressed archive)
			for s3_index, s3_object in enumerate(s3_objects, 1):
				logger.info(
					f'Processing S3 object {s3_index}/{len(s3_objects)}: {s3_object.get("bucket", "Unknown")}/{s3_object.get("key", "Unknown")}'
				)

				# Download the compressed object
				success, local_path, s3_source_info = process_s3_object(s3_object, temp_dir)
				if not success:
					logger.error(f'Failed to download object: {s3_object}')
					continue

				logger.debug(f'Decompressing archive: {local_path}')
				# Decompress and extract the archive
				success, extract_dir, compressed_size, decompressed_size = decompress_and_extract(local_path, temp_dir)
				if not success:
					logger.error(f'Failed to decompress and extract archive: {local_path}')
					continue

				# Path to the TAR file
				tar_path = os.path.join(temp_dir, 'archive.tar')
				if not os.path.exists(tar_path):
					logger.error(f'TAR file not found: {tar_path}')
					continue

				logger.debug(f'Archive ready for streaming extraction at: {extract_dir}')

				# Read the manifest file - with our new approach, this should already be extracted
				manifest_path = os.path.join(extract_dir, 'manifest.json')
				if not os.path.exists(manifest_path):
					logger.error(f'Manifest file not found: {manifest_path}')
					continue

				logger.debug(f'Reading manifest file: {manifest_path}')
				manifest = read_manifest_from_file(manifest_path)
				if not manifest:
					logger.error('Failed to read manifest file')
					continue

				try:
					# Log manifest structure for debugging
					logger.debug(
						f'Manifest structure: objects={len(manifest.get("objects", []))}, has_targets={("targets" in manifest)}'
					)
				except Exception as e:
					logger.error(f'Error logging manifest structure: {e}')

				# Get list of objects from TAR file (without extracting them yet)
				tar_members = get_tar_members(tar_path)
				object_members = [m for m in tar_members if m != 'manifest.json']
				logger.debug(f'Found {len(object_members)} object files in TAR archive')

				# Get mapping of object paths from manifest (but without the actual extracted files)
				logger.debug('Getting object information from manifest')
				object_infos = get_object_paths_from_manifest(manifest, extract_dir)
				if not object_infos:
					logger.error('No valid objects found in manifest')
					continue

				logger.info(f'Found {len(object_infos)} objects in manifest')

				# Create a dictionary mapping relative keys to their info for quick lookup
				object_map = {}
				for obj_info in object_infos:
					# Use relative_key as the primary lookup key
					relative_key = obj_info.get('relative_key', '')
					if relative_key:
						object_map[relative_key] = obj_info

				# Process each object one at a time with streaming extraction
				logger.info(f'Starting streaming extraction and upload of {len(object_members)} objects')
				upload_results = []

				try:
					# We'll still use ThreadPoolExecutor, but for each object we'll:
					# 1. Extract that object from the TAR
					# 2. Upload it
					# 3. Delete the extracted file before moving to the next one
					for member_name in object_members:
						# Get the relative key by removing the 'objects/' prefix
						relative_key = (
							member_name.replace('objects/', '', 1)
							if member_name.startswith('objects/')
							else member_name
						)

						# Skip if we can't find this object in the manifest
						if relative_key not in object_map:
							logger.warning(
								f'Object with path {relative_key} found in TAR but not in manifest, skipping'
							)
							continue

						logger.debug(f'Streaming extraction of {member_name}')

						# Extract just this one file from the TAR
						extraction_success = stream_extract_file(tar_path, member_name, extract_dir)
						if not extraction_success:
							logger.error(f'Failed to extract {member_name} from TAR')
							upload_results.append(False)
							continue

						# Get the object info using the relative key
						object_info = object_map[relative_key]

						# Set the local path (which now exists from the extraction)
						extracted_path = os.path.join(extract_dir, member_name)
						object_info['local_path'] = extracted_path

						# Upload this object
						logger.debug(f'Uploading extracted object: {object_info["object_name"]}')
						upload_success = upload_object_to_targets(object_info)
						upload_results.append(upload_success)
						logger.debug(f'Upload result for {object_info["object_name"]}: {upload_success}')

						# Delete the extracted file to free up space immediately
						try:
							if os.path.exists(extracted_path):
								os.remove(extracted_path)
								logger.debug(f'Removed extracted file after upload: {extracted_path}')
						except Exception as e:
							logger.error(f'Error removing extracted file {extracted_path}: {e}')

				except Exception as e:
					logger.exception(f'Exception in streaming extraction process: {e}')

				# Clean up the TAR file as well since we're done with it
				try:
					if os.path.exists(tar_path):
						os.remove(tar_path)
						logger.debug(f'Removed TAR file after processing: {tar_path}')
				except Exception as e:
					logger.error(f'Error removing TAR file {tar_path}: {e}')

				# Check if all uploads were successful
				successes = upload_results.count(True)
				failures = upload_results.count(False)
				logger.info(f'Upload results: {successes} successes, {failures} failures out of {len(upload_results)}')

				if failures == 0 and len(upload_results) > 0:
					logger.info(f'Successfully processed {len(object_infos)} objects')

					# Report metrics
					try:
						# Use the first target bucket for metrics
						first_object = object_infos[0]
						first_target = first_object.get('targets', [{}])[0]
						target_bucket = first_target.get('bucket', 'unknown')

						logger.debug(f'Reporting metrics to bucket: {target_bucket}')
						report_decompression_metrics(target_bucket, compressed_size, decompressed_size)
					except Exception as e:
						logger.exception(f'Error reporting metrics: {e}')

					# Delete the compressed object from the staging bucket
					try:
						if delete_s3_object(s3_source_info['bucket'], s3_source_info['key']):
							logger.debug(
								f'Deleted compressed object from staging bucket: {s3_source_info["bucket"]}/{s3_source_info["key"]}'
							)
						else:
							logger.warning(
								f'Failed to delete compressed object from staging bucket: {s3_source_info["bucket"]}/{s3_source_info["key"]}'
							)
					except Exception as e:
						logger.exception(f'Error deleting compressed object: {e}')
				else:
					logger.warning(f'Some objects failed to upload: {failures} failures out of {len(upload_results)}')

			logger.info(f'Finished processing all S3 objects, deleting {len(receipt_handles)} SQS messages')
			# Delete processed messages
			try:
				delete_sqs_messages_batch(queue_url, receipt_handles)
				logger.debug('Successfully deleted SQS messages')
			except Exception as e:
				logger.exception(f'Error deleting SQS messages: {e}')

			return len(messages)

		finally:
			# Clean up temporary directory
			logger.debug(f'Cleaning up temporary directory: {temp_dir}')
			cleanup_temp_directory(temp_dir)
			logger.debug('Temporary directory cleaned')

	except Exception:
		logger.exception(f'Unhandled exception in process_message_batch: {traceback.format_exc()}')
		return 0


def main():
	"""
	Main function to run the application.
	"""
	# Register signal handlers
	signal.signal(signal.SIGTERM, signal_handler)
	signal.signal(signal.SIGINT, signal_handler)

	# Get environment variables
	queue_url = get_env_var('SQS_QUEUE_URL')

	logger.info('Starting Target Region Container')
	logger.info(f'Queue URL: {queue_url}')
	logger.info(f'MAX_WORKERS: {MAX_WORKERS}, MAX_MESSAGES_PER_BATCH: {MAX_MESSAGES_PER_BATCH}')

	# Get current region for logging
	current_region = get_current_region()

	logger.info(f'Starting Target Region Container in region: {current_region}')
	logger.info(f'Queue URL: {queue_url}')
	logger.info(f'MAX_WORKERS: {MAX_WORKERS}, MAX_MESSAGES_PER_BATCH: {MAX_MESSAGES_PER_BATCH}')

	# Main processing loop
	while running:
		try:
			logger.info(f'Starting processing cycle in region: {current_region}')
			processed = process_message_batch(queue_url)
			logger.info(f'Processing cycle completed, processed {processed} messages')

			if processed == 0:
				# No messages processed, wait before polling again
				logger.debug(f'No messages processed, waiting {POLL_INTERVAL} seconds before polling again')
				time.sleep(POLL_INTERVAL)

		except Exception as e:
			logger.exception(f'Error in processing loop: {e}')
			logger.exception(f'Stack trace: {traceback.format_exc()}')
			time.sleep(POLL_INTERVAL)

	logger.info('Shutting down')


if __name__ == '__main__':
	main()
