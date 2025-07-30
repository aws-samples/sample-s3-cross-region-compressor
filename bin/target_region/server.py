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
import boto3

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
		# Generate a unique filename
		filename = f'{uuid.uuid4().hex}_{os.path.basename(key)}'
		local_path = os.path.join(temp_dir, filename)

		# Download the object

		if not get_s3_object(bucket, key, local_path):
			logger.error(f'Download failed for {bucket}/{key}')
			return False, '', {}


		return True, local_path, {'bucket': bucket, 'key': key}
	except Exception as e:
		logger.exception(f'Exception in process_s3_object: {e}')
		return False, '', {}


@track_processing_time
def upload_compressed_backup(compressed_file_path: str, manifest: Dict) -> bool:
	"""
	Upload compressed backup file directly to target buckets without decompression.

	Args:
	    compressed_file_path: Path to the compressed backup file
	    manifest: Manifest containing target information

	Returns:
	    True if successful, False otherwise
	"""
	try:
		targets = manifest.get('targets', [])
		if not targets:
			logger.error('No targets found in manifest for backup upload')
			return False

		current_region = get_current_region()
		current_region_targets = [t for t in targets if t.get('region') == current_region]

		if not current_region_targets:
			logger.debug(f'No targets in current region ({current_region}) for backup, skipping')
			return True

		# Legacy function - redirect to direct upload
		return upload_compressed_backup_direct(compressed_file_path, manifest, '')
	except Exception as e:
		logger.exception(f'Exception in upload_compressed_backup: {e}')
		return False

def upload_compressed_backup_direct(local_path: str, manifest: dict, subfolder: str) -> bool:
	"""
	Upload compressed backup directly without decompress/recompress (for same-folder files).
	"""
	try:
		import uuid
		from utils.aws_utils import upload_to_s3
		
		# Generate backup filename and determine placement
		backup_filename = f'{uuid.uuid4().hex}.tar.zst'
		monitored_prefix = os.environ.get('MONITORED_PREFIX', '')
		
		if monitored_prefix and subfolder:
			target_key = f'{monitored_prefix}/{subfolder}/{backup_filename}'
		elif monitored_prefix:
			target_key = f'{monitored_prefix}/{backup_filename}'
		elif subfolder:
			target_key = f'{subfolder}/{backup_filename}'
		else:
			target_key = backup_filename
		
		# Upload to each backup target
		success = True
		for target in manifest.get('targets', []):
			target_bucket = target.get('bucket')
			if not target_bucket:
				logger.warning('Target missing bucket name for backup upload')
				success = False
				continue
			
			logger.info(f'Uploading backup directly to {target_bucket}/{target_key}')
			if not upload_to_s3(local_path, target_bucket, target_key):
				logger.error(f'Failed to upload backup to {target_bucket}/{target_key}')
				success = False
			else:
				logger.info(f'Successfully uploaded backup to {target_bucket}/{target_key}')
		
		# Track manifest contents for backup file
		track_backup_manifest(backup_filename, manifest, manifest.get('targets', []))
		
		return success
		
	except Exception as e:
		logger.exception(f'Exception in upload_compressed_backup_direct: {e}')
		return False


def create_folder_backup(tar_path: str, extract_dir: str, folder_objects: list, backup_targets: list, subfolder: str) -> bool:
	"""
	Create a backup archive for files from a specific folder.
	
	Args:
	    tar_path: Path to the source TAR file
	    extract_dir: Directory where files are extracted
	    folder_objects: List of objects for this folder
	    backup_targets: List of backup target configurations
	    subfolder: The subfolder these objects belong to
	    
	Returns:
	    True if successful, False otherwise
	"""
	try:
		import tempfile
		import tarfile
		import uuid
		import json
		from utils.aws_utils import upload_to_s3
		import pyzstd
		
		# Create temporary directory for this folder's backup
		with tempfile.TemporaryDirectory() as temp_backup_dir:
			# Extract only the files for this folder from the TAR
			extracted_files = []
			with tarfile.open(tar_path, 'r') as tar:
				for obj in folder_objects:
					relative_key = obj.get('relative_key', '')
					tar_member_name = f'objects/{relative_key}'
					
					try:
						member = tar.getmember(tar_member_name)
						tar.extract(member, temp_backup_dir)
						
						# Add to extracted files list
						extracted_path = os.path.join(temp_backup_dir, tar_member_name)
						extracted_files.append({
							'local_path': extracted_path,
							'object_name': obj.get('object_name', ''),
							'relative_key': relative_key
						})
					except KeyError:
						logger.error(f'File not found in TAR: {tar_member_name}')
						continue
			
			if not extracted_files:
				logger.error(f'No files extracted for folder: {subfolder}')
				return False
			
			# Create manifest for this folder
			folder_manifest = {
				'targets': backup_targets,
				'objects': folder_objects
			}
			
			manifest_path = os.path.join(temp_backup_dir, 'manifest.json')
			with open(manifest_path, 'w') as f:
				json.dump(folder_manifest, f, indent=2)
			
			# Create new TAR with extracted files + manifest
			tar_backup_path = os.path.join(temp_backup_dir, 'backup.tar')
			with tarfile.open(tar_backup_path, 'w') as backup_tar:
				# Add manifest
				backup_tar.add(manifest_path, arcname='manifest.json')
				
				# Add extracted files
				for file_info in extracted_files:
					local_path = file_info['local_path']
					relative_key = file_info['relative_key']
					if os.path.exists(local_path):
						backup_tar.add(local_path, arcname=f'objects/{relative_key}')
			
			# Compress the TAR with zstd
			compressed_path = os.path.join(temp_backup_dir, 'backup.tar.zst')
			with open(tar_backup_path, 'rb') as tar_file:
				with open(compressed_path, 'wb') as compressed_file:
					option = {pyzstd.CParameter.compressionLevel: 3}
					pyzstd.compress_stream(tar_file, compressed_file, level_or_option=option)
			
			# Generate backup filename and determine placement
			backup_filename = f'{uuid.uuid4().hex}.tar.zst'
			monitored_prefix = os.environ.get('MONITORED_PREFIX', '')
			
			if monitored_prefix and subfolder:
				target_key = f'{monitored_prefix}/{subfolder}/{backup_filename}'
			elif monitored_prefix:
				target_key = f'{monitored_prefix}/{backup_filename}'
			elif subfolder:
				target_key = f'{subfolder}/{backup_filename}'
			else:
				target_key = backup_filename
			
			# Upload to each backup target
			success = True
			for target in backup_targets:
				target_bucket = target.get('bucket')
				if not target_bucket:
					logger.warning('Target missing bucket name for backup upload')
					success = False
					continue
				
				logger.info(f'Uploading folder backup to {target_bucket}/{target_key}')
				if not upload_to_s3(compressed_path, target_bucket, target_key):
					logger.error(f'Failed to upload backup to {target_bucket}/{target_key}')
					success = False
				else:
					logger.info(f'Successfully uploaded backup to {target_bucket}/{target_key}')
			
			# Track manifest contents for backup file
			track_backup_manifest(backup_filename, folder_manifest, backup_targets)
			
			return success
			
	except Exception as e:
		logger.exception(f'Exception in create_folder_backup: {e}')
		return False

		success = True
		for target in current_region_targets:
			target_bucket = target.get('bucket')
			if not target_bucket:
				logger.warning('Target missing bucket name for backup upload')
				success = False
				continue

			logger.info(f'Uploading compressed backup to {target_bucket}/{target_key}')
			logger.info(f'Compressed file size: {os.path.getsize(compressed_file_path)} bytes')
			
			# Upload compressed file directly with preserved path structure
			if not upload_to_s3(compressed_file_path, target_bucket, target_key):
				logger.error(f'Failed to upload backup to {target_bucket}/{target_key}')
				success = False
			else:
				logger.info(f'Successfully uploaded backup to {target_bucket}/{target_key} ({os.path.getsize(compressed_file_path)} bytes)')

		# Track manifest contents for backup file
		track_backup_manifest(backup_filename, manifest, current_region_targets)

		return success
	except Exception as e:
		logger.exception(f'Exception in upload_compressed_backup: {e}')
		return False


def track_backup_manifest(backup_filename: str, manifest: Dict, targets: list) -> None:
	"""
	Track manifest contents for backup file - prepare data for future Kinesis/Glue integration.

	Args:
	    backup_filename: Name of the backup file
	    manifest: Complete manifest dictionary
	    targets: List of target buckets where backup was stored
	"""
	try:
		# Add backup metadata to manifest
		tracking_manifest = manifest.copy()
		tracking_manifest['backup_file'] = backup_filename
		tracking_manifest['backup_timestamp'] = int(time.time())
		tracking_manifest['target_buckets'] = [t.get('bucket') for t in targets]

		# Get source prefix from manifest for consistent path structure
		objects = manifest.get('objects', [])
		source_prefix = ''
		if objects:
			first_object = objects[0]
			source_prefix = first_object.get('source_prefix', '')



		# Write catalog metadata to S3
		write_catalog_metadata(backup_filename, manifest, targets)
		logger.info(f'Tracking {len(manifest.get("objects", []))} objects in backup {backup_filename}')

	except Exception as e:
		logger.exception(f'Error tracking backup manifest: {e}')


def write_catalog_metadata(backup_filename: str, manifest: Dict, targets: list) -> None:
	"""
	Write backup file metadata to S3 catalog bucket in JSON format.
	
	Args:
	    backup_filename: Name of the backup file
	    manifest: Complete manifest dictionary
	    targets: List of target buckets where backup was stored
	"""
	try:
		catalog_bucket = os.environ.get('CATALOG_BUCKET_NAME')
		if not catalog_bucket:
			logger.warning('CATALOG_BUCKET_NAME not set, skipping catalog write')
			return
		
		objects = manifest.get('objects', [])
		backup_timestamp = int(time.time())
		current_date = time.strftime('%Y-%m-%d', time.gmtime(backup_timestamp))
		
		# Get source info for path structure
		if not objects:
			return
			
		first_object = objects[0]
		source_bucket = first_object.get('source_bucket', '')
		source_prefix = first_object.get('source_prefix', '')
		
		# Create optimized catalog records (one record per object for better Athena performance)
		catalog_records = []
		for obj in objects:
			# Parse creation time for better querying
			creation_time = obj.get('creation_time', '')
			creation_date = creation_time.split(' ')[0] if creation_time else current_date
			
			record = {
				'backup_file': backup_filename,
				'backup_timestamp': backup_timestamp,
				'backup_date': current_date,
				'source_bucket': source_bucket,
				'source_prefix': source_prefix,
				'object_name': obj.get('object_name', ''),
				'object_path': f"{source_prefix}/{obj.get('relative_key', obj.get('object_name', ''))}" if source_prefix else obj.get('relative_key', obj.get('object_name', '')),
				'object_size': obj.get('size', 0),
				'creation_time': creation_time,
				'creation_date': creation_date,
				'target_buckets': [t.get('bucket') for t in targets]
			}
			catalog_records.append(record)
		
		# Create S3 key with year/month/day structure using monitored prefix only
		# This ensures all files go into a single table regardless of subfolders
		monitored_prefix = os.environ.get('MONITORED_PREFIX', '')
		year, month, day = current_date.split('-')
		if monitored_prefix:
			s3_key = f'{source_bucket}/{monitored_prefix}/year={year}/month={month}/day={day}/{backup_filename}.jsonl'
		else:
			s3_key = f'{source_bucket}/year={year}/month={month}/day={day}/{backup_filename}.jsonl'
		
		# Write each record as a separate line (JSONL format for better Athena performance)
		import tempfile, json
		with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
			for record in catalog_records:
				f.write(json.dumps(record) + '\n')
			temp_path = f.name
			

		upload_to_s3(temp_path, catalog_bucket, s3_key)
		os.unlink(temp_path)
		
		logger.info(f'Wrote catalog metadata: {len(catalog_records)} records to s3://{catalog_bucket}/{s3_key}')
		
	except Exception as e:
		logger.exception(f'Error writing catalog metadata: {e}')


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

				# Construct target key using relative_key to preserve full directory structure
				object_name = object_info.get('object_name', '')
				source_prefix = object_info.get('source_prefix', '')
				relative_key = object_info.get('relative_key', object_name)

				# Use source prefix + relative key to maintain full path structure
				if source_prefix:
					target_key = f'{source_prefix}/{relative_key}'
				else:
					target_key = relative_key

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
		# Retrieve messages from SQS
		messages = get_sqs_messages(
			queue_url=queue_url,
			max_messages=MAX_MESSAGES_PER_BATCH,
		)

		if not messages:
			return 0



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
	
			delete_sqs_messages_batch(queue_url, test_event_receipt_handles)


		# If all messages were test events, return now
		if not regular_messages:
			logger.info('All messages were test events, no further processing needed')
			return len(messages)

		# Continue processing with regular messages only
		messages = regular_messages


		# Create temporary directory
		temp_dir = create_temp_directory()


		try:
			# Process each message
			receipt_handles = []
			s3_objects = []

			for message in messages:
				receipt_handles.append(message['ReceiptHandle'])
				extracted_objects = extract_s3_event_info(message)

				s3_objects.extend(extracted_objects)



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



				# Read the manifest file - with our new approach, this should already be extracted
				manifest_path = os.path.join(extract_dir, 'manifest.json')
				if not os.path.exists(manifest_path):
					logger.error(f'Manifest file not found: {manifest_path}')
					continue


				manifest = read_manifest_from_file(manifest_path)
				if not manifest:
					logger.error('Failed to read manifest file')
					continue
				


				# Check if any destination in current region has backup mode enabled
				current_region = get_current_region()
				targets = manifest.get('targets', [])
				
				backup_targets = [t for t in targets if t.get('region') == current_region and t.get('backup', False)]
				normal_targets = [t for t in targets if t.get('region') == current_region and not t.get('backup', False)]

				# Handle backup destinations - check if decompress/regroup is needed
				if backup_targets:
					logger.info(f'Backup mode: processing {len(manifest.get("objects", []))} objects for proper folder placement')
					
					# Check if all files are from the same subfolder
				objects = manifest.get('objects', [])
				subfolders = set()
				for obj in objects:
					relative_key = obj.get('relative_key', '')
					subfolder = '/'.join(relative_key.split('/')[:-1]) if '/' in relative_key else ''
					subfolders.add(subfolder)
				
				if len(subfolders) == 1:
					# All files from same folder - use direct upload (no decompress/recompress)
					subfolder = list(subfolders)[0]
					logger.info(f'All files from same folder "{subfolder or "root"}": using direct upload (fast path)')
					
					# Create backup-specific manifest with only backup targets
					backup_manifest = manifest.copy()
					backup_manifest['targets'] = backup_targets
					
					if upload_compressed_backup_direct(local_path, backup_manifest, subfolder):
						logger.info('Successfully uploaded backup archive directly')
					else:
						logger.error('Failed to upload backup archive directly')
				else:
					# Mixed folders - need to decompress and regroup
					logger.info(f'Mixed folders detected ({len(subfolders)} folders): {list(subfolders)} - decompress/regroup needed (slow path)')
					
					# Group objects by their subfolder
					from collections import defaultdict
					objects_by_folder = defaultdict(list)
					for obj in objects:
						relative_key = obj.get('relative_key', '')
						subfolder = '/'.join(relative_key.split('/')[:-1]) if '/' in relative_key else ''
						objects_by_folder[subfolder].append(obj)
					
					# Process each folder group
					for subfolder, folder_objects in objects_by_folder.items():
						logger.info(f'Creating backup for folder "{subfolder or "root"}": {len(folder_objects)} files')
						
						# Create backup for this folder group
						if create_folder_backup(tar_path, extract_dir, folder_objects, backup_targets, subfolder):
							logger.info(f'Successfully created backup for folder: {subfolder or "root"}')
						else:
							logger.error(f'Failed to create backup for folder: {subfolder or "root"}')

				# If all destinations are backup mode, skip decompression entirely
				if not normal_targets:
					logger.info('All destinations are backup mode - skipping decompression')
					# Delete the compressed object from staging bucket
					try:
						if delete_s3_object(s3_source_info['bucket'], s3_source_info['key']):
							logger.debug(f'Deleted compressed backup from staging: {s3_source_info["bucket"]}/{s3_source_info["key"]}')
					except Exception as e:
						logger.exception(f'Error deleting compressed backup: {e}')
					continue

				try:
					# Log manifest structure for debugging
					logger.debug(
						f'Manifest structure: objects={len(manifest.get("objects", []))}, has_targets={("targets" in manifest)}, backup_targets={len(backup_targets)}, normal_targets={len(normal_targets)}'
					)
				except Exception as e:
					logger.error(f'Error logging manifest structure: {e}')

				# Get list of objects from TAR file (without extracting them yet)
				tar_members = get_tar_members(tar_path)
				object_members = [m for m in tar_members if m != 'manifest.json']


				# Update manifest to only include normal (non-backup) targets for decompression
				if normal_targets:
					manifest['targets'] = normal_targets
					logger.info(f'Processing decompression for {len(normal_targets)} normal destinations')

				# Get mapping of object paths from manifest (but without the actual extracted files)

				object_infos = get_object_paths_from_manifest(manifest, extract_dir)
				if not object_infos:
					logger.error('No valid objects found in manifest')
					continue



				# Create a dictionary mapping relative keys to their info for quick lookup
				object_map = {}
				for obj_info in object_infos:
					# Use relative_key as the primary lookup key
					relative_key = obj_info.get('relative_key', '')
					if relative_key:
						object_map[relative_key] = obj_info

				# Process each object one at a time with streaming extraction

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

						upload_success = upload_object_to_targets(object_info)
						upload_results.append(upload_success)


						# Delete the extracted file to free up space immediately
						try:
							if os.path.exists(extracted_path):
								os.remove(extracted_path)

						except Exception as e:
							logger.error(f'Error removing extracted file {extracted_path}: {e}')

				except Exception as e:
					logger.exception(f'Exception in streaming extraction process: {e}')

				# Clean up the TAR file as well since we're done with it
				try:
					if os.path.exists(tar_path):
						os.remove(tar_path)

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


			# Delete processed messages
			try:
				delete_sqs_messages_batch(queue_url, receipt_handles)

			except Exception as e:
				logger.exception(f'Error deleting SQS messages: {e}')

			return len(messages)

		finally:
			# Clean up temporary directory

			cleanup_temp_directory(temp_dir)


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
