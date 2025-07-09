"""
Compression Utilities for Source Region Container

This module provides utilities for compressing files using zstd:
- Parallel compression of multiple files
- TAR archive creation
- Temporary file management
- Dynamic compression level selection
"""

import logging
import os
import shutil
import tarfile
import tempfile
import time
from typing import Dict, List, Tuple

import pyzstd

# For memory detection
import psutil

# Import the compression manager and metrics
from utils.compression_manager import CompressionManager

# Configure logging
logger = logging.getLogger(__name__)

# Constants
DEFAULT_COMPRESSION_LEVEL = 12
MAX_WORKERS = max(1, os.cpu_count() or 1)  # Default to 1 if cpu_count returns None


def get_available_memory():
	"""
	Detect available memory in the container environment.
	Enhanced for non-root user execution.

	Returns:
	    Available memory in bytes
	"""
	try:
		# Use psutil to get available memory
		memory_info = psutil.virtual_memory()
		available = memory_info.available
		
		# Validate the result makes sense
		if available <= 0 or available > memory_info.total:
			raise ValueError(f"Invalid memory reading: {available}")
			
		logger.info(f"Detected available memory: {available / 1024 / 1024:.1f}MB")
		return available
	except Exception as e:
		# Enhanced fallback with multiple detection methods
		logger.warning(f'Error detecting available memory with psutil: {e}')
		
		# Try reading from /proc/meminfo if available
		try:
			with open('/proc/meminfo', 'r') as f:
				for line in f:
					if line.startswith('MemAvailable:'):
						kb = int(line.split()[1])
						bytes_available = kb * 1024
						logger.info(f"Fallback memory detection from /proc/meminfo: {bytes_available / 1024 / 1024:.1f}MB")
						return bytes_available
		except Exception as proc_e:
			logger.warning(f'Failed to read /proc/meminfo: {proc_e}')
		
		# Final fallback - assume 1.5GB available (safe for 2GB container)
		fallback_memory = int(1.5 * (1024**3))
		logger.warning(f'Using conservative fallback memory estimate: {fallback_memory / 1024 / 1024:.1f}MB')
		return fallback_memory


def calculate_buffer_sizes(available_memory):
	"""
	Calculate optimal buffer sizes based on available memory.

	Args:
	    available_memory: Available memory in bytes

	Returns:
	    Tuple of (read_size, write_size) in bytes
	"""
	# Use up to 15% of available memory for compression as specified
	max_buffer_memory = available_memory * 0.15

	# 45% for read buffer, 55% for write buffer
	read_size = int(max_buffer_memory * 0.45)
	write_size = int(max_buffer_memory * 0.55)

	logger.info(f'Configured compression buffers: read_size={read_size}, write_size={write_size}')
	return read_size, write_size


# Calculate buffer sizes based on available memory - do this once at module import
AVAILABLE_MEMORY = get_available_memory()
READ_BUFFER_SIZE, WRITE_BUFFER_SIZE = calculate_buffer_sizes(AVAILABLE_MEMORY)
logger.info(
	f'System memory: {AVAILABLE_MEMORY / 1024 / 1024:.1f}MB, allocated for compression: {AVAILABLE_MEMORY * 0.15 / 1024 / 1024:.1f}MB'
)


def create_tar_archive(files: List[Dict], output_path: str, temp_dir: str) -> Tuple[bool, str]:
	"""
	Create a TAR archive containing the specified files.
	Delete each file immediately after adding it to the archive to conserve disk space.

	Args:
	    files: List of file dictionaries with 'source_path' and 'archive_path' keys
	    output_path: Path to output TAR file
	    temp_dir: Temporary directory for intermediate files

	Returns:
	    Tuple of (success, tar_path)
	"""
	tar_path = os.path.join(temp_dir, 'archive.tar')

	try:
		with tarfile.open(tar_path, 'w') as tar:
			for file_info in files:
				source_path = file_info['source_path']
				archive_path = file_info['archive_path']

				if os.path.exists(source_path):
					# Add file to archive
					tar.add(source_path, arcname=archive_path)

					# Delete file immediately after adding to archive
					# Skip manifest file - we need it for later
					if not source_path.endswith('manifest.json'):
						try:
							os.remove(source_path)
							logger.debug(f'Deleted temporary file after archiving: {source_path}')
						except Exception as e:
							logger.debug(f'Could not delete temporary file {source_path}: {e}')
				else:
					logger.warning(f'File not found for archiving: {source_path}')

		return True, tar_path
	except Exception as e:
		logger.error(f'Error creating TAR archive: {e}')
		return False, ''


def compress_tar_with_zstd(
	tar_path: str,
	output_path: str,
	source_bucket: str = None,
	source_prefix: str = None,
	level: int = None,
	threads: int = MAX_WORKERS,
	ddb_key_name: str = None,
	targets: list = None,
	file_count: int = 1,
) -> Tuple[bool, int, int, int]:
	"""
	Compress a TAR file using zstd streaming compression with multi-threading and adaptive compression level.
	Uses memory-efficient streaming approach to process files in chunks rather than loading entire file into memory.

	Args:
	    tar_path: Path to input TAR file
	    output_path: Path to output compressed file
	    source_bucket: Original S3 bucket name (for compression metrics)
	    source_prefix: Original S3 prefix (for compression metrics)
	    level: Compression level (1-22). If None, determined dynamically.
	    threads: Number of threads to use
	    ddb_key_name: DDB Item key
	    targets: List of target regions dictionary
	    file_count: Number of files being compressed (for metrics weighting)

	Returns:
	    Tuple of (success, original_size, compressed_size)
	"""
	try:
		# Get original file size
		original_size = os.path.getsize(tar_path)

		# Determine compression level
		if level is None and source_bucket:
			# Determine the key to use for DynamoDB lookups
			bucket_key = (
				ddb_key_name
				if ddb_key_name
				else CompressionManager.get_instance().get_bucket_prefix_key(source_bucket, source_prefix or '')
			)

			# Get optimal level using the new selection strategy
			# This now includes CPU-based adjustment and exploration
			level = CompressionManager.get_instance().get_compression_level(
				source_bucket, source_prefix or '', ddb_key_name=ddb_key_name
			)
			logger.debug(f'Using compression level {level} for {source_bucket}/{source_prefix or ""}')
		else:
			# Use default or explicit level
			level = level if level is not None else DEFAULT_COMPRESSION_LEVEL

		# Compression options
		option = {
			pyzstd.CParameter.compressionLevel: level,
			pyzstd.CParameter.nbWorkers: threads,
		}

		# Start timing the compression
		start_time = time.time()

		# Use pyzstd for streaming multi-threaded compression with dynamically sized buffers
		with open(tar_path, 'rb') as f_in:
			with open(output_path, 'wb') as f_out:
				total_input, total_output = pyzstd.compress_stream(
					f_in,
					f_out,
					level_or_option=option,
					read_size=READ_BUFFER_SIZE,  # Dynamically calculated based on available memory
					write_size=WRITE_BUFFER_SIZE,  # Dynamically calculated based on available memory
				)

		# Calculate compression time (just the ZSTD operation)
		compression_time = time.time() - start_time

		# Get compressed file size
		compressed_size = os.path.getsize(output_path)

		# Delete the TAR file immediately after successful compression
		try:
			os.remove(tar_path)
			logger.debug(f'Deleted intermediate TAR file after compression: {tar_path}')
		except Exception as e:
			logger.debug(f'Could not delete intermediate TAR file {tar_path}: {e}')

		# Return the level used along with other metrics

		return True, original_size, compressed_size, level
	except Exception as e:
		logger.error(f'Error compressing TAR with zstd: {e}')
		# Return 4 values - success, orig_size, comp_size, level
		return False, 0, 0, 0


def compress_objects(
	object_paths: List[Dict],
	manifest_path: str,
	output_dir: str,
	source_bucket: str = None,
	source_prefix: str = None,
	ddb_key_name: str = None,
	targets: list = None,
	file_count: int = 1,
) -> Tuple[bool, str, int, int, int]:
	"""
	Compress multiple S3 objects and a manifest into a single zstd-compressed TAR file.

	Args:
	    object_paths: List of dictionaries with 'local_path' and 'object_name' keys
	    manifest_path: Path to the manifest file
	    output_dir: Directory to store the output file
	    source_bucket: Original S3 bucket name (for compression metrics)
	    source_prefix: Original S3 prefix (for compression metrics)
	    ddb_key_name: name used as DynamoDB key
	    targets: List of target region dictionaries
	    file_count: Number of files being compressed (for metrics weighting)

	Returns:
	    Tuple of (success, output_path, original_total_size, compressed_size)
	"""
	# Create a temporary directory for intermediate files
	temp_dir = tempfile.mkdtemp()

	try:
		# Prepare files for TAR archive
		files_to_archive = []
		total_original_size = 0

		# Add objects
		for obj in object_paths:
			local_path = obj['local_path']
			object_name = obj['object_name']

			# Use relative_key if available to preserve directory structure
			# This prevents name collisions when objects from different prefixes
			# have the same basename
			if 'relative_key' in obj and obj['relative_key']:
				archive_path = f'objects/{obj["relative_key"]}'
			else:
				archive_path = f'objects/{object_name}'

			if os.path.exists(local_path):
				total_original_size += os.path.getsize(local_path)
				files_to_archive.append(
					{
						'source_path': local_path,
						'archive_path': archive_path,
					}
				)

		# Add manifest
		if os.path.exists(manifest_path):
			total_original_size += os.path.getsize(manifest_path)
			files_to_archive.append({'source_path': manifest_path, 'archive_path': 'manifest.json'})
		else:
			logger.error(f'Manifest file not found: {manifest_path}')
			return False, '', 0, 0, 0

		# Create TAR archive
		tar_success, tar_path = create_tar_archive(files_to_archive, output_dir, temp_dir)
		if not tar_success:
			return False, '', 0, 0, 0

		# Compress TAR with zstd, passing source bucket, prefix, DDB Item Key name, targets and file count
		output_path = os.path.join(output_dir, 'archive.tar.zst')
		compress_success, _, compressed_size, compression_level = compress_tar_with_zstd(
			tar_path,
			output_path,
			source_bucket,
			source_prefix,
			ddb_key_name=ddb_key_name,
			targets=targets,
			file_count=file_count,
		)

		if compress_success:
			return True, output_path, total_original_size, compressed_size, compression_level
		else:
			return False, '', 0, 0, 0

	finally:
		# Clean up temporary directory
		try:
			shutil.rmtree(temp_dir)
		except Exception as e:
			logger.warning(f'Error cleaning up temporary directory: {e}')


def create_temp_directory() -> str:
	"""
	Create a temporary directory for processing files.
	Enhanced for non-root user execution.

	Returns:
	    Path to the temporary directory
	"""
	try:
		# Use TMPDIR environment variable if set, otherwise use default
		temp_base = os.environ.get('TMPDIR', tempfile.gettempdir())
		temp_dir = tempfile.mkdtemp(dir=temp_base)
		
		# Ensure the directory is writable
		test_file = os.path.join(temp_dir, '.write_test')
		with open(test_file, 'w') as f:
			f.write('test')
		os.remove(test_file)
		
		logger.debug(f"Created temporary directory: {temp_dir}")
		return temp_dir
	except Exception as e:
		logger.error(f"Failed to create temporary directory: {e}")
		# Try alternative location
		try:
			alt_temp = os.path.join('/tmp/app-work', f'temp_{os.getpid()}_{int(time.time())}')
			os.makedirs(alt_temp, mode=0o755, exist_ok=True)
			logger.info(f"Created alternative temporary directory: {alt_temp}")
			return alt_temp
		except Exception as alt_e:
			logger.error(f"Failed to create alternative temp directory: {alt_e}")
			raise RuntimeError(f"Cannot create temporary directory: {e}, {alt_e}")


def cleanup_temp_directory(temp_dir: str) -> bool:
	"""
	Clean up a temporary directory.

	Args:
	    temp_dir: Path to the temporary directory

	Returns:
	    True if successful, False otherwise
	"""
	try:
		shutil.rmtree(temp_dir)
		return True
	except Exception as e:
		logger.error(f'Error cleaning up temporary directory: {e}')
		return False
