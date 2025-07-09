"""
Decompression Utilities for Target Region Container

This module provides utilities for decompressing files using zstd:
- Extract files from zstd-compressed TAR archives
- Manage temporary files and directories
"""

import logging
import os
import shutil
import tarfile
import tempfile
from typing import Tuple

import pyzstd

# For memory detection
import psutil

# Configure logging
logger = logging.getLogger(__name__)

# Constants
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
	# Use up to 15% of available memory for decompression
	max_buffer_memory = available_memory * 0.15

	# 25% for read buffer, 75% for write buffer
	read_size = int(max_buffer_memory * 0.25)
	write_size = int(max_buffer_memory * 0.75)

	logger.info(f'Memory available: {available_memory / 1024 / 1024:.1f}MB')
	logger.info(
		f'Configured decompression buffers: read_size={read_size / 1024 / 1024:.2f}MB, write_size={write_size / 1024 / 1024:.2f}MB'
	)
	return read_size, write_size


# Calculate buffer sizes based on available memory - do this once at module import
AVAILABLE_MEMORY = get_available_memory()
READ_BUFFER_SIZE, WRITE_BUFFER_SIZE = calculate_buffer_sizes(AVAILABLE_MEMORY)
logger.info(
	f'System memory: {AVAILABLE_MEMORY / 1024 / 1024:.1f}MB, allocated for decompression: {AVAILABLE_MEMORY * 0.15 / 1024 / 1024:.1f}MB'
)


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
			import time
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


def decompress_zstd_file(input_path: str, output_path: str, threads: int = MAX_WORKERS) -> Tuple[bool, int, int]:
	"""
	Decompress a zstd-compressed file using streaming decompression with dynamic buffer sizing.
	Uses memory-efficient approach that automatically optimizes buffer sizes based on available memory,
	processing files in chunks rather than loading entire files into memory.

	Args:
	    input_path: Path to input compressed file
	    output_path: Path to output decompressed file
	    threads: Number of threads to use

	Returns:
	    Tuple of (success, compressed_size, decompressed_size)
	"""
	try:
		# Get compressed file size
		compressed_size = os.path.getsize(input_path)

		# Use streaming decompression which processes the file in chunks
		# instead of loading the entire file into memory at once
		with open(input_path, 'rb') as f_in:
			with open(output_path, 'wb') as f_out:
				# Use dynamically sized buffers to optimize memory utilization
				total_input, total_output = pyzstd.decompress_stream(
					f_in,
					f_out,
					read_size=READ_BUFFER_SIZE,  # Dynamically calculated based on available memory
					write_size=WRITE_BUFFER_SIZE,  # Dynamically calculated based on available memory
				)

		# Get decompressed file size
		decompressed_size = os.path.getsize(output_path)

		return True, compressed_size, decompressed_size
	except Exception as e:
		logger.error(f'Error decompressing file {input_path}: {e}')
		return False, 0, 0


def extract_manifest_only(tar_path: str, extract_dir: str) -> Tuple[bool, str]:
	"""
	Extract only the manifest.json file from a TAR archive.

	Args:
	    tar_path: Path to TAR file
	    extract_dir: Directory to extract the manifest to

	Returns:
	    Tuple of (success, manifest_path)
	"""
	try:
		manifest_path = os.path.join(extract_dir, 'manifest.json')

		with tarfile.open(tar_path, 'r') as tar:
			manifest_members = [m for m in tar.getmembers() if m.name == 'manifest.json']
			if not manifest_members:
				logger.error('manifest.json not found in TAR archive')
				return False, ''

			tar.extract(manifest_members[0], path=extract_dir)

		return True, manifest_path
	except Exception as e:
		logger.error(f'Error extracting manifest from TAR archive: {e}')
		return False, ''


def stream_extract_file(tar_path: str, member_name: str, extract_dir: str) -> bool:
	"""
	Extract a single file from TAR archive.

	Args:
	    tar_path: Path to TAR file
	    member_name: Name of the file to extract
	    extract_dir: Directory to extract the file to

	Returns:
	    True if successful, False otherwise
	"""
	try:
		with tarfile.open(tar_path, 'r') as tar:
			try:
				member = tar.getmember(member_name)
				tar.extract(member, path=extract_dir)
				return True
			except KeyError:
				logger.error(f'File {member_name} not found in TAR archive')
				return False
	except Exception as e:
		logger.error(f'Error extracting file {member_name} from TAR: {e}')
		return False


def get_tar_members(tar_path: str) -> list:
	"""
	Get list of all file members in a TAR archive.

	Args:
	    tar_path: Path to TAR file

	Returns:
	    List of member names in the TAR archive
	"""
	try:
		with tarfile.open(tar_path, 'r') as tar:
			members = [m.name for m in tar.getmembers() if not m.isdir()]
			return members
	except Exception as e:
		logger.error(f'Error getting TAR members: {e}')
		return []


def decompress_and_extract(compressed_path: str, temp_dir: str) -> Tuple[bool, str, int, int]:
	"""
	Decompress a zstd-compressed TAR file and extract its contents.
	Uses streaming extraction to reduce memory usage.

	Args:
	    compressed_path: Path to compressed TAR.ZSTD file
	    temp_dir: Temporary directory for processing

	Returns:
	    Tuple of (success, extract_dir, compressed_size, decompressed_size)
	"""
	try:
		# Create a temporary file for the decompressed TAR
		tar_path = os.path.join(temp_dir, 'archive.tar')

		# Decompress the ZSTD file
		success, compressed_size, decompressed_size = decompress_zstd_file(compressed_path, tar_path)
		if not success:
			return False, '', 0, 0

		# Create a directory for extracted files
		extract_dir = os.path.join(temp_dir, 'extracted')
		os.makedirs(extract_dir, exist_ok=True)

		# Extract only the manifest file first
		manifest_success, manifest_path = extract_manifest_only(tar_path, extract_dir)
		if not manifest_success:
			logger.error('Failed to extract manifest from TAR archive')
			return False, '', 0, 0

		logger.debug(f'Successfully extracted manifest: {manifest_path}')

		# Get list of all files in the archive for future streaming extraction
		# We don't extract them now, but ensure the tar file is retained for
		# later streaming extraction in the server process
		members = get_tar_members(tar_path)
		object_members = [m for m in members if m != 'manifest.json']
		logger.debug(f'TAR archive contains {len(object_members)} object files for streaming extraction')

		# We now keep the TAR file for streaming extraction in the server process
		# Each file will be extracted on demand, reducing memory usage

		return True, extract_dir, compressed_size, decompressed_size
	except Exception as e:
		logger.error(f'Error in decompress_and_extract: {e}')
		# Clean up TAR file on error
		if os.path.exists(tar_path):
			try:
				os.remove(tar_path)
			except Exception:
				pass
		return False, '', 0, 0
