"""
Unit tests for the compression module.
"""

import os
import tarfile
import tempfile
from unittest.mock import patch, MagicMock

# Import the module under test
from bin.source_region.utils.compression import (
	get_available_memory,
	calculate_buffer_sizes,
	create_tar_archive,
	compress_tar_with_zstd,
	compress_objects,
	create_temp_directory,
	cleanup_temp_directory,
)


class TestMemoryManagement:
	"""Tests for memory detection and buffer size calculation."""

	def test_get_available_memory(self):
		"""Test detecting available memory."""
		# Given: A mocked psutil.virtual_memory
		mock_memory = MagicMock()
		mock_memory.available = 8 * (1024**3)  # 8GB available

		# When: We get available memory
		with patch('psutil.virtual_memory', return_value=mock_memory):
			memory = get_available_memory()

		# Then: We should get the correct amount
		assert memory == 8 * (1024**3)

	def test_get_available_memory_error(self):
		"""Test handling errors when detecting available memory."""
		# Given: An error occurs when calling psutil.virtual_memory

		# When: We get available memory with an error
		with patch('psutil.virtual_memory', side_effect=Exception('Test error')):
			memory = get_available_memory()

		# Then: We should get the default fallback value (2GB)
		assert memory == 2 * (1024**3)

	def test_calculate_buffer_sizes(self):
		"""Test calculating optimal buffer sizes based on available memory."""
		# Given: Available memory
		available_memory = 8 * (1024**3)  # 8GB available

		# When: We calculate buffer sizes
		read_size, write_size = calculate_buffer_sizes(available_memory)

		# Then: We should get the correct buffer sizes
		# 15% of available memory, split 45/55 for read/write
		expected_total = available_memory * 0.15
		expected_read = int(expected_total * 0.45)
		expected_write = int(expected_total * 0.55)

		assert read_size == expected_read
		assert write_size == expected_write


class TestTemporaryDirectories:
	"""Tests for temporary directory management."""

	def test_create_temp_directory(self):
		"""Test creating a temporary directory."""
		# When: We create a temporary directory
		with patch('tempfile.mkdtemp') as mock_mkdtemp:
			mock_mkdtemp.return_value = '/tmp/test-dir'
			temp_dir = create_temp_directory()

		# Then: We should get the created directory path
		assert temp_dir == '/tmp/test-dir'
		mock_mkdtemp.assert_called_once()

	def test_cleanup_temp_directory_success(self):
		"""Test cleaning up a temporary directory successfully."""
		# Given: A temporary directory
		#temp_dir = tempfile.mkdtemp()
		with tempfile.TemporaryDirectory() as temp_dir:
		
		# When: We clean up the directory
			result = cleanup_temp_directory(temp_dir)

		# Then: The cleanup should be successful
			assert result is True
			assert not os.path.exists(temp_dir)

	def test_cleanup_temp_directory_error(self):
		"""Test handling errors when cleaning up a temporary directory."""
		# Given: A nonexistent directory
		temp_dir = '/tmp/nonexistent-dir'

		# When: We try to clean up the directory and an error occurs
		with patch('shutil.rmtree', side_effect=Exception('Test error')):
			result = cleanup_temp_directory(temp_dir)

		# Then: The function should handle the error and return False
		assert result is False


class TestTarArchive:
	"""Tests for TAR archive creation."""

	def test_create_tar_archive(self, temp_directory):
		"""Test creating a TAR archive with multiple files."""
		# Given: Files to archive
		file1_path = os.path.join(temp_directory, 'file1.txt')
		with open(file1_path, 'w') as f:
			f.write('Test content for file 1')

		file2_path = os.path.join(temp_directory, 'file2.txt')
		with open(file2_path, 'w') as f:
			f.write('Test content for file 2')

		manifest_path = os.path.join(temp_directory, 'manifest.json')
		with open(manifest_path, 'w') as f:
			f.write('{"test": "manifest"}')

		files_to_archive = [
			{'source_path': file1_path, 'archive_path': 'objects/file1.txt'},
			{'source_path': file2_path, 'archive_path': 'objects/file2.txt'},
			{'source_path': manifest_path, 'archive_path': 'manifest.json'},
		]

		output_dir = os.path.join(temp_directory, 'output')
		os.makedirs(output_dir, exist_ok=True)

		# When: We create a TAR archive
		success, tar_path = create_tar_archive(files_to_archive, output_dir, temp_directory)

		# Then: The archive should be created successfully
		assert success is True
		assert os.path.exists(tar_path)

		# Verify the tar contents
		with tarfile.open(tar_path, 'r') as tar:
			members = tar.getnames()
			assert 'objects/file1.txt' in members
			assert 'objects/file2.txt' in members
			assert 'manifest.json' in members

			# Extract and check content
			extracted = tar.extractfile('objects/file1.txt')
			assert extracted.read() == b'Test content for file 1'

			extracted = tar.extractfile('manifest.json')
			assert extracted.read() == b'{"test": "manifest"}'

		# The original files should still exist, except for the non-manifest files
		assert not os.path.exists(file1_path)
		assert not os.path.exists(file2_path)
		assert os.path.exists(manifest_path)  # Manifest file should not be deleted

	def test_create_tar_archive_nonexistent_file(self, temp_directory):
		"""Test creating a TAR archive with nonexistent files."""
		# Given: A list of files including a nonexistent one
		file1_path = os.path.join(temp_directory, 'file1.txt')
		with open(file1_path, 'w') as f:
			f.write('Test content for file 1')

		nonexistent_path = os.path.join(temp_directory, 'nonexistent.txt')

		files_to_archive = [
			{'source_path': file1_path, 'archive_path': 'objects/file1.txt'},
			{'source_path': nonexistent_path, 'archive_path': 'objects/nonexistent.txt'},
		]

		output_dir = os.path.join(temp_directory, 'output')
		os.makedirs(output_dir, exist_ok=True)

		# When: We create a TAR archive
		success, tar_path = create_tar_archive(files_to_archive, output_dir, temp_directory)

		# Then: The archive should be created with only the existing file
		assert success is True
		assert os.path.exists(tar_path)

		# Verify the tar contents
		with tarfile.open(tar_path, 'r') as tar:
			members = tar.getnames()
			assert 'objects/file1.txt' in members
			assert 'objects/nonexistent.txt' not in members

	def test_create_tar_archive_error(self, temp_directory):
		"""Test handling errors when creating a TAR archive."""
		# Given: Files to archive
		file1_path = os.path.join(temp_directory, 'file1.txt')
		with open(file1_path, 'w') as f:
			f.write('Test content for file 1')

		files_to_archive = [{'source_path': file1_path, 'archive_path': 'objects/file1.txt'}]

		# When: An error occurs during tar creation
		with patch('tarfile.open', side_effect=Exception('Test error')):
			success, tar_path = create_tar_archive(files_to_archive, temp_directory, temp_directory)

		# Then: The function should handle the error and return False
		assert success is False
		assert tar_path == ''


class TestZstdCompression:
	"""Tests for ZSTD compression of TAR archives."""

	def test_compress_tar_with_zstd(self, temp_directory):
		"""Test compressing a TAR file with ZSTD."""
		# Given: A TAR file
		tar_path = os.path.join(temp_directory, 'archive.tar')
		with open(tar_path, 'wb') as f:
			f.write(b'dummy tar content' * 100)  # Create some content

		original_size = os.path.getsize(tar_path)
		output_path = os.path.join(temp_directory, 'archive.tar.zst')

		# When: We compress the TAR file with ZSTD
		with patch('pyzstd.compress_stream') as mock_compress:
			# Mock compress_stream to simulate compression
			compressed_size = original_size // 2
			mock_compress.return_value = (original_size, compressed_size)

			# Mock the function to return compressed size
			mock_output_size = original_size // 2

			# Create a proper mock implementation that returns values and writes to the file
			def mock_compress_impl(f_in, f_out, **kwargs):
				f_out.write(b'compressed content')
				return original_size, mock_output_size

			mock_compress.side_effect = mock_compress_impl

			success, orig_size, comp_size, level = compress_tar_with_zstd(
				tar_path, output_path, source_bucket='test-bucket', source_prefix='test', level=12, threads=4
			)

		# Then: The compression should be successful
		assert success is True
		assert orig_size == original_size
		assert comp_size > 0
		assert level == 12

		# The original tar file should be deleted after successful compression
		assert not os.path.exists(tar_path)

	def test_compress_tar_with_zstd_adaptive_level(self, temp_directory):
		"""Test compressing a TAR file with adaptive compression level."""
		# Given: A TAR file
		tar_path = os.path.join(temp_directory, 'archive.tar')
		with open(tar_path, 'wb') as f:
			f.write(b'dummy tar content' * 100)

		original_size = os.path.getsize(tar_path)
		output_path = os.path.join(temp_directory, 'archive.tar.zst')

		# Mock the CompressionManager
		mock_manager = MagicMock()
		mock_manager.get_bucket_prefix_key.return_value = 'test-bucket/test/'
		mock_manager.get_compression_level.return_value = 10
		mock_manager.occasionally_test_new_level.return_value = 8

		# When: We compress the TAR file with adaptive level
		with patch('bin.source_region.utils.compression.CompressionManager.get_instance', return_value=mock_manager):
			with patch('pyzstd.compress_stream') as mock_compress:
				# Mock compress_stream to simulate compression
				mock_compress.return_value = (original_size, original_size // 2)

				success, orig_size, comp_size, level = compress_tar_with_zstd(
					tar_path,
					output_path,
					source_bucket='test-bucket',
					source_prefix='test',
					ddb_key_name='test-bucket/test/',
				)

		# Then: The compression should use the level from the manager
		assert success is True
		assert level == 8  # The occasionally_test_new_level returned 8
		mock_manager.get_compression_level.assert_called_once()
		mock_manager.occasionally_test_new_level.assert_called_once()

	def test_compress_tar_with_zstd_error(self, temp_directory):
		"""Test handling errors during ZSTD compression."""
		# Given: A TAR file
		tar_path = os.path.join(temp_directory, 'archive.tar')
		with open(tar_path, 'wb') as f:
			f.write(b'dummy tar content')

		output_path = os.path.join(temp_directory, 'archive.tar.zst')

		# When: An error occurs during compression
		with patch('pyzstd.compress_stream', side_effect=Exception('Test error')):
			success, orig_size, comp_size, level = compress_tar_with_zstd(
				tar_path, output_path, source_bucket='test-bucket', source_prefix='test'
			)

		# Then: The function should handle the error and return False
		assert success is False
		assert orig_size == 0
		assert comp_size == 0
		assert level == 0  # Default level when error occurs

		# The original tar file should still exist
		assert os.path.exists(tar_path)


class TestCompleteCompression:
	"""Tests for the complete compression workflow."""

	def test_compress_objects(self, temp_directory):
		"""Test compressing multiple objects into a single ZSTD-compressed TAR file."""
		# Given: Object paths, manifest, and a temporary directory
		object1_path = os.path.join(temp_directory, 'object1.txt')
		with open(object1_path, 'w') as f:
			f.write('Test content for object 1')

		object2_path = os.path.join(temp_directory, 'object2.txt')
		with open(object2_path, 'w') as f:
			f.write('Test content for object 2')

		manifest_path = os.path.join(temp_directory, 'manifest.json')
		with open(manifest_path, 'w') as f:
			f.write('{"test": "manifest"}')

		object_paths = [
			{'local_path': object1_path, 'object_name': 'object1.txt', 'relative_key': 'test/object1.txt'},
			{'local_path': object2_path, 'object_name': 'object2.txt', 'relative_key': 'test/object2.txt'},
		]

		targets = [{'region': 'us-west-2', 'bucket': 'target-bucket', 'storage_class': 'STANDARD'}]

		# Mock the tar creation and compression functions
		with patch('bin.source_region.utils.compression.create_tar_archive') as mock_tar:
			mock_tar.return_value = (True, os.path.join(temp_directory, 'archive.tar'))

			with patch('bin.source_region.utils.compression.compress_tar_with_zstd') as mock_compress:
				# Mock the compression to return success
				mock_compress.return_value = (True, 1024, 512, 12)

				# When: We compress the objects
				success, output_path, original_size, compressed_size, compression_level = compress_objects(
					object_paths,
					manifest_path,
					temp_directory,
					source_bucket='test-bucket',
					source_prefix='test',
					ddb_key_name='test-bucket/test/',
					targets=targets,
					file_count=2,
				)

		# Then: The compression should be successful
		assert success is True
		assert output_path == os.path.join(temp_directory, 'archive.tar.zst')
		assert original_size > 0
		assert compressed_size == 512
		assert compression_level == 12

		# Verify the calls
		mock_tar.assert_called_once()
		mock_compress.assert_called_once()

	def test_compress_objects_nonexistent_manifest(self, temp_directory):
		"""Test compressing objects with a nonexistent manifest file."""
		# Given: Object paths and a nonexistent manifest
		object_paths = []
		nonexistent_manifest = os.path.join(temp_directory, 'nonexistent.json')

		# When: We try to compress with a nonexistent manifest
		success, output_path, original_size, compressed_size, compression_level = compress_objects(
			object_paths, nonexistent_manifest, temp_directory
		)

		# Then: The compression should fail
		assert success is False
		assert output_path == ''
		assert original_size == 0
		assert compressed_size == 0
		assert compression_level == 0  # Default level when compression fails
		assert compression_level == 0  # Default level when compression fails

	def test_compress_objects_tar_failure(self, temp_directory):
		"""Test handling TAR creation failure during object compression."""
		# Given: Object paths and manifest
		object_paths = []
		manifest_path = os.path.join(temp_directory, 'manifest.json')
		with open(manifest_path, 'w') as f:
			f.write('{"test": "manifest"}')

		# When: TAR creation fails
		with patch('bin.source_region.utils.compression.create_tar_archive') as mock_tar:
			mock_tar.return_value = (False, '')

			success, output_path, original_size, compressed_size, compression_level = compress_objects(
				object_paths, manifest_path, temp_directory
			)

		# Then: The compression should fail
		assert success is False
		assert output_path == ''
		assert original_size == 0
		assert compressed_size == 0

	def test_compress_objects_compression_failure(self, temp_directory):
		"""Test handling compression failure during object compression."""
		# Given: Object paths and manifest
		object_paths = []
		manifest_path = os.path.join(temp_directory, 'manifest.json')
		with open(manifest_path, 'w') as f:
			f.write('{"test": "manifest"}')

		# When: ZSTD compression fails
		with patch('bin.source_region.utils.compression.create_tar_archive') as mock_tar:
			mock_tar.return_value = (True, os.path.join(temp_directory, 'archive.tar'))

			with patch('bin.source_region.utils.compression.compress_tar_with_zstd') as mock_compress:
				mock_compress.return_value = (False, 0, 0, 0)

				success, output_path, original_size, compressed_size, compression_level = compress_objects(
					object_paths, manifest_path, temp_directory
				)

		# Then: The compression should fail
		assert success is False
		assert output_path == ''
		assert original_size == 0
		assert compressed_size == 0

	def test_compress_objects_cleanup(self, temp_directory):
		"""Test temporary directory cleanup during object compression."""
		# Given: Object paths and manifest
		object_paths = []
		manifest_path = os.path.join(temp_directory, 'manifest.json')
		with open(manifest_path, 'w') as f:
			f.write('{"test": "manifest"}')

		# When: We compress objects
		with patch('bin.source_region.utils.compression.create_tar_archive') as mock_tar:
			mock_tar.return_value = (True, os.path.join(temp_directory, 'archive.tar'))

			with patch('bin.source_region.utils.compression.compress_tar_with_zstd') as mock_compress:
				mock_compress.return_value = (True, 1024, 512, 12)

				with patch('shutil.rmtree') as mock_rmtree:
					compress_objects(object_paths, manifest_path, temp_directory)

		# Then: The temporary directory should be cleaned up
		mock_rmtree.assert_called_once()
