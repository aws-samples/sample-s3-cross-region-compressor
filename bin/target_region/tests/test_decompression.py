"""
Unit tests for the decompression module in target_region.
"""

import os
import tarfile
import tempfile
from unittest.mock import patch, MagicMock

# Import the module under test
from bin.target_region.utils.decompression import (
	get_available_memory,
	calculate_buffer_sizes,
	create_temp_directory,
	cleanup_temp_directory,
	decompress_zstd_file,
	extract_manifest_only,
	stream_extract_file,
	get_tar_members,
	decompress_and_extract,
)


class TestMemoryManagement:
	"""Tests for memory detection and buffer size calculation."""

	def test_get_available_memory(self):
		"""Test detecting available memory."""
		# Given: A system with memory
		with patch('bin.target_region.utils.decompression.psutil.virtual_memory') as mock_vm:
			# Configure the mock to return a known memory value
			mock_memory = MagicMock()
			mock_memory.available = 8 * (1024**3)  # 8 GB
			mock_vm.return_value = mock_memory

			# When: We get the available memory
			memory = get_available_memory()

			# Then: We should get the expected amount
			assert memory == 8 * (1024**3)

	def test_get_available_memory_fallback(self):
		"""Test detecting memory with fallback on error."""
		# Given: An error occurs during memory detection
		with patch('bin.target_region.utils.decompression.psutil.virtual_memory') as mock_vm:
			# Configure the mock to raise an exception
			mock_vm.side_effect = Exception('Memory detection failed')

			# When: We get the available memory
			memory = get_available_memory()

			# Then: We should get the fallback amount
			assert memory == 2 * (1024**3)  # 2 GB fallback

	def test_calculate_buffer_sizes(self):
		"""Test calculating optimal buffer sizes."""
		# Given: An available memory amount
		available_memory = 8 * (1024**3)  # 8 GB

		# When: We calculate buffer sizes
		read_size, write_size = calculate_buffer_sizes(available_memory)

		# Then: We should get the expected proportions
		assert read_size == int(available_memory * 0.15 * 0.25)  # 15% of memory * 25% for read
		assert write_size == int(available_memory * 0.15 * 0.75)  # 15% of memory * 75% for write
		assert read_size + write_size == int(available_memory * 0.15)  # Total is 15% of memory


class TestTemporaryDirectories:
	"""Tests for temporary directory functions."""

	def test_create_temp_directory(self):
		"""Test creating a temporary directory."""
		# Given: A patched tempfile.mkdtemp
		with patch('bin.target_region.utils.decompression.tempfile.mkdtemp') as mock_mkdtemp:
			mock_mkdtemp.return_value = '/tmp/test-dir'

			# When: We create a temporary directory
			temp_dir = create_temp_directory()

			# Then: We should get the path from mkdtemp
			assert temp_dir == '/tmp/test-dir'
			mock_mkdtemp.assert_called_once()

	def test_cleanup_temp_directory(self):
		"""Test cleaning up a temporary directory."""
		# Given: A temporary directory
		#temp_dir = tempfile.mkdtemp()
		#solving B108
		
		with tempfile.TemporaryDirectory() as temp_dir:
		# Create a test file in the directory
			test_file = os.path.join(temp_dir, 'test.txt')
			with open(test_file, 'w') as f:
				f.write('test content')

		# When: We clean up the directory
			result = cleanup_temp_directory(temp_dir)

		# Then: The directory should be removed and the function should return True
			assert result is True
			assert not os.path.exists(temp_dir)

	def test_cleanup_temp_directory_error(self):
		"""Test handling errors during directory cleanup."""
		# Given: A non-existent directory
		temp_dir = '/nonexistent/directory'

		# When: We try to clean up the non-existent directory
		with patch('bin.target_region.utils.decompression.shutil.rmtree', side_effect=Exception('Cleanup failed')):
			result = cleanup_temp_directory(temp_dir)

		# Then: The function should handle the error and return False
		assert result is False


class TestDecompressionFunctions:
	"""Tests for decompression functions."""

	def test_decompress_zstd_file(self, temp_directory, mock_decompress_stream):
		"""Test decompressing a zstd file."""
		# Given: A compressed file
		input_path = os.path.join(temp_directory, 'test.zstd')
		output_path = os.path.join(temp_directory, 'test.tar')

		# Create a test compressed file (just a dummy file for testing)
		with open(input_path, 'wb') as f:
			f.write(b'compressed content')

		# When: We decompress the file
		success, compressed_size, decompressed_size = decompress_zstd_file(input_path, output_path)

		# Then: The operation should be successful
		assert success is True
		assert compressed_size > 0
		assert decompressed_size > 0
		assert os.path.exists(output_path)
		with open(output_path, 'rb') as f:
			assert f.read() == b'compressed content'  # Our mock simply copies the content

	def test_decompress_zstd_file_error(self, temp_directory):
		"""Test handling errors during decompression."""
		# Given: A non-existent input file
		input_path = os.path.join(temp_directory, 'nonexistent.zstd')
		output_path = os.path.join(temp_directory, 'output.tar')

		# When: We try to decompress the non-existent file
		success, compressed_size, decompressed_size = decompress_zstd_file(input_path, output_path)

		# Then: The function should handle the error and return False
		assert success is False
		assert compressed_size == 0
		assert decompressed_size == 0
		assert not os.path.exists(output_path)

	def test_decompress_zstd_file_pyzstd_error(self, temp_directory):
		"""Test handling pyzstd errors during decompression."""
		# Given: A compressed file but pyzstd raises an error
		input_path = os.path.join(temp_directory, 'test.zstd')
		output_path = os.path.join(temp_directory, 'test.tar')

		# Create a test compressed file
		with open(input_path, 'wb') as f:
			f.write(b'compressed content')

		# When: We try to decompress with pyzstd raising an error
		with patch(
			'bin.target_region.utils.decompression.pyzstd.decompress_stream',
			side_effect=Exception('Decompression failed'),
		):
			success, compressed_size, decompressed_size = decompress_zstd_file(input_path, output_path)

		# Then: The function should handle the error and return False
		assert success is False
		assert compressed_size == 0
		assert decompressed_size == 0


class TestTarOperations:
	"""Tests for TAR archive operations."""

	def test_extract_manifest_only(self, create_test_archive):
		"""Test extracting just the manifest from a TAR archive."""
		# Given: A TAR archive with a manifest
		tar_path = create_test_archive['tar_path']
		extract_dir = os.path.join(os.path.dirname(tar_path), 'manifest_extraction')
		os.makedirs(extract_dir, exist_ok=True)

		# When: We extract only the manifest
		success, manifest_path = extract_manifest_only(tar_path, extract_dir)

		# Then: The operation should be successful
		assert success is True
		assert os.path.exists(manifest_path)
		assert os.path.basename(manifest_path) == 'manifest.json'

		# Verify only the manifest was extracted
		extracted_files = os.listdir(extract_dir)
		assert len(extracted_files) == 1
		assert 'manifest.json' in extracted_files

	def test_extract_manifest_only_no_manifest(self, temp_directory):
		"""Test extracting manifest when it doesn't exist in the archive."""
		# Given: A TAR archive without a manifest
		tar_path = os.path.join(temp_directory, 'no_manifest.tar')
		with tarfile.open(tar_path, 'w') as tar:
			# Add some other file but not manifest.json
			dummy_file = os.path.join(temp_directory, 'dummy.txt')
			with open(dummy_file, 'w') as f:
				f.write('dummy content')
			tar.add(dummy_file, arcname='dummy.txt')

		extract_dir = os.path.join(temp_directory, 'extract')
		os.makedirs(extract_dir, exist_ok=True)

		# When: We try to extract the manifest
		success, manifest_path = extract_manifest_only(tar_path, extract_dir)

		# Then: The operation should fail
		assert success is False
		assert manifest_path == ''

	def test_stream_extract_file(self, create_test_archive):
		"""Test streaming extraction of a single file from TAR."""
		# Given: A TAR archive with files
		tar_path = create_test_archive['tar_path']
		extract_dir = os.path.join(os.path.dirname(tar_path), 'file_extraction')
		os.makedirs(extract_dir, exist_ok=True)

		# When: We extract a specific file
		success = stream_extract_file(tar_path, 'objects/file1.txt', extract_dir)

		# Then: The operation should be successful
		assert success is True
		extracted_path = os.path.join(extract_dir, 'objects/file1.txt')
		assert os.path.exists(extracted_path)

		# Check that only one file was extracted
		assert not os.path.exists(os.path.join(extract_dir, 'objects/file2.txt'))
		assert not os.path.exists(os.path.join(extract_dir, 'manifest.json'))

	def test_stream_extract_file_nonexistent(self, create_test_archive):
		"""Test streaming extraction of a file that doesn't exist in the TAR."""
		# Given: A TAR archive
		tar_path = create_test_archive['tar_path']
		extract_dir = os.path.join(os.path.dirname(tar_path), 'file_extraction')
		os.makedirs(extract_dir, exist_ok=True)

		# When: We try to extract a non-existent file
		success = stream_extract_file(tar_path, 'objects/nonexistent.txt', extract_dir)

		# Then: The operation should fail
		assert success is False
		assert not os.path.exists(os.path.join(extract_dir, 'objects/nonexistent.txt'))

	def test_get_tar_members(self, create_test_archive):
		"""Test getting member list from a TAR archive."""
		# Given: A TAR archive with files
		tar_path = create_test_archive['tar_path']

		# When: We get the member list
		members = get_tar_members(tar_path)

		# Then: We should get all the files in the archive
		assert len(members) == 3
		assert 'objects/file1.txt' in members
		assert 'objects/file2.txt' in members
		assert 'manifest.json' in members

	def test_get_tar_members_invalid_tar(self, temp_directory):
		"""Test handling an invalid TAR file."""
		# Given: An invalid TAR file
		invalid_tar = os.path.join(temp_directory, 'invalid.tar')
		with open(invalid_tar, 'wb') as f:
			f.write(b'not a tar file')

		# When: We try to get member list
		members = get_tar_members(invalid_tar)

		# Then: We should get an empty list due to error handling
		assert members == []


class TestFullDecompression:
	"""Tests for complete decompression process."""

	def test_decompress_and_extract(self, create_test_archive, mock_decompress_stream):
		"""Test decompressing and extracting an archive."""
		# Given: A compressed archive
		compressed_path = create_test_archive['compressed_path']
		temp_dir = os.path.dirname(compressed_path)

		# When: We decompress and extract the archive
		success, extract_dir, compressed_size, decompressed_size = decompress_and_extract(compressed_path, temp_dir)

		# Then: The operation should be successful
		assert success is True
		assert os.path.exists(extract_dir)

		# The manifest should be extracted
		manifest_path = os.path.join(extract_dir, 'manifest.json')
		assert os.path.exists(manifest_path)

		# The uncompressed TAR should exist for later streaming extraction
		tar_path = os.path.join(temp_dir, 'archive.tar')
		assert os.path.exists(tar_path)

	def test_decompress_and_extract_decompress_failure(self, temp_directory):
		"""Test handling decompression failure during extract."""
		# Given: A mock compressed file that will fail decompression
		compressed_path = os.path.join(temp_directory, 'will_fail.tar.zstd')
		with open(compressed_path, 'wb') as f:
			f.write(b'invalid compressed data')

		# When: We try to decompress with a failing mock
		with patch('bin.target_region.utils.decompression.decompress_zstd_file', return_value=(False, 0, 0)):
			success, extract_dir, compressed_size, decompressed_size = decompress_and_extract(
				compressed_path, temp_directory
			)

		# Then: The operation should fail
		assert success is False
		assert extract_dir == ''
		assert compressed_size == 0
		assert decompressed_size == 0

	def test_decompress_and_extract_manifest_failure(self, temp_directory):
		"""Test handling manifest extraction failure."""
		# Given: A compressed file and a mocked decompression that succeeds but manifest extraction fails
		compressed_path = os.path.join(temp_directory, 'test.tar.zstd')
		with open(compressed_path, 'wb') as f:
			f.write(b'test content')

		# Mock decompress_zstd_file to succeed but extract_manifest_only to fail
		with (
			patch('bin.target_region.utils.decompression.decompress_zstd_file', return_value=(True, 100, 200)),
			patch('bin.target_region.utils.decompression.extract_manifest_only', return_value=(False, '')),
		):
			# When: We try to decompress and extract
			success, extract_dir, compressed_size, decompressed_size = decompress_and_extract(
				compressed_path, temp_directory
			)

		# Then: The operation should fail
		assert success is False
		assert extract_dir == ''
		assert compressed_size == 0
		assert decompressed_size == 0
