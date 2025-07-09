"""
Unit tests for the manifest module in target_region.
"""

import os

# Import the module under test
from bin.target_region.utils.manifest import (
	read_manifest_from_file,
	get_object_paths_from_manifest,
	prepare_object_tags,
)


class TestManifestReading:
	"""Tests for manifest file reading."""

	def test_read_manifest_from_file(self, test_manifest_file, test_manifest_data):
		"""Test reading a valid manifest file."""
		# Given: A valid manifest file

		# When: We read the manifest file
		manifest = read_manifest_from_file(test_manifest_file)

		# Then: The manifest should be loaded correctly
		assert manifest is not None
		assert manifest['format_version'] == test_manifest_data['format_version']
		assert manifest['source_bucket'] == test_manifest_data['source_bucket']
		assert len(manifest['objects']) == len(test_manifest_data['objects'])
		assert len(manifest['targets']) == len(test_manifest_data['targets'])

	def test_read_manifest_from_file_nonexistent(self, temp_directory):
		"""Test reading a nonexistent manifest file."""
		# Given: A nonexistent file path
		nonexistent_file = os.path.join(temp_directory, 'nonexistent.json')

		# When: We try to read the nonexistent file
		manifest = read_manifest_from_file(nonexistent_file)

		# Then: The function should handle the error and return None
		assert manifest is None

	def test_read_manifest_from_file_invalid_json(self, temp_directory):
		"""Test reading an invalid JSON file."""
		# Given: A file with invalid JSON
		invalid_json_file = os.path.join(temp_directory, 'invalid.json')
		with open(invalid_json_file, 'w') as f:
			f.write('not valid json { missing : quotes')

		# When: We try to read the invalid file
		manifest = read_manifest_from_file(invalid_json_file)

		# Then: The function should handle the error and return None
		assert manifest is None


class TestObjectPathsExtraction:
	"""Tests for object paths extraction from manifest."""

	def test_get_object_paths_from_manifest(self, test_manifest_data, temp_directory):
		"""Test extracting object paths from a valid manifest."""
		# Given: A valid manifest with objects and targets
		extract_dir = os.path.join(temp_directory, 'extract_dir')
		os.makedirs(extract_dir, exist_ok=True)

		# When: We get object paths from the manifest
		object_paths = get_object_paths_from_manifest(test_manifest_data, extract_dir)

		# Then: We should get information for all objects
		assert len(object_paths) == 2

		# Check first object
		obj1 = object_paths[0]
		assert obj1['object_name'] == 'test_file1.txt'
		assert obj1['relative_key'] == 'file1.txt'
		assert obj1['source_bucket'] == 'test-source-bucket'
		assert obj1['source_prefix'] == 'test'
		assert obj1['storage_class'] == 'STANDARD'
		assert obj1['size'] == 1024
		assert len(obj1['tags']) == 2

		# Verify that targets were included from the manifest
		assert 'targets' in obj1
		assert len(obj1['targets']) == 2
		assert obj1['targets'][0]['region'] == 'us-east-1'
		assert obj1['targets'][1]['region'] == 'us-west-2'

		# Check expected path
		assert obj1['local_path'] == os.path.join(extract_dir, 'objects', 'file1.txt')

	def test_get_object_paths_from_manifest_empty_objects(self, temp_directory):
		"""Test handling a manifest with no objects."""
		# Given: A manifest with no objects
		manifest = {
			'format_version': '1.0',
			'source_bucket': 'test-bucket',
			'source_prefix': 'test',
			'objects': [],
			'targets': [{'region': 'us-east-1', 'bucket': 'test-target-bucket'}],
		}

		extract_dir = os.path.join(temp_directory, 'extract_dir')
		os.makedirs(extract_dir, exist_ok=True)

		# When: We get object paths from the manifest
		object_paths = get_object_paths_from_manifest(manifest, extract_dir)

		# Then: We should get an empty list
		assert object_paths == []

	def test_get_object_paths_from_manifest_missing_targets(self, temp_directory):
		"""Test handling a manifest with missing targets."""
		# Given: A manifest with objects but no targets
		manifest = {
			'format_version': '1.0',
			'source_bucket': 'test-bucket',
			'source_prefix': 'test',
			'objects': [
				{
					'object_name': 'test.txt',
					'relative_key': 'test.txt',
					'source_bucket': 'test-bucket',
					'source_prefix': 'test',
					'size': 1024,
					'storage_class': 'STANDARD',
					'tags': [],
				}
			],
			# No targets field
		}

		extract_dir = os.path.join(temp_directory, 'extract_dir')
		os.makedirs(extract_dir, exist_ok=True)

		# When: We get object paths from the manifest
		object_paths = get_object_paths_from_manifest(manifest, extract_dir)

		# Then: We should get an empty list since targets are required
		assert object_paths == []

	def test_get_object_paths_from_manifest_missing_name(self, temp_directory):
		"""Test handling objects with missing name in manifest."""
		# Given: A manifest with an object missing name
		manifest = {
			'format_version': '1.0',
			'source_bucket': 'test-bucket',
			'source_prefix': 'test',
			'objects': [
				{
					# Missing object_name
					'relative_key': 'test.txt',
					'source_bucket': 'test-bucket',
					'source_prefix': 'test',
					'size': 1024,
				}
			],
			'targets': [{'region': 'us-east-1', 'bucket': 'test-target-bucket'}],
		}

		extract_dir = os.path.join(temp_directory, 'extract_dir')
		os.makedirs(extract_dir, exist_ok=True)

		# When: We get object paths from the manifest
		object_paths = get_object_paths_from_manifest(manifest, extract_dir)

		# Then: We should get an empty list since object_name is required
		assert object_paths == []


class TestTagPreparation:
	"""Tests for tag preparation."""

	def test_prepare_object_tags_with_tags(self):
		"""Test preparing tags from object info with existing tags."""
		# Given: Object info with tags, creation time, and ETag
		object_info = {
			'object_name': 'test.txt',
			'creation_time': '2023-01-01T12:00:00Z',
			'etag': '"1234567890abcdef"',
			'tags': [{'Purpose': 'Testing'}, {'Environment': 'Dev'}],
		}

		# When: We prepare the tags
		tags = prepare_object_tags(object_info)

		# Then: The result should include original tags plus added metadata tags
		assert len(tags) == 4
		assert tags['Purpose'] == 'Testing'
		assert tags['Environment'] == 'Dev'
		assert tags['OriginalCreationTime'] == '2023-01-01T12:00:00Z'
		assert tags['OriginalETag'] == '"1234567890abcdef"'

	def test_prepare_object_tags_without_tags(self):
		"""Test preparing tags from object info without existing tags."""
		# Given: Object info with no tags but with creation time and ETag
		object_info = {
			'object_name': 'test.txt',
			'creation_time': '2023-01-01T12:00:00Z',
			'etag': '"1234567890abcdef"',
			'tags': [],
		}

		# When: We prepare the tags
		tags = prepare_object_tags(object_info)

		# Then: The result should include only the metadata tags
		assert len(tags) == 2
		assert tags['OriginalCreationTime'] == '2023-01-01T12:00:00Z'
		assert tags['OriginalETag'] == '"1234567890abcdef"'

	def test_prepare_object_tags_without_metadata(self):
		"""Test preparing tags from object info without metadata."""
		# Given: Object info with tags but no creation time or ETag
		object_info = {
			'object_name': 'test.txt',
			'tags': [{'Purpose': 'Testing'}, {'Environment': 'Dev'}],
			# No creation_time or etag
		}

		# When: We prepare the tags
		tags = prepare_object_tags(object_info)

		# Then: The result should include only the original tags
		assert len(tags) == 2
		assert tags['Purpose'] == 'Testing'
		assert tags['Environment'] == 'Dev'
		assert 'OriginalCreationTime' not in tags
		assert 'OriginalETag' not in tags

	def test_prepare_object_tags_empty_object(self):
		"""Test preparing tags from an empty object info."""
		# Given: An empty object info
		object_info = {}

		# When: We prepare the tags
		tags = prepare_object_tags(object_info)

		# Then: The result should be an empty dictionary
		assert tags == {}
