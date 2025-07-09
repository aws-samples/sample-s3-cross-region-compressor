from s3_cross_region_compressor.utils.config_utils import detect_replication_loops

# Test case 1: Different prefixes - should NOT detect a loop
no_loop_config = {
    "replication_configuration": [
        {
            "source": {
                "region": "us-west-2",
                "bucket": "s3-crc-oregon",
                "prefix_filter": "historic"
            },
            "destinations": [
                {
                    "region": "ca-central-1",
                    "bucket": "s3-crc-canada",
                    "storage_class": "GLACIER"
                }
            ]
        },
        {
            "source": {
                "region": "ca-central-1",
                "bucket": "s3-crc-canada",
                "prefix_filter": "CanadaBackup"
            },
            "destinations": [
                {
                    "region": "us-west-2",
                    "bucket": "s3-crc-oregon",
                    "storage_class": "STANDARD"
                }
            ]
        }
    ]
}

# Test case 2: Same prefixes - should detect a loop
loop_config = {
    "replication_configuration": [
        {
            "source": {
                "region": "us-west-2",
                "bucket": "s3-crc-oregon",
                "prefix_filter": "shared"
            },
            "destinations": [
                {
                    "region": "ca-central-1",
                    "bucket": "s3-crc-canada",
                    "storage_class": "GLACIER"
                }
            ]
        },
        {
            "source": {
                "region": "ca-central-1",
                "bucket": "s3-crc-canada",
                "prefix_filter": "shared"
            },
            "destinations": [
                {
                    "region": "us-west-2",
                    "bucket": "s3-crc-oregon",
                    "storage_class": "STANDARD"
                }
            ]
        }
    ]
}

# Test case 3: Empty prefix and specific prefix - should NOT detect a loop
mixed_prefix_config = {
    "replication_configuration": [
        {
            "source": {
                "region": "us-west-2",
                "bucket": "s3-crc-oregon",
                # No prefix_filter specified
            },
            "destinations": [
                {
                    "region": "ca-central-1",
                    "bucket": "s3-crc-canada",
                    "storage_class": "GLACIER"
                }
            ]
        },
        {
            "source": {
                "region": "ca-central-1",
                "bucket": "s3-crc-canada",
                "prefix_filter": "CanadaBackup"
            },
            "destinations": [
                {
                    "region": "us-west-2",
                    "bucket": "s3-crc-oregon",
                    "storage_class": "STANDARD"
                }
            ]
        }
    ]
}

print("Testing different prefixes (should NOT detect a loop):")
has_loop = detect_replication_loops(no_loop_config)
print(f"Loop detected: {has_loop} (Expected: False)")

print("\nTesting same prefixes (should detect a loop):")
has_loop = detect_replication_loops(loop_config)
print(f"Loop detected: {has_loop} (Expected: True)")

print("\nTesting empty prefix vs specific prefix (should detect a loop):")
has_loop = detect_replication_loops(mixed_prefix_config)
print(f"Loop detected: {has_loop} (Expected: True)")
