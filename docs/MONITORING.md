# Monitoring Guide

This guide covers the monitoring capabilities of the S3 Cross-Region Compressor system, including CloudWatch metrics, logging, CloudWatch alarms, and dashboard configuration.

## CloudWatch Metrics

The system publishes comprehensive metrics to CloudWatch under a dedicated namespace, named after the `stack_name` in the `settings.json` file. These metrics provide insights into performance, cost savings, and operational health.

The Source and Target region applications implements an [Embedded Metrics strategy (EMF)](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html) to heavily optimize costs on putMetric operations. This is 
of high important due to the fact that we are tracking over 10 metrics for multiple dimension for potentially billions of S3 objects,
which can rapidly increase CloudWatch Metrics costs.

### Source Region Metrics

| Metric Name | Unit | Description |
|-------------|------|-------------|
| `CompressionRatio` | Ratio | Ratio of original size to compressed size |
| `BytesSaved` | Bytes | Number of bytes saved by compression |
| `CompressionTimeSeconds` | Seconds | Time taken to compress objects |
| `ObjectProcessingTime` | Seconds | Total time to process objects (download, compress, upload) |
| `ObjectSize` | Bytes | Size of original objects processed |
| `FailedDownloads` | Count | Number of failed object downloads |
| `FailedMetadataRetrieval` | Count | Number of failures to retrieve object metadata |
| `FailedUploads` | Count | Number of failed uploads to outbound bucket |
| `TransferSavings` | USD | Estimated cost savings on data transfer |
| `ComputeCost` | USD | Estimated compute cost for compression |
| `NetBenefit` | USD | Net financial benefit (savings minus cost) |
| `BenefitScore` | Score | Weighted benefit score for optimization |

### Target Region Metrics

| Metric Name | Unit | Description |
|-------------|------|-------------|
| `DecompressionRatio` | Ratio | Ratio of compressed size to decompressed size |
| `DecompressionTimeSeconds` | Seconds | Time taken to decompress objects |
| `ObjectRestoreTime` | Seconds | Total time to process objects (download, decompress, upload) |
| `FailedDecompressions` | Count | Number of failed decompressions |
| `FailedTargetUploads` | Count | Number of failed uploads to target buckets |
| `ObjectsProcessed` | Count | Number of objects processed |

## CloudWatch Logs

The system sends detailed logs to CloudWatch Logs, organized in the following log groups:

- `/aws/ecs/{source service name}` - Source region container logs
- `/aws/ecs/{target service name}` - Target region container logs

### Log Structure

The logs use structured JSON format for easier parsing and analysis:

```json
{
  "asctime": "2023-04-01 12:34:56",
  "levelname": "INFO",
  "name": "root",
  "message": "Processing object: my-bucket/path/to/object.txt"
}
```

### Key Log Events

Monitor for these important log messages:

| Log Pattern | Severity | Description |
|-------------|----------|-------------|
| `Successfully processed` | INFO | Successful object processing |
| `Error in processing` | ERROR | General processing error |
| `Failed to download` | ERROR | S3 download failure |
| `Failed to upload` | ERROR | S3 upload failure |
| `Updated optimal level from X to Y` | INFO | Compression optimization change |
| `No target information found` | ERROR | DynamoDB parameter not found |

## CloudWatch Alarms

The system automatically creates critical CloudWatch alarms to monitor the health and performance of your S3 Cross-Region Compressor deployment. These alarms are configured to send notifications to email addresses specified in the `notification_emails` field in your `settings.json` file.

### SNS Notifications

An SNS topic is created in each region with your specified email subscribers. All alarms in that region will notify these email addresses when triggered. The first time alarms are deployed, subscribers will receive a confirmation email that must be accepted to start receiving alarm notifications.

### Preconfigured Alarms

#### DLQ Message Count Alarm

This alarm monitors Dead Letter Queues (DLQs) across your source and target services:

- **Trigger**: When ANY message appears in a DLQ
- **Severity**: High
- **Response**: Immediate notification (evaluation period: 1 minute)
- **Action Required**: Investigate failed messages in the DLQ console and check application logs for processing errors

#### ECS Task Failures Alarm

This alarm detects when ECS tasks are failing to start or terminating prematurely:

- **Trigger**: When desired task count exceeds running task count for 3 consecutive minutes
- **Severity**: High
- **Response**: Notification after 3 minutes of persistent failures
- **Action Required**: Check ECS task logs for startup failures, investigate IAM permissions, resource constraints, or application errors

#### Task Utilization Alarm

This alarm identifies when a service is operating at its maximum capacity for extended periods:

- **Trigger**: When a service's desired task count equals its maximum configured capacity for 15+ consecutive minutes
- **Severity**: Medium
- **Response**: Notification after 15 minutes at max capacity
- **Action Required**: Consider increasing the service's maximum capacity in your configuration to allow it to scale out further

### Custom Alarm Configuration

You can modify or create additional CloudWatch alarms through the AWS Console or by extending the CDK code in the `s3_cross_region_compressor/resources/alarms.py` module.
