# Deployment Guide

This guide provides comprehensive instructions for deploying the S3 Cross-Region Compressor system to your AWS environment.

## Prerequisites

Before deployment, ensure you have the following prerequisites:

1. **AWS Account and Permissions**:
   - An AWS account with permissions to create:
     - IAM roles and policies
     - S3 buckets
     - DynamoDB tables
     - KMS keys
     - CloudWatch resources
     - ECS clusters/services
     - VPC and networking components
     - DynamoDB tables for configuration

2. **Local Development Environment**:
   - Python 3.9+ installed
   - AWS CLI v2 installed and configured
   - AWS CDK v2.44.0+ installed
   - Node.js 14.0.0+ and npm 6.0.0+ installed
   - **Docker 20.10.0+ or Finch (required for building containers)**
     - Docker: https://www.docker.com/
     - Finch: https://github.com/runfinch/finch 

3. **Self-Managed S3 Buckets**:
   - Source and target S3 buckets must already exist
   - If using KMS encryption, KMS keys must exist in each region

## Configuration

Before deploying, you need to prepare two configuration files:

### 1. settings.json

Create or update `configuration/settings.json` to define which AWS regions to use:

```json
{
    "stack_name": "s3-compressor",
    "enabled_regions": [
        {
            "region": "eu-west-1",
            "vpc_cidr": "10.100.0.0/16",
            "source_target": [ "source", "target" ],
            "availability_zones": 3
        },
        {
            "region": "us-east-2",
            "vpc_cidr": "10.102.0.0/16",
            "source_target": [ "target" ],
            "availability_zones": 3
        }
    ],
    "tags": {
        "Environment": "production",
        "Project": "s3-compressor"
    }
}
```

### 2. replication_config.json

Create or update `configuration/replication_config.json` to define which buckets to monitor and where to replicate data:

```json
{
    "replication_configuration": [
        {
            "source": {
                "region": "eu-west-1",
                "bucket": "my-source-bucket",
                "kms_key_arn": "",
                "prefix_filter": "",
                "suffix_filter": "",
                "scaling_limit": 20
            },
            "destinations": [
                {
                    "region": "us-east-2",
                    "bucket": "my-target-bucket",
                    "kms_key_arn": ""
                }
            ]
        }
    ]
}
```

See [CONFIGURATION.md](CONFIGURATION.md) for detailed configuration options.

> ⚠️ ***_Important Note:_*** The process deploys S3 Bucket notifications rules on your self-managed bucket specified as Source in the above config. This runs in parallel and from different CloudFormation stacks for each `replication_configuration` item. If you try to create multiple rules for the same S3 Bucket, you might face concurrency issues in CloudFormation. If that's the case, it's recommended that you add the first replication rule, run the deployment and then add additional rules and rerun the deployment for the new policies.

## Deployment Steps

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/s3-cross-region-compressor.git
cd s3-cross-region-compressor
```

### Step 2: Create a Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate.bat
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Authenticate to AWS

You need to have valid credentials to deploy the solution. We recommend using Roles for this,
but every organization will have different approaches.

You can use Environment Variables or pass `--profile` to the next commands to specify which
profile to use.

### Step 5: Bootstrap AWS CDK (First-time only)

If you haven't used CDK in the regions you're deploying to, bootstrap CDK first:

```bash
cdk bootstrap
```

Repeat for each region in your `settings.json` file.

### Step 6: Build Container Images

**MANDATORY**: You must build the container images before deploying. The CDK deployment requires these container tar files to be present.

#### Linux/macOS

```bash
chmod +x build_containers.sh
./build_containers.sh
```

#### Windows

```cmd
build_containers.bat
```

#### Verify Build Success

After building, verify the containers were created successfully:

```bash
ls -la bin/dist/
# Should show:
# source_region.tar
# target_region.tar
```

If the build fails:
- Ensure Docker or Finch is installed and running
- Check you have sufficient disk space
- Verify your container engine supports ARM64 builds

### Step 7: Deploy the Stacks

```bash
cdk deploy --all
```

The deployment will:
1. Create VPC and networking resources in each region
2. Create KMS keys, S3 buckets, and SQS queues
3. Deploy ECS clusters and task definitions
4. Configure S3 event notifications
5. Set up S3 cross-region replication
6. Create and configure other AWS resources

## Verification

After deployment, verify that the system is working correctly:

### 1. Check CloudFormation Stacks

Ensure all stacks are in `CREATE_COMPLETE` state:

```bash
aws cloudformation list-stacks --query "StackSummaries[?StackStatus=='CREATE_COMPLETE']"
```

### 2. Verify S3 Buckets

Verify that the necessary S3 buckets were created in each region:

```bash
aws s3 ls
```

### 3. Test the System

Upload a test file to the source bucket and verify it appears in the target bucket:

```bash
# Create a test file
echo "This is a test file" > test.txt

# Upload to source bucket as per replication_config.json config
aws s3 cp test.txt s3://my-source-bucket/

# Wait a few minutes for processing (if the service scaled down to 0, it can take more time)
sleep 300

# Check target bucket as per replication_config.json config
aws s3 ls s3://my-target-bucket/ --recursive
```

### 4. Check CloudWatch Metrics

Verify that metrics are being published to CloudWatch:

```bash
aws cloudwatch list-metrics --namespace {stack_name}
```

## Updating the Deployment

### Configuration Updates

1. Update the configuration files as needed
2. Redeploy using `cdk deploy --all`

CDK will identify what resources need to be updated and apply the changes.

### Code Updates

1. Make code changes
2. **Rebuild container images** using the build scripts:
   - Linux/macOS: `./build_containers.sh`
   - Windows: `build_containers.bat`
3. Verify the tar files were created in `./bin/dist/`:
   - `source_region.tar`
   - `target_region.tar`
4. Redeploy using `cdk deploy --all`

## Container Development

This section provides detailed instructions for contributors who want to develop changes for the containers and rebuild them.

### Prerequisites

Before building containers, ensure you have the following:

1. **UV Package Manager** (recommended):
   - UV is recommended for dependency management but not required
   - Installation: visit https://github.com/astral-sh/uv for instructions
   - If UV is not available, the build scripts will use existing requirements.txt files

2. **Container Engine**:
   - Either Docker or Finch is required
   - Docker: https://www.docker.com/
   - Finch: https://github.com/runfinch/finch
   - The build scripts will use whichever is available (preferring Finch if both are installed)
   - Will error out if neither is found

### Building Containers

The project includes scripts for building containers on different platforms:

#### Linux/macOS

Use the `build_containers.sh` script:

```bash
# Make sure the script is executable
chmod +x build_containers.sh

# Run the script
./build_containers.sh
```

#### Windows

Use the `build_containers.bat` script:

```cmd
# Run the script
build_containers.bat
```

### What the Build Scripts Do

The build scripts perform the following actions:

1. Check for dependencies (Docker/Finch is required; UV is optional)
2. If UV is available, refresh dependencies for source and target regions
   If not, use the existing requirements.txt files
3. Build ARM64 containers for both regions
4. Export container images to the ./bin/dist/ directory as tar files

### Container Development Workflow

1. **Make Changes to Container Code**:
   - Modify files in `bin/source_region/` or `bin/target_region/`
   - Update dependencies in pyproject.toml if needed

2. **Rebuild the Containers**:
   - Run the appropriate build script for your platform
   - Verify the tar files are created in `./bin/dist/`

3. **Test Your Changes**:
   - Deploy using `cdk deploy --all`
   - Monitor CloudWatch logs to verify your changes are working

4. **Troubleshooting**:
   - If the build fails due to missing Docker/Finch, install either of these container engines
   - If container builds successfully but deployment fails, check CloudWatch logs for runtime errors
   - If you prefer to manage dependencies manually:
     - You can update requirements.txt files directly without UV
     - Ensure all dependencies are properly specified with correct versions
     - Run the build scripts without UV, they will use your manually updated requirements.txt files

## Cleanup

To remove all resources created by the deployment:

```bash
cdk destroy --all
```

**Note**: This will not delete:
- Objects in your Self-Managed S3 buckets
- Manually created KMS keys
- CloudWatch logs

You'll need to manually delete these resources if needed.
