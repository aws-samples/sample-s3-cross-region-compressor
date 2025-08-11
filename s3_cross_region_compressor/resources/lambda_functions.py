from constructs import Construct
from aws_cdk import (
    Duration,
    aws_lambda as _lambda
)

def create_migration_lambda(scope: Construct, stack_name: str, ddb_replication_parameters: str) -> _lambda.Function:
    """
    Create a Lambda function to used by StepFunctions to facilitate data migration.

    Args:
        scope: The CDK construct scope

    Returns:
        The Lambda function
    """
    return _lambda.Function(
        scope,
        's3-object-filter-to-sqs',
        runtime=_lambda.Runtime.PYTHON_3_13,
        architecture=_lambda.Architecture.ARM_64,
        timeout=Duration.seconds(120),
        handler='s3-object-filter-to-sqs.lambda_handler',
        environment={
            "STACK_NAME": stack_name,
            "REPLICATION_PARAMETERS_TABLE_NAME": ddb_replication_parameters,
        },
        code=_lambda.Code.from_asset("s3_cross_region_compressor/lambda/s3-object-filter-to-sqs"),
    )